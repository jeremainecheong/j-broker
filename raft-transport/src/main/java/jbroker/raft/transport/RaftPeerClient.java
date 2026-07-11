package jbroker.raft.transport;

import io.grpc.ClientInterceptor;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import jbroker.proto.raft.AppendEntriesRequest;
import jbroker.proto.raft.AppendEntriesResponse;
import jbroker.proto.raft.RaftGrpc;
import jbroker.proto.raft.RequestVoteRequest;
import jbroker.proto.raft.RequestVoteResponse;
import jbroker.raft.core.NodeId;
import jbroker.tls.TlsConfig;
import jbroker.tls.TlsContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Blocking gRPC stub bundle for one Raft peer.
 *
 * <p><b>DNS-wedge recovery.</b> When a peer container restarts, Docker DNS
 * can leave the channel's resolver looping on {@code UnknownHostException}
 * long after the peer is back (#115, defect 1) — for Raft that means a
 * voter that never rejoins elections or replication. Every failed RPC
 * resets the channel's connect backoff, and
 * {@value #REBUILD_AFTER_UNRESOLVABLE} consecutive <em>name-resolution</em>
 * failures rebuild the channel outright, forcing a fresh resolver.
 * Connection-refused failures never rebuild: the name resolved, the peer
 * is just down.
 */
public final class RaftPeerClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RaftPeerClient.class);

    /** Consecutive unresolvable failures that trigger a channel rebuild. */
    static final int REBUILD_AFTER_UNRESOLVABLE = 3;

    private final NodeId peerId;
    private final String host;
    private final int port;
    private final io.grpc.netty.shaded.io.netty.handler.ssl.SslContext sslCtx;
    private final ClientInterceptor chaosInterceptor;

    private final AtomicInteger unresolvableStreak = new AtomicInteger();
    private final AtomicInteger rebuilds = new AtomicInteger();
    private volatile boolean closed;
    private volatile ManagedChannel channel;
    private volatile RaftGrpc.RaftBlockingStub stub;

    public RaftPeerClient(NodeId peerId, String host, int port) {
        this(peerId, host, port, TlsConfig.DISABLED);
    }

    /** TLS-aware constructor. Raft peers share the cluster TLS bundle. */
    public RaftPeerClient(NodeId peerId, String host, int port, TlsConfig tls) {
        this(peerId, host, port, tls, null);
    }

    /**
     * Audit-finding #3 — constructor accepting an optional
     * {@link ClientInterceptor} that gates every outbound RPC (chaos
     * partition in practice). Null is tolerated for legacy tests and
     * production paths that don't need chaos gating.
     */
    public RaftPeerClient(NodeId peerId, String host, int port, TlsConfig tls, ClientInterceptor chaosInterceptor) {
        this.peerId = peerId;
        this.host = host;
        this.port = port;
        try {
            this.sslCtx = TlsContexts.clientContext(tls == null ? TlsConfig.DISABLED : tls);
        } catch (javax.net.ssl.SSLException e) {
            throw new IllegalStateException("TLS client context build failed", e);
        }
        this.chaosInterceptor = chaosInterceptor;
        this.channel = buildChannel();
        this.stub = RaftGrpc.newBlockingStub(channel);
    }

    private ManagedChannel buildChannel() {
        var b = NettyChannelBuilder.forAddress(host, port);
        if (sslCtx == null) {
            b.usePlaintext();
        } else {
            b.sslContext(sslCtx);
        }
        if (chaosInterceptor != null) {
            b.intercept(chaosInterceptor);
        }
        return b.build();
    }

    public NodeId peerId() {
        return peerId;
    }

    /**
     * Requests a connection to the peer eagerly so the first real RPC does not
     * pay the connection-establishment latency.  Safe to call before the peer's
     * server is up; the channel will retry in the background.
     */
    public void warmUp() {
        channel.getState(true); // requestConnection=true triggers IDLE→CONNECTING
    }

    /**
     * Blocks until the channel is in the READY state or the timeout elapses.
     * Returns {@code true} if READY was reached, {@code false} on timeout.
     */
    public boolean waitForReady(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            var ch = channel;
            ConnectivityState state = ch.getState(true);
            if (state == ConnectivityState.READY) {
                return true;
            }
            // Register a one-shot callback that fires when the state changes,
            // then wait at most 50 ms before re-checking.
            var latch = new CountDownLatch(1);
            ch.notifyWhenStateChanged(state, latch::countDown);
            latch.await(50, TimeUnit.MILLISECONDS);
        }
        return channel.getState(false) == ConnectivityState.READY;
    }

    public AppendEntriesResponse appendEntries(AppendEntriesRequest req) {
        return call(() -> stub.withDeadlineAfter(2, TimeUnit.SECONDS).appendEntries(req));
    }

    public RequestVoteResponse requestVote(RequestVoteRequest req) {
        return call(() -> stub.withDeadlineAfter(2, TimeUnit.SECONDS).requestVote(req));
    }

    public jbroker.proto.raft.TimeoutNowResponse timeoutNow(jbroker.proto.raft.TimeoutNowRequest req) {
        return call(() -> stub.withDeadlineAfter(2, TimeUnit.SECONDS).timeoutNow(req));
    }

    public jbroker.proto.raft.InstallSnapshotResponse installSnapshot(jbroker.proto.raft.InstallSnapshotRequest req) {
        // Snapshots can be large; allow 30s for the single-shot transfer.
        return call(() -> stub.withDeadlineAfter(30, TimeUnit.SECONDS).installSnapshot(req));
    }

    /** Forward a client propose to this peer (believed to be the Raft leader). */
    public jbroker.proto.raft.ProposeResponse propose(byte[] payload, int hops) {
        var req = jbroker.proto.raft.ProposeRequest.newBuilder()
                .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                .setHops(hops)
                .build();
        return call(() -> stub.withDeadlineAfter(2, TimeUnit.SECONDS).propose(req));
    }

    private <T> T call(Supplier<T> rpc) {
        try {
            T resp = rpc.get();
            unresolvableStreak.set(0);
            return resp;
        } catch (RuntimeException e) {
            recordFailure(e);
            throw e;
        }
    }

    private void recordFailure(Throwable t) {
        if (nameResolutionFailure(t)) {
            if (unresolvableStreak.incrementAndGet() >= REBUILD_AFTER_UNRESOLVABLE) {
                rebuildChannel();
            }
            return;
        }
        unresolvableStreak.set(0);
        // Peer down / connect refused: RaftDriver's dispatch cadence is the
        // retry policy; don't let the channel's exponential reconnect
        // backoff stack on top of it.
        channel.resetConnectBackoff();
    }

    private synchronized void rebuildChannel() {
        if (closed) return;
        // Re-check under the lock — concurrent dispatch threads may race
        // here and only one rebuild per streak is wanted.
        if (unresolvableStreak.get() < REBUILD_AFTER_UNRESOLVABLE) return;
        unresolvableStreak.set(0);
        var old = channel;
        channel = buildChannel();
        stub = RaftGrpc.newBlockingStub(channel);
        old.shutdownNow();
        log.info(
                "rebuilt raft channel to peer {} at {}:{} after {} consecutive unresolvable failures (rebuild #{})",
                peerId.value(),
                host,
                port,
                REBUILD_AFTER_UNRESOLVABLE,
                rebuilds.incrementAndGet());
    }

    /**
     * True when the failure's cause chain says the peer's hostname did not
     * resolve — the signature of a wedged resolver after a container
     * restart, as opposed to a reachable-but-down peer.
     */
    static boolean nameResolutionFailure(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
            if (c instanceof java.net.UnknownHostException
                    || c instanceof java.nio.channels.UnresolvedAddressException) {
                return true;
            }
            if (c instanceof io.grpc.StatusRuntimeException sre) {
                var statusCause = sre.getStatus().getCause();
                if (statusCause != null && statusCause != c && nameResolutionFailure(statusCause)) {
                    return true;
                }
                var desc = sre.getStatus().getDescription();
                if (desc != null && desc.contains("Unable to resolve host")) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Test hook: number of channel rebuilds performed so far. */
    int channelRebuilds() {
        return rebuilds.get();
    }

    @Override
    public void close() {
        closed = true;
        ManagedChannel ch;
        synchronized (this) {
            ch = channel;
        }
        ch.shutdown();
        try {
            ch.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
