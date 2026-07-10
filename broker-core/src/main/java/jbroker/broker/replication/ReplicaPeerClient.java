package jbroker.broker.replication;

import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import jbroker.proto.broker.OffsetsForLeaderEpochRequest;
import jbroker.proto.broker.ReplicaConsumerGrpc;
import jbroker.proto.broker.ReplicaFetchRequest;
import jbroker.proto.broker.ReplicaFetchResponse;
import jbroker.tls.TlsConfig;
import jbroker.tls.TlsContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thin blocking gRPC stub for the {@code ReplicaFetch} RPC. One instance
 * per peer broker. {@link jbroker.broker.replication.ReplicaFetcher} owns
 * the retry / backoff policy; this class just sends a single request and
 * returns the response.
 *
 * <p><b>DNS-wedge recovery.</b> When a peer container restarts, Docker DNS
 * can leave this channel's resolver looping on {@code UnknownHostException}
 * long after the peer is back — the replica fetcher then starves silently
 * (#115, defect 1; broker2 fell 3,353 records behind that way). Every
 * failed RPC resets the channel's connect backoff (the #110 heartbeat
 * treatment), and {@value #REBUILD_AFTER_UNRESOLVABLE} consecutive
 * <em>name-resolution</em> failures rebuild the channel outright, forcing a
 * fresh resolver. Connection-refused failures never rebuild: the name
 * resolved, the peer is just down, and churning channels there is waste.
 */
public final class ReplicaPeerClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ReplicaPeerClient.class);

    /** Consecutive unresolvable failures that trigger a channel rebuild. */
    static final int REBUILD_AFTER_UNRESOLVABLE = 3;

    private final String host;
    private final int port;
    private final io.grpc.netty.shaded.io.netty.handler.ssl.SslContext sslCtx;
    private final ClientInterceptor chaosInterceptor;

    private final AtomicInteger unresolvableStreak = new AtomicInteger();
    private final AtomicInteger rebuilds = new AtomicInteger();
    private volatile boolean closed;
    private volatile ManagedChannel channel;
    private volatile ReplicaConsumerGrpc.ReplicaConsumerBlockingStub stub;

    public ReplicaPeerClient(String host, int port) {
        this(host, port, TlsConfig.DISABLED);
    }

    /** TLS-aware constructor. Inter-broker replication uses the same cert bundle as Raft. */
    public ReplicaPeerClient(String host, int port, TlsConfig tls) {
        this(host, port, tls, null);
    }

    /**
     * Audit-finding #3 — constructor accepting an optional outbound
     * {@link ClientInterceptor} (chaos partition in practice). Null is
     * tolerated for legacy tests.
     */
    public ReplicaPeerClient(String host, int port, TlsConfig tls, ClientInterceptor chaosInterceptor) {
        this.host = host;
        this.port = port;
        try {
            this.sslCtx = TlsContexts.clientContext(tls);
        } catch (javax.net.ssl.SSLException e) {
            throw new IllegalStateException("TLS client context build failed", e);
        }
        this.chaosInterceptor = chaosInterceptor;
        this.channel = buildChannel();
        this.stub = ReplicaConsumerGrpc.newBlockingStub(channel);
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

    public ReplicaFetchResponse fetch(ReplicaFetchRequest req, long timeoutMillis) {
        var s = stub;
        try {
            var resp = s.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS).replicaFetch(req);
            recordSuccess();
            return resp;
        } catch (RuntimeException e) {
            recordFailure(e);
            throw e;
        }
    }

    /**
     * Queries the leader's {@code OffsetsForLeaderEpoch}. Throws
     * {@link RuntimeException} on a non-NONE error so the follower's
     * reconciliation loop surfaces the failure cleanly.
     */
    public long offsetsForLeaderEpoch(String topic, int partition, int leaderEpoch, long timeoutMillis) {
        var s = stub;
        OffsetsForLeaderEpochRequest req = OffsetsForLeaderEpochRequest.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setLeaderEpoch(leaderEpoch)
                .build();
        jbroker.proto.broker.OffsetsForLeaderEpochResponse resp;
        try {
            resp = s.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS).offsetsForLeaderEpoch(req);
            recordSuccess();
        } catch (RuntimeException e) {
            recordFailure(e);
            throw e;
        }
        if (resp.hasError() && resp.getError().getCode() != 0) {
            throw new RuntimeException(
                    "offsetsForLeaderEpoch failed: " + resp.getError().getMessage());
        }
        return resp.getEndOffset();
    }

    private void recordSuccess() {
        unresolvableStreak.set(0);
    }

    private void recordFailure(Throwable t) {
        if (nameResolutionFailure(t)) {
            if (unresolvableStreak.incrementAndGet() >= REBUILD_AFTER_UNRESOLVABLE) {
                rebuildChannel();
            }
            return;
        }
        unresolvableStreak.set(0);
        // Peer down / connect refused: the periodic fetch tick is the retry
        // policy, so don't let the channel's exponential reconnect backoff
        // stack on top of it (same rationale as the heartbeat sender).
        channel.resetConnectBackoff();
    }

    private synchronized void rebuildChannel() {
        if (closed) return;
        // Re-check under the lock — a concurrent caller may have rebuilt
        // for this same streak already.
        if (unresolvableStreak.get() < REBUILD_AFTER_UNRESOLVABLE) return;
        unresolvableStreak.set(0);
        var old = channel;
        channel = buildChannel();
        stub = ReplicaConsumerGrpc.newBlockingStub(channel);
        old.shutdownNow();
        log.info(
                "rebuilt replica-fetch channel to {}:{} after {} consecutive unresolvable failures (rebuild #{})",
                host,
                port,
                REBUILD_AFTER_UNRESOLVABLE,
                rebuilds.incrementAndGet());
    }

    /**
     * True when the failure's cause chain says the peer's hostname did not
     * resolve — the signature of a wedged resolver after a container
     * restart, as opposed to a reachable-but-down peer. Public because
     * {@link jbroker.broker.BrokerHeartbeatSender} applies the same
     * rebuild policy to its own per-peer channels.
     */
    public static boolean nameResolutionFailure(Throwable t) {
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
        // Graceful shutdown only — may need shutdownNow() here once
        // Broker.close owns a fleet of peer clients and we want fast-fail
        // rather than waiting up to 5s for each.
        ch.shutdown();
        try {
            ch.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
