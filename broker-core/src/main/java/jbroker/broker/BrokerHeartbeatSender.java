package jbroker.broker;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import jbroker.proto.broker.BrokerHeartbeatRequest;
import jbroker.proto.broker.ClusterGrpc;
import jbroker.tls.TlsConfig;
import jbroker.tls.TlsContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically broadcasts a {@code BrokerHeartbeat} RPC from {@code self}
 * to every configured peer address. Each peer's handler updates its local
 * {@link BrokerLiveness}; over time every broker has a consistent
 * cluster-wide liveness view.
 *
 * <p>Addresses are static (from {@code Broker.Config.voters()}) rather
 * than dynamic ({@code BrokerRegistry}) because on startup the sender
 * must work before the registry is populated — registration itself is
 * leader-driven and takes ~1 s to land.
 *
 * <p>Failures (peer down, connect refused) are logged at debug and
 * dropped. The next tick retries.
 */
public final class BrokerHeartbeatSender implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(BrokerHeartbeatSender.class);

    public record PeerAddress(int brokerId, String host, int port) {}

    /** Consecutive unresolvable failures that trigger a channel rebuild. */
    static final int REBUILD_AFTER_UNRESOLVABLE = 3;

    private final int selfBrokerId;
    private final java.util.List<PeerAddress> peers;
    private final LongSupplier metadataOffset;
    private final long intervalMs;
    private final BooleanSupplier paused;
    private final ScheduledExecutorService scheduler;
    private final io.grpc.netty.shaded.io.netty.handler.ssl.SslContext sslCtx;
    private final java.util.function.IntFunction<io.grpc.ClientInterceptor> chaosInterceptorFactory;
    private final Map<Integer, ManagedChannel> channels = new HashMap<>();
    private final Map<Integer, ClusterGrpc.ClusterBlockingStub> stubs = new HashMap<>();
    // Only touched from the single scheduler thread — plain map is safe.
    private final Map<Integer, Integer> unresolvableStreaks = new HashMap<>();

    /**
     * Back-compat 4-arg constructor — no pause gate. Existing tests keep
     * working; only Broker.start currently needs the pause-aware variant.
     */
    public BrokerHeartbeatSender(
            int selfBrokerId, java.util.List<PeerAddress> peers, LongSupplier metadataOffset, long intervalMs) {
        this(selfBrokerId, peers, metadataOffset, intervalMs, () -> false);
    }

    /**
     * Chaos-aware constructor. {@code paused.getAsBoolean()} is
     * polled once per tick; when it returns true the whole tick no-ops so
     * peers' {@link BrokerLiveness} eventually flips this broker to dead and
     * the fencer promotes a replacement leader. Combined with the existing
     * {@link jbroker.broker.chaos.ChaosServerInterceptor} (which blocks
     * inbound traffic), pause now simulates a bidirectional disconnect
     * instead of a one-way "refuse-to-answer".
     */
    public BrokerHeartbeatSender(
            int selfBrokerId,
            java.util.List<PeerAddress> peers,
            LongSupplier metadataOffset,
            long intervalMs,
            BooleanSupplier paused) {
        this(selfBrokerId, peers, metadataOffset, intervalMs, paused, TlsConfig.DISABLED);
    }

    /** TLS-aware constructor. Inter-broker heartbeats share the cluster TLS bundle. */
    public BrokerHeartbeatSender(
            int selfBrokerId,
            java.util.List<PeerAddress> peers,
            LongSupplier metadataOffset,
            long intervalMs,
            BooleanSupplier paused,
            TlsConfig tls) {
        this(selfBrokerId, peers, metadataOffset, intervalMs, paused, tls, null);
    }

    /**
     * Audit-finding #3 — per-peer {@code ClientInterceptor} factory so chaos
     * outbound-partition gating actually fires on heartbeat RPCs. Null factory
     * is tolerated and skips the install.
     */
    public BrokerHeartbeatSender(
            int selfBrokerId,
            java.util.List<PeerAddress> peers,
            LongSupplier metadataOffset,
            long intervalMs,
            BooleanSupplier paused,
            TlsConfig tls,
            java.util.function.IntFunction<io.grpc.ClientInterceptor> chaosInterceptorFactory) {
        this.selfBrokerId = selfBrokerId;
        this.peers = java.util.List.copyOf(peers);
        this.metadataOffset = metadataOffset;
        this.intervalMs = intervalMs;
        this.paused = paused;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "broker-heartbeat-sender-" + selfBrokerId);
            t.setDaemon(true);
            return t;
        });
        try {
            this.sslCtx = TlsContexts.clientContext(tls == null ? TlsConfig.DISABLED : tls);
        } catch (javax.net.ssl.SSLException e) {
            throw new IllegalStateException("TLS client context build failed", e);
        }
        this.chaosInterceptorFactory = chaosInterceptorFactory;
        for (var p : this.peers) {
            var ch = buildChannel(p);
            channels.put(p.brokerId(), ch);
            stubs.put(p.brokerId(), ClusterGrpc.newBlockingStub(ch));
        }
    }

    private ManagedChannel buildChannel(PeerAddress p) {
        var b = NettyChannelBuilder.forAddress(p.host(), p.port());
        if (sslCtx == null) {
            b.usePlaintext();
        } else {
            b.sslContext(sslCtx);
        }
        if (chaosInterceptorFactory != null) {
            var interceptor = chaosInterceptorFactory.apply(p.brokerId());
            if (interceptor != null) b.intercept(interceptor);
        }
        return b.build();
    }

    public void start() {
        scheduler.scheduleWithFixedDelay(this::tick, 0, intervalMs, TimeUnit.MILLISECONDS);
    }

    private void tick() {
        if (paused.getAsBoolean()) {
            // Chaos pause — skip the tick entirely. Peers will observe our
            // last-signal age climb past BrokerFencer's staleness threshold
            // (3s) and mark us dead.
            return;
        }
        long offset = metadataOffset.getAsLong();
        var req = BrokerHeartbeatRequest.newBuilder()
                .setBrokerId(selfBrokerId)
                .setCurrentMetadataOffset(offset)
                .setSupportedProtocolMin(ProtocolVersion.MIN_SUPPORTED)
                .setSupportedProtocolMax(ProtocolVersion.CURRENT)
                .build();
        for (var p : peers) {
            var stub = stubs.get(p.brokerId());
            if (stub == null) continue;
            try {
                stub.withDeadlineAfter(intervalMs, TimeUnit.MILLISECONDS).brokerHeartbeat(req);
                unresolvableStreaks.remove(p.brokerId());
            } catch (Exception e) {
                log.debug(
                        "heartbeat to broker {} at {}:{} failed: {}", p.brokerId(), p.host(), p.port(), e.getMessage());
                onFailure(p, e);
            }
        }
    }

    private void onFailure(PeerAddress p, Exception e) {
        // A wedged resolver (Docker DNS after a peer restart, #115 defect 1)
        // loops on UnknownHostException long after the peer is back; a
        // backoff reset alone doesn't always clear it. After
        // REBUILD_AFTER_UNRESOLVABLE consecutive name-resolution failures,
        // rebuild the channel outright to force a fresh resolver.
        if (jbroker.broker.replication.ReplicaPeerClient.nameResolutionFailure(e)) {
            int streak = unresolvableStreaks.merge(p.brokerId(), 1, Integer::sum);
            if (streak >= REBUILD_AFTER_UNRESOLVABLE) {
                unresolvableStreaks.remove(p.brokerId());
                var old = channels.get(p.brokerId());
                if (old != null) old.shutdownNow();
                var fresh = buildChannel(p);
                channels.put(p.brokerId(), fresh);
                stubs.put(p.brokerId(), ClusterGrpc.newBlockingStub(fresh));
                log.info(
                        "rebuilt heartbeat channel to broker {} at {}:{} after {} consecutive unresolvable failures",
                        p.brokerId(),
                        p.host(),
                        p.port(),
                        REBUILD_AFTER_UNRESOLVABLE);
            }
            return;
        }
        unresolvableStreaks.remove(p.brokerId());
        // This tick is the retry policy; don't let the channel's
        // exponential reconnect backoff stack on top of it. Without
        // this, a peer that comes up after a few failed connects
        // (sequential cluster start, broker restart) receives no
        // heartbeats for seconds — long enough for a broker to live
        // and die without ever landing in any peer's BrokerLiveness,
        // which made it unfenceable (MultiBrokerFailoverIT flake).
        var ch = channels.get(p.brokerId());
        if (ch != null) ch.resetConnectBackoff();
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        for (var ch : channels.values()) {
            ch.shutdown();
        }
        for (var ch : channels.values()) {
            try {
                ch.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
