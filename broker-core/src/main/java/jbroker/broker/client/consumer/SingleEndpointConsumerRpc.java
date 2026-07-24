package jbroker.broker.client.consumer;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.concurrent.TimeUnit;
import jbroker.proto.broker.CommitOffsetsRequest;
import jbroker.proto.broker.CommitOffsetsResponse;
import jbroker.proto.broker.ConsumerGroupHeartbeatRequest;
import jbroker.proto.broker.ConsumerGroupHeartbeatResponse;
import jbroker.proto.broker.ConsumerGrpc;
import jbroker.proto.broker.FetchOffsetsRequest;
import jbroker.proto.broker.FetchOffsetsResponse;
import jbroker.proto.broker.FetchRequest;
import jbroker.proto.broker.FetchResponse;
import jbroker.proto.broker.FindCoordinatorRequest;
import jbroker.proto.broker.ListOffsetsRequest;
import jbroker.proto.broker.ListOffsetsResponse;
import jbroker.proto.broker.ProduceRequest;
import jbroker.proto.broker.ProduceResponse;
import jbroker.proto.broker.ProducerGrpc;
import jbroker.proto.common.ErrorCode;
import jbroker.tls.TlsContexts;

/**
 * The original single-endpoint wiring behind {@link ConsumerRpc}: one
 * bootstrap channel from {@link ConsumerConfig}, a cached coordinator
 * resolved via {@code FindCoordinator} (reusing the bootstrap channel when
 * the coordinator IS the bootstrap broker), and DLT produces over the
 * bootstrap channel. Coordinator-routed calls return {@code null} when
 * {@code FindCoordinator} fails; the consumer decides whether that is a
 * skip-this-tick (heartbeat) or an error (commit).
 */
final class SingleEndpointConsumerRpc implements ConsumerRpc {

    private final ConsumerConfig cfg;
    private final ManagedChannel bootstrapChannel;
    private final ConsumerGrpc.ConsumerBlockingStub bootstrapStub;
    private ManagedChannel coordinatorChannel;
    private ConsumerGrpc.ConsumerBlockingStub coordinatorStub;
    private CoordinatorEndpoint cachedCoordinator;
    // Lazy ProducerGrpc stub over the bootstrap channel for DLT routing.
    private ProducerGrpc.ProducerBlockingStub dltProducerStub;

    SingleEndpointConsumerRpc(ConsumerConfig cfg) {
        this.cfg = cfg;
        this.bootstrapChannel = buildChannel(cfg, cfg.bootstrapHost(), cfg.bootstrapPort());
        this.bootstrapStub = ConsumerGrpc.newBlockingStub(bootstrapChannel);
    }

    private static ManagedChannel buildChannel(ConsumerConfig cfg, String host, int port) {
        var b = NettyChannelBuilder.forAddress(host, port);
        try {
            var sslCtx = TlsContexts.clientContext(cfg.tls());
            if (sslCtx == null) {
                b.usePlaintext();
            } else {
                b.sslContext(sslCtx);
            }
        } catch (javax.net.ssl.SSLException e) {
            throw new IllegalStateException("TLS client context build failed", e);
        }
        return b.build();
    }

    private record CoordinatorEndpoint(String host, int port) {}

    private ConsumerGrpc.ConsumerBlockingStub ensureCoordinator() {
        if (cachedCoordinator != null && coordinatorStub != null) {
            return coordinatorStub;
        }
        var resp = bootstrapStub
                .withDeadlineAfter(cfg.pollFetchDeadline().toMillis(), TimeUnit.MILLISECONDS)
                .findCoordinator(FindCoordinatorRequest.newBuilder()
                        .setKey(cfg.groupId())
                        .build());
        if (resp.getError() != ErrorCode.OK) {
            return null;
        }
        var ep = new CoordinatorEndpoint(
                resp.getCoordinator().getHost(), resp.getCoordinator().getPort());
        cachedCoordinator = ep;
        if (ep.host().equals(cfg.bootstrapHost()) && ep.port() == cfg.bootstrapPort()) {
            // Coordinator is the bootstrap broker — reuse the channel.
            coordinatorChannel = null;
            coordinatorStub = bootstrapStub;
        } else {
            coordinatorChannel = buildChannel(cfg, ep.host(), ep.port());
            coordinatorStub = ConsumerGrpc.newBlockingStub(coordinatorChannel);
        }
        return coordinatorStub;
    }

    @Override
    public ConsumerGroupHeartbeatResponse heartbeat(ConsumerGroupHeartbeatRequest req) {
        var stub = ensureCoordinator();
        if (stub == null) return null;
        return stub.withDeadlineAfter(cfg.pollFetchDeadline().toMillis(), TimeUnit.MILLISECONDS)
                .consumerGroupHeartbeat(req);
    }

    @Override
    public void invalidateCoordinator() {
        cachedCoordinator = null;
        if (coordinatorChannel != null) {
            shutdownChannel(coordinatorChannel);
            coordinatorChannel = null;
        }
        coordinatorStub = null;
    }

    @Override
    public CommitOffsetsResponse commitOffsets(CommitOffsetsRequest req) {
        var stub = ensureCoordinator();
        if (stub == null) return null;
        return stub.withDeadlineAfter(cfg.pollFetchDeadline().toMillis(), TimeUnit.MILLISECONDS)
                .commitOffsets(req);
    }

    @Override
    public FetchOffsetsResponse fetchOffsets(FetchOffsetsRequest req) {
        var stub = ensureCoordinator();
        if (stub == null) return null;
        return stub.withDeadlineAfter(cfg.pollFetchDeadline().toMillis(), TimeUnit.MILLISECONDS)
                .fetchOffsets(req);
    }

    @Override
    public ListOffsetsResponse listOffsets(ListOffsetsRequest req) {
        var stub = ensureCoordinator();
        if (stub == null) return null;
        return stub.withDeadlineAfter(cfg.pollFetchDeadline().toMillis(), TimeUnit.MILLISECONDS)
                .listOffsets(req);
    }

    @Override
    public FetchResponse fetch(FetchRequest req) {
        var stub = ensureCoordinator();
        if (stub == null) return null;
        return stub.withDeadlineAfter(cfg.pollFetchDeadline().toMillis(), TimeUnit.MILLISECONDS)
                .fetch(req);
    }

    @Override
    public ProduceResponse produceDlt(ProduceRequest req) {
        if (dltProducerStub == null) {
            dltProducerStub = ProducerGrpc.newBlockingStub(bootstrapChannel);
        }
        return dltProducerStub
                .withDeadlineAfter(cfg.pollFetchDeadline().toMillis(), TimeUnit.MILLISECONDS)
                .produce(req);
    }

    @Override
    public void leaveGroup(ConsumerGroupHeartbeatRequest req) {
        // Best-effort — if the coordinator is unreachable, the
        // session-timeout eviction will pick up the slack.
        try {
            if (coordinatorStub != null) {
                coordinatorStub.withDeadlineAfter(2, TimeUnit.SECONDS).consumerGroupHeartbeat(req);
            }
        } catch (Exception ignored) {
            // best-effort
        }
    }

    @Override
    public void close() {
        shutdownChannel(coordinatorChannel);
        shutdownChannel(bootstrapChannel);
    }

    private static void shutdownChannel(ManagedChannel ch) {
        if (ch == null) return;
        ch.shutdown();
        try {
            ch.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
