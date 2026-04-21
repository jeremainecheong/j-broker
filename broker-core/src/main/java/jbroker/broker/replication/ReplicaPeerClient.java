package jbroker.broker.replication;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.concurrent.TimeUnit;
import jbroker.proto.broker.ReplicaConsumerGrpc;
import jbroker.proto.broker.ReplicaFetchRequest;
import jbroker.proto.broker.ReplicaFetchResponse;

/**
 * Thin blocking gRPC stub for the {@code ReplicaFetch} RPC. One instance
 * per peer broker. {@link jbroker.broker.replication.ReplicaFetcher} owns
 * the retry / backoff policy; this class just sends a single request and
 * returns the response.
 */
public final class ReplicaPeerClient implements AutoCloseable {

    private final ManagedChannel channel;
    private final ReplicaConsumerGrpc.ReplicaConsumerBlockingStub stub;

    public ReplicaPeerClient(String host, int port) {
        this.channel = NettyChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.stub = ReplicaConsumerGrpc.newBlockingStub(channel);
    }

    public ReplicaFetchResponse fetch(ReplicaFetchRequest req, long timeoutMillis) {
        return stub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS).replicaFetch(req);
    }

    @Override
    public void close() {
        // Graceful shutdown only — P6.5 may need shutdownNow() here once
        // Broker.close owns a fleet of peer clients and we want fast-fail
        // rather than waiting up to 5s for each.
        channel.shutdown();
        try {
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
