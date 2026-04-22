package jbroker.broker.replication;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.concurrent.TimeUnit;
import jbroker.proto.broker.OffsetsForLeaderEpochRequest;
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

    /**
     * Queries the leader's {@code OffsetsForLeaderEpoch} (P6.4). Throws
     * {@link RuntimeException} on a non-NONE error so the follower's
     * reconciliation loop surfaces the failure cleanly.
     */
    public long offsetsForLeaderEpoch(String topic, int partition, int leaderEpoch, long timeoutMillis) {
        var resp = stub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                .offsetsForLeaderEpoch(OffsetsForLeaderEpochRequest.newBuilder()
                        .setTopic(topic)
                        .setPartition(partition)
                        .setLeaderEpoch(leaderEpoch)
                        .build());
        if (resp.hasError() && resp.getError().getCode() != 0) {
            throw new RuntimeException(
                    "offsetsForLeaderEpoch failed: " + resp.getError().getMessage());
        }
        return resp.getEndOffset();
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
