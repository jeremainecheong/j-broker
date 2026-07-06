package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import jbroker.app.testkit.TestBrokers;
import jbroker.proto.broker.ConsumerGrpc;
import jbroker.proto.broker.FindCoordinatorRequest;
import jbroker.proto.broker.ListOffsetsPartition;
import jbroker.proto.broker.ListOffsetsRequest;
import jbroker.proto.common.ErrorCode;
import jbroker.proto.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * wire-up IT — confirms the new Consumer-group RPCs are exposed on the
 * gRPC server with the placeholder error codes documented in
 * {@code ConsumerHandler}'s javadoc.
 *
 * <p>This is the end-to-end counterpart to
 * {@code ConsumerHandlerSkeletonTest}, which exercises the handler in
 * isolation. The IT also exercises that {@code ListOffsets} is genuinely
 * implemented (returns 0 for an empty partition) rather than a placeholder.
 */
class ConsumerProtoWireUpIT {

    @Test
    void findCoordinatorReturnsCoordinatorNotAvailableBeforeTopicLands(@TempDir Path dir) throws Exception {
        var broker = TestBrokers.startSingleNode(dir);
        int brokerPort = broker.brokerPort();
        var channel = NettyChannelBuilder.forAddress("127.0.0.1", brokerPort)
                .usePlaintext()
                .build();
        try {
            var stub = ConsumerGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
            // Before __consumer_offsets auto-creates, FindCoordinator
            // legitimately returns CNA. After / the heartbeat,
            // commit, and fetch RPCs all moved off the placeholder path
            // and have proper coverage in GroupHeartbeatEndToEndIT and
            // OffsetCommitFetchEndToEndIT.
            var find = stub.findCoordinator(
                    FindCoordinatorRequest.newBuilder().setKey("g1").build());
            assertThat(find.getError()).isEqualTo(ErrorCode.COORDINATOR_NOT_AVAILABLE);
        } finally {
            channel.shutdown();
            channel.awaitTermination(2, TimeUnit.SECONDS);
            broker.close();
        }
    }

    @Test
    void listOffsetsReturnsLatestForExistingPartition(@TempDir Path dir) throws Exception {
        var broker = TestBrokers.startSingleNode(dir);
        int brokerPort = broker.brokerPort();
        try (var client = new jbroker.broker.client.BrokerClient("127.0.0.1", brokerPort)) {
            client.createTopic("orders", 1, 1);
        }
        var channel = NettyChannelBuilder.forAddress("127.0.0.1", brokerPort)
                .usePlaintext()
                .build();
        try {
            var stub = ConsumerGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
            var resp = stub.listOffsets(ListOffsetsRequest.newBuilder()
                    .setReplicaId(-1)
                    .addPartitions(ListOffsetsPartition.newBuilder()
                            .setTp(TopicPartition.newBuilder()
                                    .setTopic("orders")
                                    .setPartition(0)
                                    .build())
                            .setTimestamp(-1)
                            .build())
                    .build());
            assertThat(resp.getResults(0).getError()).isEqualTo(ErrorCode.OK);
            // Empty partition → nextOffset() == 0.
            assertThat(resp.getResults(0).getOffset()).isEqualTo(0L);
        } finally {
            channel.shutdown();
            channel.awaitTermination(2, TimeUnit.SECONDS);
            broker.close();
        }
    }
}
