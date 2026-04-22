package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import jbroker.proto.broker.DescribeConsumerGroupRequest;
import jbroker.proto.broker.DescribeRaftRequest;
import jbroker.proto.broker.DescribeTopicPartitionsRequest;
import jbroker.proto.broker.ListConsumerGroupsRequest;
import jbroker.proto.broker.MetadataGrpc;
import jbroker.proto.common.ErrorCode;
import jbroker.raft.core.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Wire-up IT — the Milestone 8 {@code Metadata} gRPC surface is reachable on
 * the broker. As slices land, more RPCs move off the UNIMPLEMENTED
 * placeholder:
 * <ul>
 *   <li>{@code DescribeCluster} (exercised in {@link DescribeClusterIT}).</li>
 *   <li>{@code DescribeTopicPartitions} (returns {@code UNKNOWN} for
 *       an unknown topic).</li>
 *   <li>{@code ListConsumerGroups} + {@code DescribeConsumerGroup}
 *       (list returns empty OK, describe returns {@code NOT_COORDINATOR}
 *       for unknown groups — E2E coverage in
 *       {@link jbroker.admin.e2e.ConsumerGroupLagIT}).</li>
 *   <li>{@code DescribeRaft} — still placeholder.</li>
 * </ul>
 */
class MetadataServiceWireUpIT {

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void unimplementedRpcsReturnUnimplementedUntilOwnerSliceLands(@TempDir Path dir) throws Exception {
        int brokerPort = freePort();
        int raftPort = freePort();
        var broker = Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort));
        var channel = NettyChannelBuilder.forAddress("127.0.0.1", brokerPort)
                .usePlaintext()
                .build();
        try {
            var stub = MetadataGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
            // describeTopicPartitions is implemented as of ; requesting
            // an unknown topic still exercises the RPC surface and is
            // expected to return UNKNOWN rather than UNIMPLEMENTED.
            assertThat(stub.describeTopicPartitions(DescribeTopicPartitionsRequest.newBuilder()
                                    .setTopic("does-not-exist")
                                    .build())
                            .getError())
                    .isEqualTo(ErrorCode.UNKNOWN);
            // listConsumerGroups is implemented as of ; a freshly-booted
            // broker with no members yet returns OK with an empty list.
            assertThat(stub.listConsumerGroups(
                                    ListConsumerGroupsRequest.newBuilder().build())
                            .getError())
                    .isEqualTo(ErrorCode.OK);
            // describeConsumerGroup for an unknown group returns
            // NOT_COORDINATOR — the admin-app fan-out uses that to iterate.
            assertThat(stub.describeConsumerGroup(DescribeConsumerGroupRequest.newBuilder()
                                    .setGroupId("does-not-exist")
                                    .build())
                            .getError())
                    .isEqualTo(ErrorCode.NOT_COORDINATOR);
            assertThat(stub.describeRaft(DescribeRaftRequest.newBuilder().build())
                            .getError())
                    .isEqualTo(ErrorCode.UNIMPLEMENTED);
        } finally {
            channel.shutdown();
            channel.awaitTermination(2, TimeUnit.SECONDS);
            broker.close();
        }
    }
}
