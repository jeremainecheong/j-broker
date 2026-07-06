package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import jbroker.app.testkit.TestBrokers;
import jbroker.proto.broker.DescribeConsumerGroupRequest;
import jbroker.proto.broker.DescribeRaftRequest;
import jbroker.proto.broker.DescribeTopicPartitionsRequest;
import jbroker.proto.broker.ListConsumerGroupsRequest;
import jbroker.proto.broker.MetadataGrpc;
import jbroker.proto.common.ErrorCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Wire-up IT — the Milestone 8 {@code Metadata} gRPC surface is reachable on
 * the broker. As of every RPC has a real implementation; this IT
 * just confirms the surface is registered and returns well-formed
 * responses (deeper coverage lives in slice-specific tests).
 */
class MetadataServiceWireUpIT {

    @Test
    void unimplementedRpcsReturnUnimplementedUntilOwnerSliceLands(@TempDir Path dir) throws Exception {
        var broker = TestBrokers.startSingleNode(dir);
        int brokerPort = broker.brokerPort();
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
            // describeRaft is implemented as of .
            assertThat(stub.describeRaft(DescribeRaftRequest.newBuilder().build())
                            .getError())
                    .isEqualTo(ErrorCode.OK);
        } finally {
            channel.shutdown();
            channel.awaitTermination(2, TimeUnit.SECONDS);
            broker.close();
        }
    }
}
