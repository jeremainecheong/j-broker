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
 * /wire-up IT — the Milestone 8 {@code Metadata} gRPC surface is
 * reachable on the broker. {@code DescribeCluster} is implemented as of
 * (tested in {@link DescribeClusterIT}); every other RPC continues
 * to return {@link ErrorCode#UNIMPLEMENTED} until its owning slice lands.
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
            assertThat(stub.describeTopicPartitions(DescribeTopicPartitionsRequest.newBuilder()
                                    .setTopic("orders")
                                    .build())
                            .getError())
                    .isEqualTo(ErrorCode.UNIMPLEMENTED);
            assertThat(stub.listConsumerGroups(
                                    ListConsumerGroupsRequest.newBuilder().build())
                            .getError())
                    .isEqualTo(ErrorCode.UNIMPLEMENTED);
            assertThat(stub.describeConsumerGroup(DescribeConsumerGroupRequest.newBuilder()
                                    .setGroupId("g1")
                                    .build())
                            .getError())
                    .isEqualTo(ErrorCode.UNIMPLEMENTED);
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
