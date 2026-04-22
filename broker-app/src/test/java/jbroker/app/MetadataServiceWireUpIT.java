package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import jbroker.proto.broker.DescribeClusterRequest;
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
 * P8.1 wire-up IT — the Phase 8 {@code Metadata} gRPC surface is reachable on
 * the broker. Every RPC returns {@link ErrorCode#UNIMPLEMENTED} for now;
 * later Phase 8 slices replace the body and add scenario-specific tests.
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
    void everyMetadataRpcIsReachableAndReturnsUnimplemented(@TempDir Path dir) throws Exception {
        int brokerPort = freePort();
        int raftPort = freePort();
        var broker = Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort));
        var channel = NettyChannelBuilder.forAddress("127.0.0.1", brokerPort)
                .usePlaintext()
                .build();
        try {
            var stub = MetadataGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
            assertThat(stub.describeCluster(DescribeClusterRequest.newBuilder().build())
                            .getError())
                    .isEqualTo(ErrorCode.UNIMPLEMENTED);
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
