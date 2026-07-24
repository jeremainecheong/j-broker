package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.broker.ProtocolVersion;
import jbroker.proto.broker.ApiVersionsRequest;
import jbroker.proto.broker.DescribeClusterRequest;
import jbroker.proto.broker.DescribeClusterResponse;
import jbroker.proto.broker.MetadataGrpc;
import jbroker.proto.common.ErrorCode;
import jbroker.raft.core.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code Metadata.ApiVersions} against a live 3-broker cluster: every
 * broker answers its supported protocol range immediately after start
 * (no leadership, no settle needed), and once heartbeats have flowed
 * each broker's {@code DescribeCluster} carries the range every peer
 * advertised — so one call tells a client what the whole cluster speaks.
 */
class ApiVersionsIT {

    @Test
    void everyBrokerAnswersItsRangeAndDescribeClusterCarriesAllRanges(
            @TempDir Path d1, @TempDir Path d2, @TempDir Path d3) throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        try (var cluster = TestBrokerCluster.start(
                3,
                2,
                (i, voters, ports) ->
                        new Broker.Config(new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters))) {
            // ApiVersions answers from every broker with no settle wait —
            // version discovery must not depend on elections or metadata.
            for (int i = 0; i < 3; i++) {
                var channel = NettyChannelBuilder.forAddress("127.0.0.1", cluster.brokerPort(i))
                        .usePlaintext()
                        .build();
                try {
                    var resp = MetadataGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(5, TimeUnit.SECONDS)
                            .apiVersions(ApiVersionsRequest.newBuilder().build());
                    assertThat(resp.getError()).isEqualTo(ErrorCode.OK);
                    assertThat(resp.getMinProtocolVersion()).isEqualTo(1);
                    assertThat(resp.getMaxProtocolVersion()).isEqualTo(1);
                    assertThat(resp.getBrokerVersion()).isEqualTo(ProtocolVersion.BROKER_VERSION);
                } finally {
                    channel.shutdown();
                    channel.awaitTermination(2, TimeUnit.SECONDS);
                }
            }

            // Each broker's DescribeCluster eventually reports all three
            // nodes with the range they advertised over heartbeats.
            for (int i = 0; i < 3; i++) {
                var resp = awaitAllRanges(cluster.brokerPort(i));
                assertThat(resp.getNodesCount()).isEqualTo(3);
                for (var node : resp.getNodesList()) {
                    assertThat(node.hasSupportedProtocolMin())
                            .as("broker %d should carry a protocol range", node.getBrokerId())
                            .isTrue();
                    assertThat(node.getSupportedProtocolMin()).isEqualTo(1);
                    assertThat(node.getSupportedProtocolMax()).isEqualTo(1);
                }
            }
        }
    }

    /**
     * Polls DescribeCluster until all 3 nodes appear with a protocol
     * range (peers report one after their first heartbeat, ~250ms).
     */
    private static DescribeClusterResponse awaitAllRanges(int brokerPort) throws InterruptedException {
        var channel = NettyChannelBuilder.forAddress("127.0.0.1", brokerPort)
                .usePlaintext()
                .build();
        try {
            var stub = MetadataGrpc.newBlockingStub(channel);
            DescribeClusterResponse last = null;
            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline) {
                last = stub.withDeadlineAfter(5, TimeUnit.SECONDS)
                        .describeCluster(DescribeClusterRequest.newBuilder().build());
                if (last.getNodesCount() == 3
                        && last.getNodesList().stream().allMatch(n -> n.hasSupportedProtocolMin())) {
                    return last;
                }
                Thread.sleep(50);
            }
            return last;
        } finally {
            channel.shutdown();
            channel.awaitTermination(2, TimeUnit.SECONDS);
        }
    }
}
