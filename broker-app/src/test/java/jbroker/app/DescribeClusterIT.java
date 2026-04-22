package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import jbroker.proto.broker.DescribeClusterRequest;
import jbroker.proto.broker.DescribeClusterResponse;
import jbroker.proto.broker.MetadataGrpc;
import jbroker.proto.common.ErrorCode;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * P8.2 — {@code Metadata.DescribeCluster} against a live 3-broker cluster.
 * Every broker answers with the same controller id + term within a short
 * settle window; peers appear with role="UNKNOWN" but {@code alive=true}
 * once their first point-to-point heartbeat has been received.
 */
class DescribeClusterIT {

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void everyBrokerReportsTheSameControllerId(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3) throws Exception {
        int r1 = freePort(), r2 = freePort(), r3 = freePort();
        int b1 = freePort(), b2 = freePort(), b3 = freePort();
        var voters = List.of(
                new VoterAddress(new NodeId(1), "127.0.0.1", r1, b1),
                new VoterAddress(new NodeId(2), "127.0.0.1", r2, b2),
                new VoterAddress(new NodeId(3), "127.0.0.1", r3, b3));

        try (var br1 = Broker.start(new Broker.Config(new NodeId(1), d1, r1, b1, voters));
                var br2 = Broker.start(new Broker.Config(new NodeId(2), d2, r2, b2, voters));
                var br3 = Broker.start(new Broker.Config(new NodeId(3), d3, r3, b3, voters))) {

            // Wait for the single Raft leader + every broker to populate its
            // BrokerRegistry + BrokerLiveness entries for the other two.
            long deadline = System.currentTimeMillis() + 15_000;
            boolean settled = false;
            while (System.currentTimeMillis() < deadline) {
                int leaders = (br1.role() == Role.LEADER ? 1 : 0)
                        + (br2.role() == Role.LEADER ? 1 : 0)
                        + (br3.role() == Role.LEADER ? 1 : 0);
                boolean allKnowAll = br1.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3))
                        && br2.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3))
                        && br3.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3));
                if (leaders == 1 && allKnowAll) {
                    settled = true;
                    break;
                }
                Thread.sleep(50);
            }
            assertThat(settled)
                    .as("cluster should settle to 1 leader + full registry")
                    .isTrue();

            int leaderId = br1.role() == Role.LEADER ? 1 : (br2.role() == Role.LEADER ? 2 : 3);

            for (int port : List.of(b1, b2, b3)) {
                var resp = describeCluster(port);
                assertThat(resp.getError()).isEqualTo(ErrorCode.OK);
                // Followers may not yet have observed the leader, but steady
                // state must have them agree on the controller.
                assertThat(resp.getControllerId()).isEqualTo(leaderId);
                assertThat(resp.getCurrentTerm()).isGreaterThan(0L);
                assertThat(resp.getNodesCount()).isEqualTo(3);
            }
        }
    }

    private static DescribeClusterResponse describeCluster(int brokerPort) throws InterruptedException {
        var channel = NettyChannelBuilder.forAddress("127.0.0.1", brokerPort)
                .usePlaintext()
                .build();
        try {
            return MetadataGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .describeCluster(DescribeClusterRequest.newBuilder().build());
        } finally {
            channel.shutdown();
            channel.awaitTermination(2, TimeUnit.SECONDS);
        }
    }
}
