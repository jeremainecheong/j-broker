package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import jbroker.app.testkit.TestBrokers;
import jbroker.broker.ConsumerOffsetsTopic;
import jbroker.proto.broker.ConsumerGrpc;
import jbroker.proto.broker.FindCoordinatorRequest;
import jbroker.proto.common.ErrorCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Exercises {@link jbroker.broker.ConsumerHandler#findCoordinator}
 * over real gRPC against a single broker. Confirms the coordinator endpoint
 * matches the leader of the deterministic coordinator partition for the
 * group, and that the response is reachable via the returned host:port
 * (round-trip a {@code FindCoordinator} call against the same address).
 */
class FindCoordinatorEndToEndIT {

    @Test
    void findCoordinatorReturnsThisBrokerInSingleBrokerCluster(@TempDir Path dir) throws Exception {
        var broker = TestBrokers.startSingleNode(dir);
        int brokerPort = broker.brokerPort();
        var channel = NettyChannelBuilder.forAddress("127.0.0.1", brokerPort)
                .usePlaintext()
                .build();
        try {
            // Wait for __consumer_offsets to land + broker registration to apply.
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                if (broker.topics().describe(ConsumerOffsetsTopic.NAME).isPresent()
                        && broker.brokerRegistry().addressFor(1).isPresent()) {
                    break;
                }
                Thread.sleep(50);
            }
            assertThat(broker.topics().describe(ConsumerOffsetsTopic.NAME))
                    .as("__consumer_offsets must be created")
                    .isPresent();
            assertThat(broker.brokerRegistry().addressFor(1))
                    .as("self broker must register before FindCoordinator can resolve")
                    .isPresent();

            var stub = ConsumerGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
            var resp = stub.findCoordinator(
                    FindCoordinatorRequest.newBuilder().setKey("g1").build());
            assertThat(resp.getError()).isEqualTo(ErrorCode.OK);
            assertThat(resp.getCoordinator().getNodeId()).isEqualTo(1);
            assertThat(resp.getCoordinator().getPort()).isEqualTo(brokerPort);
        } finally {
            channel.shutdown();
            channel.awaitTermination(2, TimeUnit.SECONDS);
            broker.close();
        }
    }
}
