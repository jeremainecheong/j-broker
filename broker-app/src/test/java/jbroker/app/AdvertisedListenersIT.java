package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import jbroker.broker.ConsumerOffsetsTopic;
import jbroker.proto.broker.ConsumerGrpc;
import jbroker.proto.broker.DescribeClusterRequest;
import jbroker.proto.broker.FindCoordinatorRequest;
import jbroker.proto.broker.MetadataGrpc;
import jbroker.raft.core.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * P15.1 — prove {@code BrokerRegistrationRecord.advertised_host / port}
 * flow through to {@code FindCoordinator} and {@code DescribeCluster}
 * replies so external clients see a reachable address, not the broker's
 * inter-broker bind address.
 */
class AdvertisedListenersIT {

    @Test
    void advertisedAddressIsReturnedByFindCoordinator(@TempDir Path dir) throws Exception {
        int brokerPort = freePort();
        int raftPort = freePort();
        // Pretend the broker is also published at another host/port.
        // Inter-broker "host" is 127.0.0.1:<brokerPort>; advertised is the
        // fake string below.
        var voters = List.of(new VoterAddress(
                new NodeId(1),
                "127.0.0.1",
                raftPort,
                brokerPort,
                /*advertisedHost*/ "external.example.com",
                /*advertisedBrokerPort*/ 19092));

        try (var broker = Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort, voters))) {
            awaitCoordinatorTopic(broker);
            awaitBrokerRegistration(broker);

            var channel = NettyChannelBuilder.forAddress("127.0.0.1", brokerPort)
                    .usePlaintext()
                    .build();
            try {
                var consumerStub = ConsumerGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS);
                var metadataStub = MetadataGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS);

                var resp = consumerStub.findCoordinator(
                        FindCoordinatorRequest.newBuilder().setKey("any-group").build());
                assertThat(resp.getCoordinator().getHost())
                        .as("FindCoordinator should return advertised host, not inter-broker bind host")
                        .isEqualTo("external.example.com");
                assertThat(resp.getCoordinator().getPort()).isEqualTo(19092);

                var cluster = metadataStub.describeCluster(
                        DescribeClusterRequest.newBuilder().build());
                assertThat(cluster.getNodesList())
                        .as("DescribeCluster should surface advertised host for external visibility")
                        .anySatisfy(n -> {
                            if (n.getBrokerId() == 1) {
                                assertThat(n.getHost()).isEqualTo("external.example.com");
                                assertThat(n.getPort()).isEqualTo(19092);
                            }
                        });
            } finally {
                channel.shutdown();
                channel.awaitTermination(2, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void withoutAdvertisedListenersFallsBackToInternalAddress(@TempDir Path dir) throws Exception {
        int brokerPort = freePort();
        int raftPort = freePort();
        var voters = List.of(new VoterAddress(new NodeId(1), "127.0.0.1", raftPort, brokerPort));
        try (var broker = Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort, voters))) {
            awaitCoordinatorTopic(broker);
            awaitBrokerRegistration(broker);

            var channel = NettyChannelBuilder.forAddress("127.0.0.1", brokerPort)
                    .usePlaintext()
                    .build();
            try {
                var stub = ConsumerGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS);
                var resp = stub.findCoordinator(
                        FindCoordinatorRequest.newBuilder().setKey("g").build());
                assertThat(resp.getCoordinator().getHost())
                        .as("pre-P15.1 single-network behavior: advertised falls back to internal bind")
                        .isEqualTo("127.0.0.1");
                assertThat(resp.getCoordinator().getPort()).isEqualTo(brokerPort);
            } finally {
                channel.shutdown();
                channel.awaitTermination(2, TimeUnit.SECONDS);
            }
        }
    }

    private static void awaitCoordinatorTopic(Broker broker) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (broker.topics().describe(ConsumerOffsetsTopic.NAME).isPresent()) return;
            Thread.sleep(50);
        }
        throw new AssertionError("__consumer_offsets did not auto-create within 10s");
    }

    private static void awaitBrokerRegistration(Broker broker) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (broker.brokerRegistry().addressFor(1).isPresent()) return;
            Thread.sleep(50);
        }
        throw new AssertionError("broker 1 never registered within 5s");
    }

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
