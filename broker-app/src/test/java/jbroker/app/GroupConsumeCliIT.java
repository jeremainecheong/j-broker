package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import jbroker.broker.ConsumerOffsetsTopic;
import jbroker.broker.client.BrokerClient;
import jbroker.proto.broker.ListConsumerGroupsRequest;
import jbroker.proto.broker.MetadataGrpc;
import jbroker.raft.core.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * end-to-end: the {@code j-broker consume} subcommand is a real
 * consumer-group client (not the raw {@code console-consumer} single-
 * partition Fetch). This IT spins up the loop via the extracted
 * {@link BrokerApp#consumeGroupLoop} helper and asserts that the
 * coordinator registers the group within a few seconds — which is
 * exactly what the admin UI's Groups page renders.
 */
class GroupConsumeCliIT {

    @Test
    void consumeLoopRegistersGroupWithCoordinator(@TempDir Path dir) throws Exception {
        int brokerPort = freePort();
        int raftPort = freePort();
        try (var broker = Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort))) {
            awaitCoordinatorTopic(broker);
            try (var producer = new BrokerClient("127.0.0.1", brokerPort)) {
                producer.createTopic("orders", 1, 1);
                for (int i = 0; i < 10; i++) {
                    producer.produce("orders", 0, ("msg-" + i).getBytes(StandardCharsets.UTF_8));
                }
            }

            var running = new AtomicBoolean(true);
            var bg = Thread.ofVirtual().name("p14-4-consume-loop").start(() -> {
                try {
                    BrokerApp.consumeGroupLoop("127.0.0.1:" + brokerPort, "p14-4-group", List.of("orders"), running);
                } catch (Exception e) {
                    // swallow — consumer shutdown on close() sometimes
                    // throws; test cares about the group showing up.
                }
            });

            try {
                var channel = NettyChannelBuilder.forAddress("127.0.0.1", brokerPort)
                        .usePlaintext()
                        .build();
                try {
                    var stub = MetadataGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS);
                    long deadline = System.currentTimeMillis() + 10_000;
                    boolean seen = false;
                    while (System.currentTimeMillis() < deadline) {
                        var resp = stub.listConsumerGroups(
                                ListConsumerGroupsRequest.newBuilder().build());
                        if (resp.getGroupsList().stream().anyMatch(g -> "p14-4-group".equals(g.getGroupId()))) {
                            seen = true;
                            break;
                        }
                        Thread.sleep(100);
                    }
                    assertThat(seen)
                            .as("coordinator should list p14-4-group within 10s of subscribe")
                            .isTrue();
                } finally {
                    channel.shutdown();
                    channel.awaitTermination(2, TimeUnit.SECONDS);
                }
            } finally {
                running.set(false);
                bg.join(5_000);
            }
        }
    }

    private static void awaitCoordinatorTopic(Broker broker) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (broker.topics().describe(ConsumerOffsetsTopic.NAME).isPresent()
                    && broker.brokerRegistry().addressFor(1).isPresent()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("__consumer_offsets did not auto-create within 10s");
    }

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
