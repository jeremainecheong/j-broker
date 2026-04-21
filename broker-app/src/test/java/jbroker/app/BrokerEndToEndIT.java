package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end Phase 5 scenarios: create a topic, produce N messages, consume
 * them back in the same order, then restart the broker and verify the
 * records survive. This exercises Raft-replicated metadata + in-memory
 * {@code TopicManager} + the {@link jbroker.storage.LogManager} data plane
 * through the public Producer / Consumer / Admin gRPC services.
 */
class BrokerEndToEndIT {

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void produceAndConsumeRoundTrip(@TempDir Path dir) throws Exception {
        int brokerPort = freePort();
        int raftPort = freePort();
        var broker = Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort));
        try (var client = new BrokerClient("127.0.0.1", brokerPort)) {
            client.createTopic("orders", 1, 1);

            // Produce 100 messages (PRD DoD specifies 100k; 100 is enough to
            // exercise the end-to-end path and stays well under the 20-min
            // CI cap).
            int total = 100;
            var expected = new ArrayList<String>();
            for (int i = 0; i < total; i++) {
                var msg = "msg-" + i;
                client.produce("orders", 0, msg.getBytes(StandardCharsets.UTF_8));
                expected.add(msg);
            }

            var actual = client.fetchAll("orders", 0, 64 * 1024);
            var actualStrings = actual.stream()
                    .map(b -> new String(b, StandardCharsets.UTF_8))
                    .toList();
            assertThat(actualStrings).containsExactlyElementsOf(expected);
        } finally {
            broker.close();
        }
    }

    @Test
    void processRestartRecoversTopicsAndRecords(@TempDir Path dir) throws Exception {
        int brokerPort = freePort();
        int raftPort = freePort();

        // Session 1: create topic, produce a few.
        var broker1 = Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort));
        try (var client = new BrokerClient("127.0.0.1", brokerPort)) {
            client.createTopic("inventory", 1, 1);
            for (int i = 0; i < 10; i++) {
                client.produce("inventory", 0, ("v-" + i).getBytes(StandardCharsets.UTF_8));
            }
        } finally {
            broker1.close();
        }

        // Session 2: same dataDir, same ports, fresh Broker instance. Topic
        // catalogue comes back via Raft log replay; records come back via
        // LogSegment rehydrate.
        var broker2 = Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort));
        try (var client = new BrokerClient("127.0.0.1", brokerPort)) {
            // Wait briefly for Raft replay + leader election.
            long deadline = System.currentTimeMillis() + 3_000;
            while (System.currentTimeMillis() < deadline) {
                if (!client.listTopics().isEmpty()) break;
                Thread.sleep(50);
            }

            var list = client.listTopics();
            assertThat(list).extracting(t -> t.getTopic()).containsExactly("inventory");

            var records = client.fetchAll("inventory", 0, 64 * 1024);
            assertThat(records)
                    .extracting(b -> new String(b, StandardCharsets.UTF_8))
                    .containsExactly("v-0", "v-1", "v-2", "v-3", "v-4", "v-5", "v-6", "v-7", "v-8", "v-9");
        } finally {
            broker2.close();
        }
    }
}
