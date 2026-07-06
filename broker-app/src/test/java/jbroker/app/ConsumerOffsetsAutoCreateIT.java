package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import jbroker.app.testkit.TestBrokers;
import jbroker.broker.ConsumerOffsetsTopic;
import jbroker.raft.core.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * proves that {@code __consumer_offsets} is auto-created by the
 * controller within a few seconds of broker startup, with the expected
 * partition count and internal/compact flags, and that it does NOT show up
 * in {@code Admin.ListTopics}.
 */
class ConsumerOffsetsAutoCreateIT {

    @Test
    void autoCreatesConsumerOffsetsTopicWithConfiguredPartitions(@TempDir Path dir) throws Exception {
        // Explicit opt-up to the canonical 50 — the convenience Config
        // overload defaults to 1 to keep existing IT fixtures fast.
        var node = TestBrokers.start(
                (rp, bp) -> new Broker.Config(new NodeId(1), dir, rp, bp).withConsumerOffsetsPartitions(50));
        var broker = node.broker();
        int brokerPort = node.brokerPort();
        try {
            // Tick interval is 1s; allow up to 10s for the create to
            // commit + apply (in single-broker mode the loopback Raft
            // commit is sub-100ms but CI noise can stretch it).
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline
                    && broker.topics().describe(ConsumerOffsetsTopic.NAME).isEmpty()) {
                Thread.sleep(100);
            }
            var desc = broker.topics().describe(ConsumerOffsetsTopic.NAME).orElseThrow();
            assertThat(desc.partitions()).isEqualTo(50);
            assertThat(desc.internal()).isTrue();
            assertThat(desc.compact()).isTrue();
        } finally {
            broker.close();
        }
    }

    @Test
    void singleBrokerConvenienceConfigDefaultsToOnePartition(@TempDir Path dir) throws Exception {
        var broker = TestBrokers.startSingleNode(dir);
        int brokerPort = broker.brokerPort();
        try {
            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline
                    && broker.topics().describe(ConsumerOffsetsTopic.NAME).isEmpty()) {
                Thread.sleep(50);
            }
            var desc = broker.topics().describe(ConsumerOffsetsTopic.NAME).orElseThrow();
            // Test-friendly default — keeps BrokerFencer cycles fast under
            // existing P5/P6 ITs that don't exercise consumer-group routing.
            assertThat(desc.partitions()).isEqualTo(1);
        } finally {
            broker.close();
        }
    }

    @Test
    void adminListTopicsHidesInternalConsumerOffsets(@TempDir Path dir) throws Exception {
        var broker = TestBrokers.startSingleNode(dir);
        int brokerPort = broker.brokerPort();
        try (var client = new jbroker.broker.client.BrokerClient("127.0.0.1", brokerPort)) {
            client.createTopic("orders", 1, 1);

            // Wait for __consumer_offsets to commit so the assertion is
            // meaningful (else listTopics being empty-of-internals would
            // pass trivially).
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline
                    && broker.topics().describe(ConsumerOffsetsTopic.NAME).isEmpty()) {
                Thread.sleep(100);
            }
            assertThat(broker.topics().describe(ConsumerOffsetsTopic.NAME))
                    .as("__consumer_offsets should have been auto-created")
                    .isPresent();

            var listed = client.listTopics();
            assertThat(listed)
                    .extracting(t -> t.getTopic())
                    .containsExactly("orders")
                    .doesNotContain(ConsumerOffsetsTopic.NAME);
        } finally {
            broker.close();
        }
    }
}
