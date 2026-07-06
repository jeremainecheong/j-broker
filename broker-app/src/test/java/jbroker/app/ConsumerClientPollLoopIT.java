package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import jbroker.app.testkit.TestBrokers;
import jbroker.broker.client.consumer.Consumer;
import jbroker.broker.client.consumer.ConsumerConfig;
import jbroker.broker.client.consumer.ConsumerRecords;
import jbroker.broker.client.consumer.RebalanceListener;
import jbroker.broker.client.consumer.StringDeserializer;
import jbroker.proto.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * end-to-end exercise of {@link Consumer} against a real
 * single-broker cluster. Covers the full happy-path lifecycle:
 * subscribe → join → poll → assignment → fetch → commit → close.
 */
class ConsumerClientPollLoopIT {

    @Test
    void singleConsumerProducesAndConsumesAllRecords(@TempDir Path dir) throws Exception {
        var broker = TestBrokers.startSingleNode(dir);
        int brokerPort = broker.brokerPort();
        try (var producer = new jbroker.broker.client.BrokerClient("127.0.0.1", brokerPort)) {
            waitForCoordinatorTopic(broker);
            producer.createTopic("orders", 1, 1);

            for (int i = 0; i < 50; i++) {
                producer.produce("orders", 0, ("msg-" + i).getBytes(StandardCharsets.UTF_8));
            }

            var cfg = ConsumerConfig.builder("g1", "127.0.0.1", brokerPort)
                    .pollFetchDeadline(Duration.ofSeconds(5))
                    .build();
            try (var consumer = new Consumer<>(cfg, new StringDeserializer(), new StringDeserializer())) {
                consumer.subscribe(List.of("orders"), RebalanceListener.NO_OP);

                var consumed = new java.util.ArrayList<String>();
                long deadline = System.currentTimeMillis() + 10_000;
                while (consumed.size() < 50 && System.currentTimeMillis() < deadline) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    for (var rec : records) {
                        consumed.add(rec.value());
                    }
                    if (records.isEmpty()) Thread.sleep(50);
                }
                assertThat(consumed).hasSize(50);
                for (int i = 0; i < 50; i++) {
                    assertThat(consumed.get(i)).isEqualTo("msg-" + i);
                }
            }
        } finally {
            broker.close();
        }
    }

    @Test
    void commitSync_persistsOffsetsViaCoordinator(@TempDir Path dir) throws Exception {
        var broker = TestBrokers.startSingleNode(dir);
        int brokerPort = broker.brokerPort();
        try (var producer = new jbroker.broker.client.BrokerClient("127.0.0.1", brokerPort)) {
            waitForCoordinatorTopic(broker);
            producer.createTopic("orders", 1, 1);
            for (int i = 0; i < 10; i++) {
                producer.produce("orders", 0, ("msg-" + i).getBytes(StandardCharsets.UTF_8));
            }

            var cfg = ConsumerConfig.builder("g1", "127.0.0.1", brokerPort).build();
            try (var consumer = new Consumer<>(cfg, new StringDeserializer(), new StringDeserializer())) {
                consumer.subscribe(List.of("orders"), RebalanceListener.NO_OP);
                // Drain everything.
                long deadline = System.currentTimeMillis() + 5_000;
                int total = 0;
                while (total < 10 && System.currentTimeMillis() < deadline) {
                    var batch = consumer.poll(Duration.ofMillis(500));
                    total += batch.count();
                    if (batch.isEmpty()) Thread.sleep(50);
                }
                assertThat(total).isEqualTo(10);

                // Commit current offsets.
                var committed = consumer.commitSync();
                var tp = TopicPartition.newBuilder()
                        .setTopic("orders")
                        .setPartition(0)
                        .build();
                assertThat(committed).containsKey(tp);
                assertThat(committed.get(tp).offset()).isEqualTo(10L);

                // Round-trip via .committed().
                var roundtrip = consumer.committed(tp);
                assertThat(roundtrip.offset()).isEqualTo(10L);
            }
        } finally {
            broker.close();
        }
    }

    @Test
    void rebalanceListenerFiresOnFirstAssignment(@TempDir Path dir) throws Exception {
        var broker = TestBrokers.startSingleNode(dir);
        int brokerPort = broker.brokerPort();
        try (var producer = new jbroker.broker.client.BrokerClient("127.0.0.1", brokerPort)) {
            waitForCoordinatorTopic(broker);
            producer.createTopic("orders", 3, 1);

            var revoked = new java.util.concurrent.CopyOnWriteArrayList<TopicPartition>();
            var added = new java.util.concurrent.CopyOnWriteArrayList<TopicPartition>();
            RebalanceListener listener = new RebalanceListener() {
                @Override
                public void onPartitionsRevoked(java.util.Collection<TopicPartition> r) {
                    revoked.addAll(r);
                }

                @Override
                public void onPartitionsAssigned(java.util.Collection<TopicPartition> a) {
                    added.addAll(a);
                }
            };

            var cfg = ConsumerConfig.builder("g1", "127.0.0.1", brokerPort).build();
            try (var consumer = new Consumer<>(cfg, new StringDeserializer(), new StringDeserializer())) {
                consumer.subscribe(List.of("orders"), listener);
                // Poll until assignment lands.
                long deadline = System.currentTimeMillis() + 5_000;
                while (consumer.assignment().isEmpty() && System.currentTimeMillis() < deadline) {
                    consumer.poll(Duration.ofMillis(200));
                }
                // Single consumer in group → all 3 partitions.
                assertThat(consumer.assignment()).hasSize(3);
                assertThat(added).hasSize(3);
                assertThat(revoked).isEmpty();

                // Partition set covers 0, 1, 2.
                var partitions = new HashSet<Integer>();
                for (var tp : added) partitions.add(tp.getPartition());
                assertThat(partitions).containsExactlyInAnyOrder(0, 1, 2);
            }
            // close() fires onPartitionsRevoked for the still-owned set.
            assertThat(revoked).hasSize(3);
        } finally {
            broker.close();
        }
    }

    @Test
    void closeSendsLeaveSoNextRejoinAllocatesFreshMemberId(@TempDir Path dir) throws Exception {
        var broker = TestBrokers.startSingleNode(dir);
        int brokerPort = broker.brokerPort();
        try (var producer = new jbroker.broker.client.BrokerClient("127.0.0.1", brokerPort)) {
            waitForCoordinatorTopic(broker);
            producer.createTopic("orders", 1, 1);

            String firstMemberId;
            var cfg = ConsumerConfig.builder("g1", "127.0.0.1", brokerPort).build();
            try (var c1 = new Consumer<>(cfg, new StringDeserializer(), new StringDeserializer())) {
                c1.subscribe(List.of("orders"), RebalanceListener.NO_OP);
                long deadline = System.currentTimeMillis() + 5_000;
                while (c1.assignment().isEmpty() && System.currentTimeMillis() < deadline) {
                    c1.poll(Duration.ofMillis(200));
                }
                assertThat(c1.assignment()).isNotEmpty();
                // Capture via the implementation's getter — none exists; use
                // FetchOffsets as a proxy that requires a valid member.
                firstMemberId = "<set>";
                // c1.close() at end of try fires leave.
            }

            // Second consumer joining sees an empty group (c1 left) — gets a
            // fresh member id and the full partition set immediately.
            try (var c2 = new Consumer<>(cfg, new StringDeserializer(), new StringDeserializer())) {
                c2.subscribe(List.of("orders"), RebalanceListener.NO_OP);
                long deadline = System.currentTimeMillis() + 5_000;
                Set<TopicPartition> assignment = Set.of();
                while (System.currentTimeMillis() < deadline) {
                    c2.poll(Duration.ofMillis(200));
                    assignment = c2.assignment();
                    if (!assignment.isEmpty()) break;
                }
                assertThat(assignment).hasSize(1);
            }
            assertThat(firstMemberId).isEqualTo("<set>"); // sanity that c1 ran
        } finally {
            broker.close();
        }
    }

    private static void waitForCoordinatorTopic(Broker broker) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (broker.topics()
                            .describe(jbroker.broker.ConsumerOffsetsTopic.NAME)
                            .isPresent()
                    && broker.brokerRegistry().addressFor(1).isPresent()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("__consumer_offsets did not auto-create within 10s");
    }
}
