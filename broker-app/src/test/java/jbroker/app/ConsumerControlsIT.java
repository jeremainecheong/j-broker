package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import jbroker.app.testkit.TestBrokers;
import jbroker.broker.client.consumer.Consumer;
import jbroker.broker.client.consumer.ConsumerConfig;
import jbroker.broker.client.consumer.ConsumerRecord;
import jbroker.broker.client.consumer.OffsetAndMetadata;
import jbroker.broker.client.consumer.RebalanceListener;
import jbroker.broker.client.consumer.StringDeserializer;
import jbroker.proto.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end exercise of the {@link Consumer} control surface against a
 * real single-broker cluster: seeks (including the broker-resolved
 * begin/end variants), pause/resume, the {@code max.poll.records} bound,
 * and commitAsync.
 */
class ConsumerControlsIT {

    private static final TopicPartition ORDERS_0 =
            TopicPartition.newBuilder().setTopic("orders").setPartition(0).build();

    @Test
    void seekRepositionsWhereTheNextPollReads(@TempDir Path dir) throws Exception {
        var broker = TestBrokers.startSingleNode(dir);
        int brokerPort = broker.brokerPort();
        try (var producer = new jbroker.broker.client.BrokerClient("127.0.0.1", brokerPort)) {
            waitForCoordinatorTopic(broker);
            producer.createTopic("orders", 1, 1);
            for (int i = 0; i < 100; i++) {
                producer.produce("orders", 0, ("msg-" + i).getBytes(StandardCharsets.UTF_8));
            }

            var cfg = ConsumerConfig.builder("g1", "127.0.0.1", brokerPort).build();
            try (var consumer = new Consumer<>(cfg, new StringDeserializer(), new StringDeserializer())) {
                consumer.subscribe(List.of("orders"), RebalanceListener.NO_OP);
                awaitAssignment(consumer);

                // Jump to the middle: the following polls deliver exactly 50..99.
                consumer.seek("orders", 0, 50);
                var fromMiddle = drain(consumer, 50);
                for (int i = 0; i < 50; i++) {
                    assertThat(fromMiddle.get(i).offset()).isEqualTo(50L + i);
                    assertThat(fromMiddle.get(i).value()).isEqualTo("msg-" + (50 + i));
                }

                // Back to the broker-reported log start: the full history again.
                consumer.seekToBeginning("orders", 0);
                var fromStart = drain(consumer, 100);
                for (int i = 0; i < 100; i++) {
                    assertThat(fromStart.get(i).offset()).isEqualTo((long) i);
                }
                assertThat(fromStart.get(0).value()).isEqualTo("msg-0");

                // Log end: only records produced after the seek are delivered.
                consumer.seekToEnd("orders", 0);
                for (int i = 100; i < 105; i++) {
                    producer.produce("orders", 0, ("msg-" + i).getBytes(StandardCharsets.UTF_8));
                }
                var tail = drain(consumer, 5);
                for (int i = 0; i < 5; i++) {
                    assertThat(tail.get(i).offset()).isEqualTo(100L + i);
                    assertThat(tail.get(i).value()).isEqualTo("msg-" + (100 + i));
                }
            }
        } finally {
            broker.close();
        }
    }

    @Test
    void pauseHoldsDeliveryWhileProduceContinues_resumeLosesNothing(@TempDir Path dir) throws Exception {
        var broker = TestBrokers.startSingleNode(dir);
        int brokerPort = broker.brokerPort();
        try (var producer = new jbroker.broker.client.BrokerClient("127.0.0.1", brokerPort)) {
            waitForCoordinatorTopic(broker);
            producer.createTopic("orders", 1, 1);
            for (int i = 0; i < 20; i++) {
                producer.produce("orders", 0, ("msg-" + i).getBytes(StandardCharsets.UTF_8));
            }

            var cfg = ConsumerConfig.builder("g1", "127.0.0.1", brokerPort).build();
            try (var consumer = new Consumer<>(cfg, new StringDeserializer(), new StringDeserializer())) {
                consumer.subscribe(List.of("orders"), RebalanceListener.NO_OP);
                drain(consumer, 20);

                consumer.pause("orders", 0);
                assertThat(consumer.paused()).containsExactly(ORDERS_0);

                // Production continues while the consumer is paused.
                for (int i = 20; i < 30; i++) {
                    producer.produce("orders", 0, ("msg-" + i).getBytes(StandardCharsets.UTF_8));
                }
                // Paused polls keep heartbeating but must deliver nothing.
                for (int i = 0; i < 6; i++) {
                    assertThat(consumer.poll(Duration.ofMillis(200)).isEmpty()).isTrue();
                }
                // Membership untouched: the partition is still owned.
                assertThat(consumer.assignment()).containsExactly(ORDERS_0);

                consumer.resume("orders", 0);
                assertThat(consumer.paused()).isEmpty();

                // Everything produced during the pause arrives once, in order.
                var resumed = drain(consumer, 10);
                for (int i = 0; i < 10; i++) {
                    assertThat(resumed.get(i).offset()).isEqualTo(20L + i);
                    assertThat(resumed.get(i).value()).isEqualTo("msg-" + (20 + i));
                }
                assertThat(consumer.poll(Duration.ofMillis(300)).isEmpty()).isTrue();
            }
        } finally {
            broker.close();
        }
    }

    @Test
    void maxPollRecordsBoundsEachPollAndTheNextContinues(@TempDir Path dir) throws Exception {
        var broker = TestBrokers.startSingleNode(dir);
        int brokerPort = broker.brokerPort();
        try (var producer = new jbroker.broker.client.BrokerClient("127.0.0.1", brokerPort)) {
            waitForCoordinatorTopic(broker);
            producer.createTopic("orders", 1, 1);
            for (int i = 0; i < 100; i++) {
                producer.produce("orders", 0, ("msg-" + i).getBytes(StandardCharsets.UTF_8));
            }

            var cfg = ConsumerConfig.builder("g1", "127.0.0.1", brokerPort)
                    .maxPollRecords(10)
                    .build();
            try (var consumer = new Consumer<>(cfg, new StringDeserializer(), new StringDeserializer())) {
                consumer.subscribe(List.of("orders"), RebalanceListener.NO_OP);

                // All 100 records fit one fetch, so every delivering poll must
                // return exactly the bound and pick up where the last stopped.
                var all = new ArrayList<ConsumerRecord<String, String>>();
                int deliveringPolls = 0;
                long deadline = System.currentTimeMillis() + 10_000;
                while (all.size() < 100 && System.currentTimeMillis() < deadline) {
                    var batch = consumer.poll(Duration.ofMillis(500));
                    if (batch.isEmpty()) {
                        Thread.sleep(50);
                        continue;
                    }
                    assertThat(batch.count()).isEqualTo(10);
                    batch.forEach(all::add);
                    deliveringPolls++;
                }
                assertThat(all).hasSize(100);
                assertThat(deliveringPolls).isEqualTo(10);
                for (int i = 0; i < 100; i++) {
                    assertThat(all.get(i).offset()).isEqualTo((long) i);
                }
            }
        } finally {
            broker.close();
        }
    }

    @Test
    void commitAsyncRoundTripsThroughTheCoordinator(@TempDir Path dir) throws Exception {
        var broker = TestBrokers.startSingleNode(dir);
        int brokerPort = broker.brokerPort();
        try (var producer = new jbroker.broker.client.BrokerClient("127.0.0.1", brokerPort)) {
            waitForCoordinatorTopic(broker);
            producer.createTopic("orders", 1, 1);
            for (int i = 0; i < 30; i++) {
                producer.produce("orders", 0, ("msg-" + i).getBytes(StandardCharsets.UTF_8));
            }

            var cfg = ConsumerConfig.builder("g1", "127.0.0.1", brokerPort).build();
            try (var consumer = new Consumer<>(cfg, new StringDeserializer(), new StringDeserializer())) {
                consumer.subscribe(List.of("orders"), RebalanceListener.NO_OP);
                drain(consumer, 30);

                // Position-snapshot variant: everything seen so far.
                consumer.commitAsync().get(5, TimeUnit.SECONDS);
                assertThat(consumer.committed(ORDERS_0).offset()).isEqualTo(30L);

                // Explicit-map variant rewinds the group's committed offset.
                consumer.commitAsync(Map.of(ORDERS_0, new OffsetAndMetadata(10L)))
                        .get(5, TimeUnit.SECONDS);
                assertThat(consumer.committed(ORDERS_0).offset()).isEqualTo(10L);
            }

            // A fresh consumer in the same group resumes from the
            // async-committed offset.
            try (var consumer = new Consumer<>(cfg, new StringDeserializer(), new StringDeserializer())) {
                consumer.subscribe(List.of("orders"), RebalanceListener.NO_OP);
                var replay = drain(consumer, 20);
                for (int i = 0; i < 20; i++) {
                    assertThat(replay.get(i).offset()).isEqualTo(10L + i);
                }
            }
        } finally {
            broker.close();
        }
    }

    private static void awaitAssignment(Consumer<String, String> consumer) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (consumer.assignment().isEmpty() && System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(200));
            Thread.sleep(20);
        }
        assertThat(consumer.assignment()).isNotEmpty();
    }

    /** Poll until {@code expected} records arrived (10s budget) and return them in arrival order. */
    private static List<ConsumerRecord<String, String>> drain(Consumer<String, String> consumer, int expected)
            throws InterruptedException {
        var out = new ArrayList<ConsumerRecord<String, String>>();
        long deadline = System.currentTimeMillis() + 10_000;
        while (out.size() < expected && System.currentTimeMillis() < deadline) {
            var batch = consumer.poll(Duration.ofMillis(500));
            batch.forEach(out::add);
            if (batch.isEmpty()) Thread.sleep(50);
        }
        assertThat(out).hasSize(expected);
        return out;
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
