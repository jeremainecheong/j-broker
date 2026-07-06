package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import jbroker.app.testkit.TestBrokers;
import jbroker.broker.ConsumerOffsetsTopic;
import jbroker.broker.client.BrokerClient;
import jbroker.broker.client.consumer.Consumer;
import jbroker.broker.client.consumer.ConsumerConfig;
import jbroker.broker.client.consumer.RebalanceListener;
import jbroker.broker.client.consumer.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Incremental fetch sessions.
 *
 * <p>Asserts that after the first Fetch per consumer allocates a session on
 * the broker, follow-up Fetches (on any partition that consumer owns) echo
 * the session id and register as incremental hits. Two consumers in the
 * same group, each owning a disjoint subset of partitions, each bootstrap
 * their own session.
 */
class IncrementalFetchSessionIT {

    @Test
    void twoConsumersEachRegisterSessionHitsAfterBootstrap(@TempDir Path dir) throws Exception {
        var broker = TestBrokers.startSingleNode(dir);
        int brokerPort = broker.brokerPort();
        try (var producer = new BrokerClient("127.0.0.1", brokerPort)) {
            waitForCoordinatorTopic(broker);
            producer.createTopic("orders", 4, 1);
            for (int i = 0; i < 20; i++) {
                int p = i % 4;
                producer.produce("orders", p, ("msg-" + i).getBytes(StandardCharsets.UTF_8));
            }

            // Baseline: BrokerClient.fetch has already allocated its own
            // sessions above as part of the idempotency plumbing — we only
            // care about the delta from the two Consumer<K,V> clients below.
            long baseline = broker.metrics().incrementalFetchHits();

            var cfg1 = ConsumerConfig.builder("g-fetch-sessions", "127.0.0.1", brokerPort)
                    .instanceId("c1")
                    .build();
            var cfg2 = ConsumerConfig.builder("g-fetch-sessions", "127.0.0.1", brokerPort)
                    .instanceId("c2")
                    .build();
            try (var c1 = new Consumer<>(cfg1, new StringDeserializer(), new StringDeserializer());
                    var c2 = new Consumer<>(cfg2, new StringDeserializer(), new StringDeserializer())) {
                c1.subscribe(List.of("orders"), RebalanceListener.NO_OP);
                c2.subscribe(List.of("orders"), RebalanceListener.NO_OP);

                // Drive both consumers through several poll ticks so each
                // has bootstrapped a session (first Fetch per partition
                // allocates) and then echoed it on follow-ups.
                long deadline = System.currentTimeMillis() + 8_000;
                while (System.currentTimeMillis() < deadline) {
                    c1.poll(Duration.ofMillis(200));
                    c2.poll(Duration.ofMillis(200));
                    if (!c1.assignment().isEmpty()
                            && !c2.assignment().isEmpty()
                            && broker.metrics().incrementalFetchHits() > baseline + 4) {
                        break;
                    }
                }

                // Both consumers got a slice of the partitions.
                assertThat(c1.assignment()).isNotEmpty();
                assertThat(c2.assignment()).isNotEmpty();

                long observed = broker.metrics().incrementalFetchHits() - baseline;
                assertThat(observed)
                        .as("each consumer's subsequent Fetches should register as incremental hits")
                        .isGreaterThan(0);
            }
        } finally {
            broker.close();
        }
    }

    private static void waitForCoordinatorTopic(Broker broker) throws InterruptedException {
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
}
