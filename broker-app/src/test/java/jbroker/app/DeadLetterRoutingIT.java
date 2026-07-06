package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import jbroker.app.testkit.TestBrokers;
import jbroker.broker.ConsumerOffsetsTopic;
import jbroker.broker.client.BrokerClient;
import jbroker.broker.client.consumer.Consumer;
import jbroker.broker.client.consumer.ConsumerConfig;
import jbroker.broker.client.consumer.DeadLetterPolicy;
import jbroker.broker.client.consumer.RebalanceListener;
import jbroker.broker.client.consumer.RecordHandler;
import jbroker.broker.client.consumer.RetryableException;
import jbroker.broker.client.consumer.StringDeserializer;
import jbroker.proto.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Dead-letter topic routing.
 *
 * <p>Verifies that when a handler rejects a record {@code maxAttempts}
 * times with {@link RetryableException}, the consumer routes it to the
 * configured DLT with an {@code X-DLT-Failure-Cause} header and commits
 * past the failed offset.
 */
class DeadLetterRoutingIT {

    @Test
    void recordFailingNAttemptsLandsOnDltAndOffsetAdvances(@TempDir Path dir) throws Exception {
        var broker = TestBrokers.startSingleNode(dir);
        int brokerPort = broker.brokerPort();
        try (var producer = new BrokerClient("127.0.0.1", brokerPort)) {
            waitForCoordinatorTopic(broker);
            producer.createTopic("orders", 1, 1);
            producer.createTopic("orders.dlt", 1, 1);

            producer.produce("orders", 0, "bad-msg".getBytes(StandardCharsets.UTF_8));

            var policy = new DeadLetterPolicy("orders.dlt", 3, Duration.ofMillis(10));
            var cfg = ConsumerConfig.builder("g-dlt", "127.0.0.1", brokerPort)
                    .deadLetterPolicy(policy)
                    .build();
            var attempts = new AtomicInteger();
            RecordHandler<String, String> failing = rec -> {
                attempts.incrementAndGet();
                throw new RetryableException("kaboom");
            };
            try (var consumer = new Consumer<>(cfg, new StringDeserializer(), new StringDeserializer())) {
                consumer.subscribe(List.of("orders"), RebalanceListener.NO_OP);

                long deadline = System.currentTimeMillis() + 10_000;
                while (attempts.get() < 3 && System.currentTimeMillis() < deadline) {
                    consumer.poll(Duration.ofMillis(500), failing);
                }
                assertThat(attempts.get())
                        .as("handler should be invoked exactly maxAttempts=3 times")
                        .isEqualTo(3);

                // After DLT routing, source offset is past the failed record.
                var srcTp = TopicPartition.newBuilder()
                        .setTopic("orders")
                        .setPartition(0)
                        .build();
                assertThat(consumer.committed(srcTp).offset()).isEqualTo(1L);

                // A follow-up poll is a steady-state no-op (no new records).
                consumer.poll(Duration.ofMillis(500), failing);
                assertThat(attempts.get())
                        .as("no further retries once record is DLT'd")
                        .isEqualTo(3);
            }

            // The DLT topic now holds the failed record, decorated with the
            // synthesised failure-cause header. Read it back via a second
            // consumer.
            var dltCfg = ConsumerConfig.builder("g-dlt-verify", "127.0.0.1", brokerPort)
                    .build();
            try (var dltConsumer = new Consumer<>(dltCfg, new StringDeserializer(), new StringDeserializer())) {
                dltConsumer.subscribe(List.of("orders.dlt"), RebalanceListener.NO_OP);
                long deadline = System.currentTimeMillis() + 5_000;
                String value = null;
                byte[][] headers = null;
                while (value == null && System.currentTimeMillis() < deadline) {
                    var batch = dltConsumer.poll(Duration.ofMillis(500));
                    for (var rec : batch) {
                        value = rec.value();
                        headers = rec.headers();
                    }
                }
                assertThat(value).isEqualTo("bad-msg");
                assertThat(headers).isNotNull();
                assertThat(headers.length).isGreaterThanOrEqualTo(2);
                // Find X-DLT-Failure-Cause among the key/value pairs.
                String cause = null;
                for (int i = 0; i < headers.length; i += 2) {
                    if (new String(headers[i], StandardCharsets.UTF_8).equals("X-DLT-Failure-Cause")) {
                        cause = new String(headers[i + 1], StandardCharsets.UTF_8);
                        break;
                    }
                }
                assertThat(cause)
                        .as("DLT record carries X-DLT-Failure-Cause header")
                        .isNotNull()
                        .contains("RetryableException")
                        .contains("kaboom");
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
