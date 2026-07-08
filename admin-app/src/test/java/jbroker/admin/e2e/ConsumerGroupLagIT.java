package jbroker.admin.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import jbroker.admin.AdminApp;
import jbroker.admin.dto.ConsumerGroupDetail;
import jbroker.app.Broker;
import jbroker.app.testkit.BindRetry;
import jbroker.broker.client.BrokerClient;
import jbroker.broker.client.consumer.Consumer;
import jbroker.broker.client.consumer.ConsumerConfig;
import jbroker.broker.client.consumer.RebalanceListener;
import jbroker.broker.client.consumer.StringDeserializer;
import jbroker.proto.common.TopicPartition;
import jbroker.raft.core.NodeId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Produce 1000, consume 400, assert
 * {@code GET /api/v1/consumer-groups/{id}} reports aggregate lag == 600 ± 10.
 */
@SpringBootTest(
        classes = AdminApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "jbroker.redis.url=")
class ConsumerGroupLagIT {

    private static Broker broker;
    private static Path dataDir;
    private static int brokerPort;

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @BeforeAll
    static void bringUp() throws Exception {
        dataDir = Files.createTempDirectory("e2e-8-4-lag");
        broker = BindRetry.startWithBindRetry(() ->
                Broker.start(new Broker.Config(new NodeId(1), dataDir, BindRetry.freePort(), BindRetry.freePort())));
        brokerPort = broker.brokerPort();
        waitForCoordinatorTopic(broker);
    }

    @AfterAll
    static void tearDown() {
        if (broker != null) broker.close();
        if (dataDir != null) deleteQuietly(dataDir);
    }

    @DynamicPropertySource
    static void brokers(DynamicPropertyRegistry registry) {
        registry.add("jbroker.admin.brokers", () -> "localhost:" + brokerPort);
    }

    @Test
    void lagReportsProducedMinusConsumed() throws Exception {
        try (var producer = new BrokerClient("localhost", brokerPort)) {
            producer.createTopic("orders", 3, 1);
            for (int i = 0; i < 1000; i++) {
                int part = i % 3;
                producer.produce("orders", part, ("msg-" + i).getBytes(StandardCharsets.UTF_8));
            }
        }

        // The default per-partition fetch_max_bytes (64 KB) is large enough
        // that a single poll against small records pulls the entire 1000
        // in one round-trip — if we let the consumer auto-advance position
        // we'd commit everything and see lag=0. Drive an explicit commit
        // map totalling 400 records instead.
        var cfg = ConsumerConfig.builder("lag-group", "localhost", brokerPort)
                .pollFetchDeadline(Duration.ofSeconds(5))
                .build();
        try (var consumer = new Consumer<>(cfg, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of("orders"), RebalanceListener.NO_OP);
            // First poll drives join+assignment; subsequent polls drain.
            long deadline = System.currentTimeMillis() + 15_000;
            while (consumer.assignment().size() < 3 && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(200));
            }
            assertThat(consumer.assignment())
                    .as("consumer should own all 3 partitions")
                    .hasSize(3);

            // Produce an explicit commit targeting offset 134/133/133 → 400
            // total. Matches the round-robin distribution above (msg-0, 3, 6, ...
            // on partition 0 → 334 records on partition 0 and 333 each on 1+2).
            var commits = new java.util.HashMap<
                    jbroker.proto.common.TopicPartition, jbroker.broker.client.consumer.OffsetAndMetadata>();
            commits.put(tp("orders", 0), new jbroker.broker.client.consumer.OffsetAndMetadata(134L));
            commits.put(tp("orders", 1), new jbroker.broker.client.consumer.OffsetAndMetadata(133L));
            commits.put(tp("orders", 2), new jbroker.broker.client.consumer.OffsetAndMetadata(133L));
            consumer.commitSync(commits);
        }

        var resp = rest.getForEntity(
                "http://localhost:" + port + "/api/v1/consumer-groups/lag-group", ConsumerGroupDetail.class);
        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
        var detail = resp.getBody();
        assertThat(detail).isNotNull();
        long totalLag = 0L;
        for (var pl : detail.partitions()) {
            assertThat(pl.lag()).as("per-partition lag must be >= 0").isGreaterThanOrEqualTo(0L);
            totalLag += pl.lag();
        }
        // 1000 produced - 400 consumed = 600. Allow ±10 for the in-flight
        // commit/heartbeat timing wobble this assertion is designed to tolerate.
        assertThat(totalLag).as("aggregate lag = produced - committed").isBetween(590L, 610L);
    }

    private static TopicPartition tp(String topic, int partition) {
        return TopicPartition.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .build();
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

    private static void deleteQuietly(Path root) {
        try (var s = Files.walk(root)) {
            s.sorted((a, b) -> b.getNameCount() - a.getNameCount()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best-effort
                }
            });
        } catch (IOException ignored) {
            // best-effort
        }
    }
}
