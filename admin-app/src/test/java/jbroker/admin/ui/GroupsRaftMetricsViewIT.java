package jbroker.admin.ui;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import jbroker.admin.AdminApp;
import jbroker.app.Broker;
import jbroker.app.testkit.BindRetry;
import jbroker.broker.client.BrokerClient;
import jbroker.broker.client.consumer.Consumer;
import jbroker.broker.client.consumer.ConsumerConfig;
import jbroker.broker.client.consumer.OffsetAndMetadata;
import jbroker.broker.client.consumer.RebalanceListener;
import jbroker.broker.client.consumer.StringDeserializer;
import jbroker.proto.common.TopicPartition;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
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
 * P8.8 — HTML-assertion coverage for the remaining Thymeleaf pages:
 * {@code /groups}, {@code /groups/{id}}, {@code /raft}, {@code /metrics}.
 */
@SpringBootTest(
        classes = AdminApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "jbroker.redis.url=")
class GroupsRaftMetricsViewIT {

    private static Broker broker;
    private static Path dataDir;
    private static int brokerPort;

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @BeforeAll
    static void startBroker() throws Exception {
        dataDir = Files.createTempDirectory("p8-8-ui");
        broker = BindRetry.startWithBindRetry(() ->
                Broker.start(new Broker.Config(new NodeId(1), dataDir, BindRetry.freePort(), BindRetry.freePort())));
        brokerPort = broker.brokerPort();
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline && broker.role() != Role.LEADER) {
            Thread.sleep(25);
        }
        waitForCoordinatorTopic(broker);
    }

    @AfterAll
    static void stopBroker() {
        if (broker != null) broker.close();
        if (dataDir != null) deleteQuietly(dataDir);
    }

    @DynamicPropertySource
    static void brokers(DynamicPropertyRegistry registry) {
        registry.add("jbroker.admin.brokers", () -> "localhost:" + brokerPort);
    }

    @Test
    void raftPageRendersTableWithLeaderMarker() {
        var resp = rest.getForEntity("http://localhost:" + port + "/raft", String.class);
        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(resp.getBody()).contains("Raft state");
        assertThat(resp.getBody()).contains("data-node-id=\"1\"");
        assertThat(resp.getBody()).contains("data-node-role=\"LEADER\"");
    }

    @Test
    void metricsPageEmbedsChartCanvases() {
        var resp = rest.getForEntity("http://localhost:" + port + "/metrics", String.class);
        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(resp.getBody()).contains("id=\"throughputChart\"");
        assertThat(resp.getBody()).contains("id=\"latencyChart\"");
        // Page pulls from the live REST endpoints; assert the URLs are wired.
        assertThat(resp.getBody()).contains("/api/v1/metrics/throughput");
        assertThat(resp.getBody()).contains("/api/v1/metrics/latency");
    }

    @Test
    void groupsPageReflectsLiveGroupWithLagCell() throws Exception {
        try (var producer = new BrokerClient("localhost", brokerPort)) {
            producer.createTopic("ui-groups", 2, 1);
            for (int i = 0; i < 20; i++) {
                producer.produce("ui-groups", i % 2, ("v" + i).getBytes());
            }
        }
        var cfg = ConsumerConfig.builder("ui-lag-group", "localhost", brokerPort)
                .pollFetchDeadline(Duration.ofSeconds(5))
                .build();
        try (var consumer = new Consumer<>(cfg, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of("ui-groups"), RebalanceListener.NO_OP);
            long deadline = System.currentTimeMillis() + 10_000;
            while (consumer.assignment().size() < 2 && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(200));
            }
            var commits = new HashMap<TopicPartition, OffsetAndMetadata>();
            commits.put(tp("ui-groups", 0), new OffsetAndMetadata(3L));
            commits.put(tp("ui-groups", 1), new OffsetAndMetadata(4L));
            consumer.commitSync(commits);
        }

        var list = rest.getForEntity("http://localhost:" + port + "/groups", String.class);
        assertThat(list.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(list.getBody()).contains("data-group-id=\"ui-lag-group\"");

        var detail = rest.getForEntity("http://localhost:" + port + "/groups/ui-lag-group", String.class);
        assertThat(detail.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(detail.getBody()).contains("Consumer group: ui-lag-group");
        assertThat(detail.getBody()).contains("data-partition=\"0\"");
        // data-lag attribute carries the computed lag — non-zero after 7 committed / 20 produced.
        assertThat(detail.getBody()).contains("data-lag=");
    }

    @Test
    void unknownGroupReturns404() {
        var resp = rest.getForEntity("http://localhost:" + port + "/groups/does-not-exist", String.class);
        assertThat(resp.getStatusCode().value()).isEqualTo(404);
        assertThat(resp.getBody()).contains("Consumer group not found");
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
