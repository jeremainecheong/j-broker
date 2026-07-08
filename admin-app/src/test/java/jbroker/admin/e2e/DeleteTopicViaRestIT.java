package jbroker.admin.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import jbroker.admin.AdminApp;
import jbroker.admin.api.TopicsController.CreateTopicBody;
import jbroker.app.Broker;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * DELETE {@code /api/v1/topics/{name}} removes the topic
 * cluster-wide; subsequent {@code GET} returns 404 from every broker.
 */
@SpringBootTest(
        classes = AdminApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "jbroker.redis.url=")
class DeleteTopicViaRestIT {

    private static Broker br1, br2, br3;
    private static Path d1, d2, d3;
    private static int b1, b2, b3;

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @BeforeAll
    static void bringUp() throws Exception {
        d1 = Files.createTempDirectory("delete-topic-n1");
        d2 = Files.createTempDirectory("delete-topic-n2");
        d3 = Files.createTempDirectory("delete-topic-n3");
        var dirs = new Path[] {d1, d2, d3};
        var cluster = TestBrokerCluster.start(
                3,
                2,
                (i, voters, ports) -> new Broker.Config(new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters));
        br1 = cluster.broker(0);
        br2 = cluster.broker(1);
        br3 = cluster.broker(2);
        b1 = cluster.brokerPort(0);
        b2 = cluster.brokerPort(1);
        b3 = cluster.brokerPort(2);

        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            int leaders = (br1.role() == Role.LEADER ? 1 : 0)
                    + (br2.role() == Role.LEADER ? 1 : 0)
                    + (br3.role() == Role.LEADER ? 1 : 0);
            boolean full = br1.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3))
                    && br2.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3))
                    && br3.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3));
            if (leaders == 1 && full) break;
            Thread.sleep(50);
        }
    }

    @AfterAll
    static void tearDown() {
        if (br1 != null) br1.close();
        if (br2 != null) br2.close();
        if (br3 != null) br3.close();
        for (var p : List.of(d1, d2, d3)) {
            if (p != null) deleteQuietly(p);
        }
    }

    @DynamicPropertySource
    static void brokers(DynamicPropertyRegistry registry) {
        registry.add("jbroker.admin.brokers", () -> "localhost:" + b1 + ",localhost:" + b2 + ",localhost:" + b3);
    }

    @Test
    void createAndDeleteViaRestRemovesTopicClusterWide() throws InterruptedException {
        var body = new CreateTopicBody("audits", 2, 1, Map.of());
        var headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        var create = rest.postForEntity(
                "http://localhost:" + port + "/api/v1/topics", new HttpEntity<>(body, headers), String.class);
        assertThat(create.getStatusCode().value()).isEqualTo(201);

        // Wait for all brokers to see the topic before deleting.
        long deadline = System.currentTimeMillis() + 2_000;
        while (System.currentTimeMillis() < deadline) {
            if (br1.topics().exists("audits")
                    && br2.topics().exists("audits")
                    && br3.topics().exists("audits")) {
                break;
            }
            Thread.sleep(25);
        }
        assertThat(br1.topics().exists("audits")).isTrue();

        var del = rest.exchange(
                "http://localhost:" + port + "/api/v1/topics/audits",
                HttpMethod.DELETE,
                HttpEntity.EMPTY,
                String.class);
        assertThat(del.getStatusCode().value()).isEqualTo(204);

        // Within 2 s every broker should have dropped the topic.
        long delDeadline = System.currentTimeMillis() + 2_000;
        while (System.currentTimeMillis() < delDeadline) {
            if (!br1.topics().exists("audits")
                    && !br2.topics().exists("audits")
                    && !br3.topics().exists("audits")) {
                break;
            }
            Thread.sleep(25);
        }
        assertThat(br1.topics().exists("audits")).isFalse();
        assertThat(br2.topics().exists("audits")).isFalse();
        assertThat(br3.topics().exists("audits")).isFalse();

        // UI path: GET /api/v1/topics/audits → 404 on every broker.
        var get = rest.getForEntity("http://localhost:" + port + "/api/v1/topics/audits", String.class);
        assertThat(get.getStatusCode().value()).isEqualTo(404);
        assertThat(get.getBody()).contains("UNKNOWN_TOPIC");
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
