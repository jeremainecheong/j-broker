package jbroker.admin.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import jbroker.admin.AdminApp;
import jbroker.admin.api.TopicsController.CreateTopicBody;
import jbroker.app.Broker;
import jbroker.app.VoterAddress;
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
 * PRD §11.3 E2E-8-3 — DELETE {@code /api/v1/topics/{name}} removes the topic
 * cluster-wide; subsequent {@code GET} returns 404 from every broker.
 */
@SpringBootTest(
        classes = AdminApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "jbroker.redis.url=")
class E2E_8_3_DeleteTopicViaRestIT {

    private static Broker br1, br2, br3;
    private static Path d1, d2, d3;
    private static int b1, b2, b3;

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @BeforeAll
    static void bringUp() throws IOException, InterruptedException {
        int r1 = freePort(), r2 = freePort(), r3 = freePort();
        b1 = freePort();
        b2 = freePort();
        b3 = freePort();
        var voters = List.of(
                new VoterAddress(new NodeId(1), "127.0.0.1", r1, b1),
                new VoterAddress(new NodeId(2), "127.0.0.1", r2, b2),
                new VoterAddress(new NodeId(3), "127.0.0.1", r3, b3));
        d1 = Files.createTempDirectory("e2e-8-3-n1");
        d2 = Files.createTempDirectory("e2e-8-3-n2");
        d3 = Files.createTempDirectory("e2e-8-3-n3");
        br1 = Broker.start(new Broker.Config(new NodeId(1), d1, r1, b1, voters));
        br2 = Broker.start(new Broker.Config(new NodeId(2), d2, r2, b2, voters));
        br3 = Broker.start(new Broker.Config(new NodeId(3), d3, r3, b3, voters));

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

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
