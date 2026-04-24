package jbroker.admin.ui;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import jbroker.admin.AdminApp;
import jbroker.app.Broker;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Admin-UI pin for the topic-detail page "Force compact" button and the
 * "Edit config" modal (P-admin-topic-actions).
 *
 * <p>Scope: HTML shape + smoke-test that the compact REST route the button
 * wires to still responds 200. The deeper REST-layer semantics are already
 * covered by {@code ForceCompactEndpointIT} (records_kept serialisation,
 * UNKNOWN_TOPIC 4xx), so we don't duplicate those here.
 *
 * <p>Also re-asserts the {@code x-cloak} contract on the new modal since
 * {@link AlpineCloakIT} only checks {@code topics.html} — a future refactor
 * could strip it off {@code topic-detail.html} without anyone noticing.
 */
@SpringBootTest(
        classes = AdminApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "jbroker.redis.url=")
class TopicDetailActionsViewIT {

    private static Broker broker;
    private static Path dataDir;
    private static int brokerPort;

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @BeforeAll
    static void startBroker() throws IOException, InterruptedException {
        brokerPort = freePort();
        int raftPort = freePort();
        dataDir = Files.createTempDirectory("admin-ui-topic-actions");
        broker = Broker.start(new Broker.Config(new NodeId(1), dataDir, raftPort, brokerPort));
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline && broker.role() != Role.LEADER) {
            Thread.sleep(25);
        }
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
    void topicDetailExposesForceCompactAndEditConfigControls() throws Exception {
        try (var client = new BrokerClient("localhost", brokerPort)) {
            client.createTopic("action-ui", 2, 1);
        }

        var html = rest.getForObject("http://localhost:" + port + "/topics/action-ui", String.class);
        assertThat(html).isNotNull();

        // 1) Per-partition compact button is wired to the existing REST route
        // for every partition. Both p0 and p1 must carry their own hx-post.
        assertThat(html)
                .as("force-compact button must exist on p0")
                .contains("hx-post=\"/api/v1/topics/action-ui/partitions/0/compact\"")
                .as("force-compact button must exist on p1")
                .contains("hx-post=\"/api/v1/topics/action-ui/partitions/1/compact\"")
                .contains("class=\"btn secondary partition-compact-btn\"");

        // 2) Shared flash target + x-cloak modal. AlpineCloakIT only pins
        // topics.html; replicate the guard here for topic-detail.html.
        assertThat(html)
                .as("shared flash div must exist for the compact+PATCH results")
                .contains("id=\"topic-flash\"")
                .as("edit-config modal must gate on x-cloak to avoid first-paint flash")
                .contains("x-cloak")
                .as("edit-config button opens the modal via Alpine state")
                .contains("@click=\"showEditConfig = true\"")
                .as("modal form carries the topic name so the inline PATCH handler can build the URL")
                .contains("id=\"edit-config-form\"")
                .contains("data-topic-name=\"action-ui\"");
    }

    @Test
    void forceCompactRestRouteStillReturns200() throws Exception {
        try (var client = new BrokerClient("localhost", brokerPort)) {
            client.createTopic("action-ui-2", 1, 1);
            // At least one record so the broker has a warm log — otherwise
            // the endpoint returns NO_REPLICA_HOSTS_PARTITION (404).
            client.produce("action-ui-2", 0, "seed".getBytes());
        }
        var resp = rest.postForEntity(
                "http://localhost:" + port + "/api/v1/topics/action-ui-2/partitions/0/compact", null, String.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
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
