package jbroker.admin.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import jbroker.admin.AdminApp;
import jbroker.app.Broker;
import jbroker.app.testkit.BindRetry;
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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * The UI topology page renders and reflects live cluster
 * state. Asserted via HTML content scraping (no screenshots) so CI doesn't
 * need a browser runtime.
 */
@SpringBootTest(
        classes = AdminApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "jbroker.redis.url=")
class UiTopologyPageIT {

    private static Broker broker;
    private static Path dataDir;
    private static int brokerPort;

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @BeforeAll
    static void startBroker() throws Exception {
        dataDir = Files.createTempDirectory("e2e-8-7-ui");
        broker = BindRetry.startWithBindRetry(() ->
                Broker.start(new Broker.Config(new NodeId(1), dataDir, BindRetry.freePort(), BindRetry.freePort())));
        brokerPort = broker.brokerPort();
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
    void indexPageRendersSvgWithControllerRoleMarker() {
        var resp = rest.getForEntity("http://localhost:" + port + "/", String.class);
        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
        var body = resp.getBody();
        assertThat(body).isNotNull();
        assertThat(body).contains("<svg");
        assertThat(body).contains("data-broker-id=\"1\"");
        assertThat(body).contains("data-broker-role=\"LEADER\"");
        // Topology nav links are present.
        assertThat(body).contains("href=\"/topics\"");
        assertThat(body).contains("href=\"/groups\"");
        assertThat(body).contains("href=\"/raft\"");
    }

    @Test
    void topicsPageRendersTableAndReflectsCreatedTopic() throws Exception {
        try (var admin = new BrokerClient("localhost", brokerPort)) {
            admin.createTopic("ui-orders", 2, 1);
        }
        var resp = rest.getForEntity("http://localhost:" + port + "/topics", String.class);
        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(resp.getBody()).contains("data-topic-name=\"ui-orders\"");
        assertThat(resp.getBody()).contains("+ Create topic");
    }

    @Test
    void topicDetailPageRendersPartitionStates() throws Exception {
        try (var admin = new BrokerClient("localhost", brokerPort)) {
            admin.createTopic("ui-detail", 3, 1);
        }
        var resp = rest.getForEntity("http://localhost:" + port + "/topics/ui-detail", String.class);
        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(resp.getBody()).contains("Topic: ui-detail");
        assertThat(resp.getBody()).contains("data-partition=\"0\"");
        assertThat(resp.getBody()).contains("data-partition=\"1\"");
        assertThat(resp.getBody()).contains("data-partition=\"2\"");
    }

    @Test
    void topicDetailForUnknownReturns404() {
        var resp = rest.getForEntity("http://localhost:" + port + "/topics/does-not-exist", String.class);
        assertThat(resp.getStatusCode().value()).isEqualTo(404);
        assertThat(resp.getBody()).contains("Topic not found");
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
