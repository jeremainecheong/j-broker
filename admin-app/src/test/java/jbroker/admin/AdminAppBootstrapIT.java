package jbroker.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import jbroker.app.Broker;
import jbroker.app.testkit.BindRetry;
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
 * P8.1 — admin-app boots with {@code /actuator/health} returning 200 while
 * pointed at a live in-process broker. Confirms the Spring Boot skeleton is
 * viable so every later slice has a running harness to layer controllers
 * into.
 */
@SpringBootTest(
        classes = AdminApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        // Phase 8 slices P8.2+ add Redis-optional plumbing; for the bootstrap
        // IT we explicitly run without Redis.
        properties = "jbroker.redis.url=")
class AdminAppBootstrapIT {

    private static Broker broker;
    private static Path dataDir;

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @BeforeAll
    static void startBroker() throws Exception {
        dataDir = Files.createTempDirectory("jbroker-admin-bootstrap");
        broker = BindRetry.startWithBindRetry(() ->
                Broker.start(new Broker.Config(new NodeId(1), dataDir, BindRetry.freePort(), BindRetry.freePort())));
        System.setProperty("jbroker.test.brokerPort", Integer.toString(broker.brokerPort()));
    }

    @AfterAll
    static void stopBroker() throws IOException {
        if (broker != null) broker.close();
        if (dataDir != null) deleteRecursively(dataDir);
    }

    @DynamicPropertySource
    static void wireBrokerEndpoint(DynamicPropertyRegistry registry) {
        registry.add("jbroker.admin.brokers", () -> "localhost:" + System.getProperty("jbroker.test.brokerPort"));
    }

    @Test
    void healthEndpointReportsUp() {
        var resp = rest.getForEntity("http://localhost:" + port + "/actuator/health", String.class);
        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(resp.getBody()).contains("\"status\":\"UP\"");
    }

    private static void deleteRecursively(Path root) {
        try (var stream = Files.walk(root)) {
            stream.sorted((a, b) -> b.getNameCount() - a.getNameCount()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best-effort cleanup
                }
            });
        } catch (IOException ignored) {
            // best-effort cleanup
        }
    }
}
