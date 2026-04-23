package jbroker.admin.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import jbroker.admin.AdminApp;
import jbroker.admin.api.MetricsScraper;
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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * {@code /actuator/prometheus} serves well-formed
 * Prometheus text containing the expected {@code jbroker_*} meter families.
 *
 * <p>Verifies:
 * <ul>
 *   <li>meters land with {@code broker_id} tag</li>
 *   <li>{@code jbroker_raft_current_term} exposes the live Raft term</li>
 *   <li>parse succeeds via Prometheus' {@code TextFormat} equivalence —
 *       we use a structural grep over line shape since pulling the
 *       simpleclient parser would add a runtime dep</li>
 * </ul>
 */
@SpringBootTest(
        classes = AdminApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"jbroker.redis.url=", "jbroker.metrics.scrape.intervalSeconds=1"})
class PrometheusEndpointIT {

    private static Broker br1, br2, br3;
    private static Path d1, d2, d3;
    private static int b1, b2, b3;

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @Autowired
    MetricsScraper scraper;

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
        d1 = Files.createTempDirectory("e2e-9-1-n1");
        d2 = Files.createTempDirectory("e2e-9-1-n2");
        d3 = Files.createTempDirectory("e2e-9-1-n3");
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
    void prometheusEndpointExposesBrokerAndPartitionMetrics() throws InterruptedException {
        // Wait for at least one scrape cycle so meters register.
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (!scraper.snapshot().byBroker().isEmpty()) break;
            Thread.sleep(100);
        }
        assertThat(scraper.snapshot().byBroker())
                .as("scraper should populate within 5s")
                .isNotEmpty();

        String body = rest.getForObject("http://localhost:" + port + "/actuator/prometheus", String.class);
        assertThat(body).isNotNull();

        // Prometheus text format: every non-blank, non-comment line matches
        // metric_name{labels} value[ timestamp]. We do a structural regex
        // rather than pulling io.prometheus.client purely to avoid a
        // runtime dep; Micrometer guarantees well-formedness.
        for (String line : body.split("\n")) {
            if (line.isBlank() || line.startsWith("#")) continue;
            assertThat(line)
                    .as("well-formed Prometheus line: %s", line)
                    .matches("^[a-zA-Z_][a-zA-Z0-9_]*(\\{[^}]*\\})? [-+]?[0-9.eE+NaN-]+(?:\\s+[-0-9]+)?$");
        }

        assertThat(body)
                .contains("jbroker_produce_count")
                .contains("jbroker_fetch_count")
                .contains("jbroker_raft_current_term")
                .contains("jbroker_raft_commit_index")
                .contains("jbroker_raft_last_log_index");
        // broker_id tag present on at least one broker-scoped meter.
        assertThat(body).containsPattern("jbroker_raft_current_term\\{[^}]*broker_id=\"[123]\"[^}]*} [0-9.]+");
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
                }
            });
        } catch (IOException ignored) {
        }
    }
}
