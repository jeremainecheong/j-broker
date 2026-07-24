package jbroker.admin.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import jbroker.admin.AdminApp;
import jbroker.admin.api.MetricsScraper;
import jbroker.app.Broker;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * {@code /actuator/prometheus} serves well-formed Prometheus text
 * containing the expected {@code jbroker_*} meter families.
 *
 * <p>Verifies:
 * <ul>
 *   <li>meters land with {@code broker_id} tag</li>
 *   <li>{@code jbroker_raft_current_term} exposes the live Raft term</li>
 *   <li>{@code jbroker_disk_usable_bytes} / {@code jbroker_disk_headroom_low}
 *       carry the broker-reported storage headroom</li>
 *   <li>{@code jbroker_broker_scrape_ok} is 1 while a broker answers the
 *       scrape fan-out and flips to 0 once it stops</li>
 *   <li>parse succeeds via Prometheus' {@code TextFormat} equivalence —
 *       we use a structural grep over line shape since pulling the
 *       simpleclient parser would add a runtime dep</li>
 * </ul>
 *
 * <p>Methods are ordered: the scrape-ok flip stops broker 3, so it must
 * run after the assertions that expect all three brokers live.
 */
@SpringBootTest(
        classes = AdminApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"jbroker.redis.url=", "jbroker.metrics.scrape.intervalSeconds=1"})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
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
    static void bringUp() throws Exception {
        d1 = Files.createTempDirectory("prometheus-n1");
        d2 = Files.createTempDirectory("prometheus-n2");
        d3 = Files.createTempDirectory("prometheus-n3");
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
    @Order(1)
    void prometheusEndpointExposesBrokerAndPartitionMetrics() throws InterruptedException {
        // Wait for a scrape cycle that reached all three brokers. Polling
        // the endpoint body (not just the scraper snapshot) also covers
        // the gap between snapshot publication and meter registration.
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (scraper.snapshot().byBroker().keySet().containsAll(List.of(1, 2, 3))) break;
            Thread.sleep(100);
        }
        assertThat(scraper.snapshot().byBroker().keySet())
                .as("scraper should reach all three brokers within 10s")
                .containsAll(List.of(1, 2, 3));
        String body = awaitGauge("jbroker_broker_scrape_ok", 3, 1.0, 10_000);

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

        for (int id : List.of(1, 2, 3)) {
            assertThat(gaugeValue(body, "jbroker_disk_usable_bytes", id))
                    .as("jbroker_disk_usable_bytes for broker %s reflects a live volume probe", id)
                    .isNotNull()
                    .isGreaterThan(0.0);
            assertThat(gaugeValue(body, "jbroker_disk_headroom_low", id))
                    .as("jbroker_disk_headroom_low for broker %s is a 0/1 flag", id)
                    .isNotNull()
                    .isIn(0.0, 1.0);
            assertThat(gaugeValue(body, "jbroker_broker_scrape_ok", id))
                    .as("jbroker_broker_scrape_ok for live broker %s", id)
                    .isEqualTo(1.0);
        }
    }

    @Test
    @Order(2)
    void scrapeOkFlipsToZeroWhenBrokerStops() throws InterruptedException {
        // Re-establish the live baseline so the test stands alone.
        awaitGauge("jbroker_broker_scrape_ok", 3, 1.0, 10_000);

        br3.close();
        br3 = null;

        // The next fan-out fails against the closed port; broker 3 must
        // flip to 0 while the surviving brokers stay at 1.
        String body = awaitGauge("jbroker_broker_scrape_ok", 3, 0.0, 20_000);
        assertThat(gaugeValue(body, "jbroker_broker_scrape_ok", 1))
                .as("live broker 1 keeps scrape_ok=1")
                .isEqualTo(1.0);
        assertThat(gaugeValue(body, "jbroker_broker_scrape_ok", 2))
                .as("live broker 2 keeps scrape_ok=1")
                .isEqualTo(1.0);
    }

    /**
     * Polls {@code /actuator/prometheus} until {@code metric{broker_id}}
     * reports {@code expected}; returns the body that satisfied it. Fails
     * with the last observed value once the deadline passes.
     */
    private String awaitGauge(String metric, int brokerId, double expected, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        String body = "";
        Double last = null;
        while (System.currentTimeMillis() < deadline) {
            body = rest.getForObject("http://localhost:" + port + "/actuator/prometheus", String.class);
            last = body == null ? null : gaugeValue(body, metric, brokerId);
            if (last != null && last == expected) return body;
            Thread.sleep(100);
        }
        assertThat(last)
                .as("%s{broker_id=\"%s\"} should reach %s within %sms", metric, brokerId, expected, timeoutMillis)
                .isEqualTo(expected);
        return body;
    }

    /** Value of {@code metric{... broker_id="id" ...}} in the exposition body, or null when absent. */
    private static Double gaugeValue(String body, String metric, int brokerId) {
        var p = Pattern.compile(
                "^" + Pattern.quote(metric) + "\\{[^}]*broker_id=\"" + brokerId + "\"[^}]*} (\\S+)\\s*$",
                Pattern.MULTILINE);
        var m = p.matcher(body);
        return m.find() ? Double.parseDouble(m.group(1)) : null;
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
