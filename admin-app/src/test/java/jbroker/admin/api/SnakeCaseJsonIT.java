package jbroker.admin.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import jbroker.admin.AdminApp;
import jbroker.app.Broker;
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
 * load-bearing contract test: every {@code /api/v1/*} JSON payload
 * uses snake_case keys. Guards against a future contributor dropping a
 * {@code @JsonProperty("someCamelCase")} override onto a DTO field.
 *
 * <p>Asserts by substring-inspection of the raw response body rather than
 * deserialising into the typed DTO — the whole point is the on-the-wire
 * shape, not the Java side.
 */
@SpringBootTest(
        classes = AdminApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "jbroker.redis.url=")
class SnakeCaseJsonIT {

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
        dataDir = Files.createTempDirectory("jbroker-admin-snake-case");
        broker = Broker.start(new Broker.Config(new NodeId(1), dataDir, raftPort, brokerPort));
        // Wait for the broker to register itself so DescribeCluster has a node.
        long deadline = System.currentTimeMillis() + 5_000;
        while (broker.brokerRegistry().knownBrokerIds().isEmpty()
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
    }

    @AfterAll
    static void stopBroker() throws Exception {
        if (broker != null) broker.close();
        if (dataDir != null) {
            try (var paths = Files.walk(dataDir)) {
                paths.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }

    @DynamicPropertySource
    static void brokerProps(DynamicPropertyRegistry reg) {
        reg.add("jbroker.admin.brokers", () -> "localhost:" + brokerPort);
    }

    @Test
    void clusterResponseUsesSnakeCaseKeys() {
        String body = rest.getForObject("http://localhost:" + port + "/api/v1/cluster", String.class);
        assertThat(body)
                .contains("\"controller_id\"")
                .contains("\"metadata_offset\"")
                .contains("\"broker_id\"")
                .contains("\"last_seen_millis\"")
                .doesNotContain("\"controllerId\"")
                .doesNotContain("\"metadataOffset\"")
                .doesNotContain("\"brokerId\"")
                .doesNotContain("\"lastSeenMillis\"");
    }

    @Test
    void healthBadgeResponseIsSimpleSnakeCase() {
        String body = rest.getForObject("http://localhost:" + port + "/api/v1/health/badge", String.class);
        // HealthBadge has only two single-word fields; no camelCase risk.
        assertThat(body).contains("\"status\"").contains("\"reason\"");
    }

    @Test
    void topicCreateAcceptsSnakeCaseBody() {
        var createRes = rest.postForEntity(
                "http://localhost:" + port + "/api/v1/topics",
                java.util.Map.of(
                        "name", "snake-case-probe",
                        "partitions", 1,
                        "replication_factor", 1),
                String.class);
        assertThat(createRes.getStatusCode().value()).isEqualTo(201);

        var detail = rest.getForObject(
                "http://localhost:" + port + "/api/v1/topics/snake-case-probe", String.class);
        assertThat(detail)
                .contains("\"replication_factor\"")
                .contains("\"partition_states\"")
                .contains("\"high_watermark\"")
                .contains("\"log_end_offset\"")
                .contains("\"created_millis\"")
                .doesNotContain("\"replicationFactor\"")
                .doesNotContain("\"partitionStates\"")
                .doesNotContain("\"highWatermark\"")
                .doesNotContain("\"logEndOffset\"")
                .doesNotContain("\"createdMillis\"");
    }

    @Test
    void throughputMetricsUseSnakeCaseKeys() {
        String body = rest.getForObject(
                "http://localhost:" + port + "/api/v1/metrics/throughput", String.class);
        assertThat(body)
                .contains("\"produce_bytes_per_sec\"")
                .contains("\"fetch_bytes_per_sec\"")
                .contains("\"window_seconds\"")
                .doesNotContain("\"produceBytesPerSec\"")
                .doesNotContain("\"fetchBytesPerSec\"")
                .doesNotContain("\"windowSeconds\"");
    }

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
