package jbroker.admin.api;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import jbroker.admin.AdminApp;
import jbroker.app.Broker;
import jbroker.app.testkit.BindRetry;
import jbroker.raft.core.NodeId;
import jbroker.storage.Record;
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
 * P13.1 — Spring-level IT covering the HTTP→gRPC plumbing of
 * {@code POST /api/v1/topics/{name}/partitions/{p}/compact}. The main
 * correctness guarantee (sparse offsets survive compact-then-fetch) is
 * asserted at the gRPC layer by {@code E2E_13_1_ForceCompactIT}; this
 * test only proves the admin-app wires the REST layer to
 * {@code BrokerAdminClient.forceCompactPartition} and serialises the
 * response in snake_case.
 */
@SpringBootTest(
        classes = AdminApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "jbroker.redis.url=")
class ForceCompactEndpointIT {

    private static Broker broker;
    private static Path dataDir;
    private static int brokerPort;

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @BeforeAll
    static void startBroker() throws Exception {
        dataDir = Files.createTempDirectory("jbroker-admin-force-compact");
        broker = BindRetry.startWithBindRetry(() ->
                Broker.start(new Broker.Config(new NodeId(1), dataDir, BindRetry.freePort(), BindRetry.freePort())));
        brokerPort = broker.brokerPort();
        long deadline = System.currentTimeMillis() + 5_000;
        while (broker.brokerRegistry().knownBrokerIds().isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
    }

    @AfterAll
    static void stopBroker() throws Exception {
        if (broker != null) broker.close();
        if (dataDir != null) {
            try (var paths = Files.walk(dataDir)) {
                paths.sorted(java.util.Comparator.reverseOrder())
                        .forEach(p -> p.toFile().delete());
            }
        }
    }

    @DynamicPropertySource
    static void brokerProps(DynamicPropertyRegistry reg) {
        reg.add("jbroker.admin.brokers", () -> "localhost:" + brokerPort);
    }

    @Test
    void postToCompactEndpointReturnsSnakeCaseCountOfSurvivors() throws Exception {
        rest.postForEntity(
                "http://localhost:" + port + "/api/v1/topics",
                Map.of(
                        "name",
                        "prices",
                        "partitions",
                        1,
                        "replication_factor",
                        1,
                        "config",
                        Map.of("cleanup.policy", "compact")),
                String.class);

        // Seed the log with 20 records across 4 keys so the compact RPC has
        // something to compact. Driven through the broker's internal Log
        // handle — the producer gRPC path doesn't carry keys today, and the
        // compaction contract doesn't care how records were appended.
        var log = broker.logManager().logFor("prices", 0);
        for (int i = 0; i < 20; i++) {
            String key = "k" + (i % 4);
            String value = "v" + i;
            log.append(
                    List.of(new Record(0, 0L, key.getBytes(UTF_8), value.getBytes(UTF_8))), System.currentTimeMillis());
        }

        var response = rest.postForEntity(
                "http://localhost:" + port + "/api/v1/topics/prices/partitions/0/compact", null, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody())
                .contains("\"records_kept\":4")
                .contains("\"brokers_compacted\":1")
                .doesNotContain("recordsKept")
                .doesNotContain("brokersCompacted");
    }

    @Test
    void postToCompactEndpointReturnsErrorForUnknownTopic() {
        var response = rest.postForEntity(
                "http://localhost:" + port + "/api/v1/topics/does-not-exist/partitions/0/compact", null, String.class);

        assertThat(response.getStatusCode().is4xxClientError()).isTrue();
        assertThat(response.getBody()).contains("UNKNOWN_TOPIC");
    }
}
