package jbroker.admin.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import jbroker.admin.AdminApp;
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
 * End-to-end over the wire: the admin-app scrapes the broker
 * every second (accelerated via the scrape interval property), and
 * {@code /api/v1/metrics/timeseries} surfaces the ring buffer as
 * snake_case JSON.
 */
@SpringBootTest(
        classes = AdminApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"jbroker.redis.url=", "jbroker.metrics.scrape.intervalSeconds=1"})
class TimeseriesEndpointIT {

    private static Broker broker;
    private static Path dataDir;
    private static int brokerPort;

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @BeforeAll
    static void startBroker() throws Exception {
        dataDir = Files.createTempDirectory("p14-3-timeseries");
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
    void timeseriesEndpointAccumulatesSamplesAndReturnsSnakeCase() throws InterruptedException {
        // At 1s scrape interval, 3.5s should net at least 2–3 samples.
        Thread.sleep(3_500);

        String body =
                rest.getForObject("http://localhost:" + port + "/api/v1/metrics/timeseries?window=5m", String.class);
        assertThat(body).isNotNull();
        assertThat(body)
                .as("envelope must be snake_case and include samples + window_seconds")
                .contains("\"window_seconds\":300")
                .contains("\"samples\":")
                .contains("\"produce_bytes_per_sec\"")
                .contains("\"fetch_bytes_per_sec\"")
                .contains("\"produce_p99_nanos\"")
                .contains("\"fetch_p99_nanos\"")
                .contains("\"ts\":")
                .doesNotContain("produceBytesPerSec")
                .doesNotContain("windowSeconds");
    }

    @Test
    void timeseriesEndpointHonorsShortWindowOverride() throws InterruptedException {
        Thread.sleep(2_500);
        String body =
                rest.getForObject("http://localhost:" + port + "/api/v1/metrics/timeseries?window=30s", String.class);
        assertThat(body).contains("\"window_seconds\":30");
    }

    @Test
    void timeseriesEndpointFallsBackToFiveMinutesOnMalformedWindow() {
        String body = rest.getForObject(
                "http://localhost:" + port + "/api/v1/metrics/timeseries?window=notatime", String.class);
        assertThat(body).contains("\"window_seconds\":300");
    }
}
