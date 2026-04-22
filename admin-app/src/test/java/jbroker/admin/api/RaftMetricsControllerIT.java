package jbroker.admin.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import jbroker.admin.AdminApp;
import jbroker.admin.dto.RaftNodeState;
import jbroker.app.Broker;
import jbroker.broker.client.BrokerClient;
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
 * covers {@code /api/v1/raft} + {@code /api/v1/metrics/*}.
 */
@SpringBootTest(
        classes = AdminApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "jbroker.redis.url=")
class RaftMetricsControllerIT {

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
        dataDir = Files.createTempDirectory("p8-5-raft-metrics");
        broker = Broker.start(new Broker.Config(new NodeId(1), dataDir, raftPort, brokerPort));
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline && broker.role() != jbroker.raft.core.Role.LEADER) {
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
    void raftEndpointReturnsSelfWithLeaderRoleAndPositiveTerm() {
        var resp = rest.getForEntity("http://localhost:" + port + "/api/v1/raft", RaftNodeState[].class);
        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
        var arr = resp.getBody();
        assertThat(arr).isNotNull().hasSize(1);
        var self = arr[0];
        assertThat(self.nodeId()).isEqualTo(1);
        assertThat(self.role()).isEqualTo("LEADER");
        assertThat(self.currentTerm()).isGreaterThan(0L);
        assertThat(self.status()).isEqualTo("REACHABLE");
    }

    @Test
    void metricsEndpointsReportNonZeroAfterProduceFetch() throws Exception {
        try (var client = new BrokerClient("localhost", brokerPort)) {
            client.createTopic("metrics-smoke", 1, 1);
            for (int i = 0; i < 20; i++) {
                client.produce("metrics-smoke", 0, ("v" + i).getBytes());
            }
            client.fetchAll("metrics-smoke", 0, 64 * 1024);
        }
        var tp = rest.getForEntity(
                "http://localhost:" + port + "/api/v1/metrics/throughput", MetricsController.ThroughputView.class);
        assertThat(tp.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(tp.getBody()).isNotNull();
        assertThat(tp.getBody().produceCount()).isGreaterThanOrEqualTo(20L);
        assertThat(tp.getBody().fetchCount()).isGreaterThanOrEqualTo(1L);

        var lat = rest.getForEntity(
                "http://localhost:" + port + "/api/v1/metrics/latency", MetricsController.LatencyView.class);
        assertThat(lat.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(lat.getBody()).isNotNull();
        assertThat(lat.getBody().produce().p50Nanos()).isGreaterThan(0L);
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
