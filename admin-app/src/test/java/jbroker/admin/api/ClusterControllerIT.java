package jbroker.admin.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import jbroker.admin.AdminApp;
import jbroker.admin.dto.ClusterSummary;
import jbroker.admin.dto.NodeInfo;
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
 * {@code GET /api/v1/cluster} + {@code /nodes} against a single in-process
 * broker. Confirms the REST layer round-trips the broker's
 * {@code DescribeCluster} response into {@link ClusterSummary} shape and
 * that {@code /nodes/{id}} honours 404 for unknown broker ids.
 */
@SpringBootTest(
        classes = AdminApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "jbroker.redis.url=")
class ClusterControllerIT {

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
        dataDir = Files.createTempDirectory("jbroker-admin-cluster");
        broker = Broker.start(new Broker.Config(new NodeId(1), dataDir, raftPort, brokerPort));
        // Wait for Raft election so the controller-id in the first
        // describeCluster() response is deterministic.
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
    void clusterSummaryReportsControllerIdAndSelfAlive() {
        var resp = rest.getForEntity("http://localhost:" + port + "/api/v1/cluster", ClusterSummary.class);
        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
        var body = resp.getBody();
        assertThat(body).isNotNull();
        assertThat(body.controllerId()).isEqualTo(1);
        assertThat(body.term()).isGreaterThan(0L);
        assertThat(body.nodes()).hasSize(1);
        NodeInfo self = body.nodes().get(0);
        assertThat(self.brokerId()).isEqualTo(1);
        assertThat(self.role()).isEqualTo("LEADER");
        assertThat(self.alive()).isTrue();
    }

    @Test
    void nodesEndpointReturnsListView() {
        var resp = rest.getForEntity("http://localhost:" + port + "/api/v1/nodes", NodeInfo[].class);
        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(resp.getBody()).hasSize(1);
    }

    @Test
    void nodeByIdReturnsSingleNode() {
        var resp = rest.getForEntity("http://localhost:" + port + "/api/v1/nodes/1", NodeInfo.class);
        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(resp.getBody()).isNotNull();
        assertThat(resp.getBody().brokerId()).isEqualTo(1);
    }

    @Test
    void nodeByIdReturnsNotFoundForUnknownBroker() {
        var resp = rest.getForEntity("http://localhost:" + port + "/api/v1/nodes/99", String.class);
        assertThat(resp.getStatusCode().value()).isEqualTo(404);
        assertThat(resp.getBody()).contains("UNKNOWN_NODE");
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
