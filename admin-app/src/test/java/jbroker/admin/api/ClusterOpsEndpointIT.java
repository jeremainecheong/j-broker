package jbroker.admin.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
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
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * {@code /api/v1/cluster/*} against a single in-process broker (which is
 * its own controller). Round-trips the quiet-state reads and the broker's
 * validation refusals; none of the refused mutations here mutate anything,
 * so the membership stays IDLE regardless of test order. The 409 paths
 * need a stubbed in-flight operation and live in
 * {@link ClusterLifecycleControllerTest}.
 */
@SpringBootTest(
        classes = AdminApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "jbroker.redis.url=")
class ClusterOpsEndpointIT {

    private static Broker broker;
    private static Path dataDir;
    private static int brokerPort;

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @BeforeAll
    static void startBroker() throws Exception {
        dataDir = Files.createTempDirectory("jbroker-admin-cluster-ops");
        broker = BindRetry.startWithBindRetry(() ->
                Broker.start(new Broker.Config(new NodeId(1), dataDir, BindRetry.freePort(), BindRetry.freePort())));
        brokerPort = broker.brokerPort();
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

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private org.springframework.http.ResponseEntity<String> postJson(String path, String json) {
        var headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return rest.postForEntity(url(path), new HttpEntity<>(json, headers), String.class);
    }

    @Test
    void membershipReportsSingleVoterAndQuietPhases() {
        var resp = rest.getForEntity(url("/api/v1/cluster/membership"), String.class);
        assertThat(resp.getStatusCode().value()).isEqualTo(200);
        assertThat(resp.getBody()).contains("\"voter_ids\":[1]");
        assertThat(resp.getBody()).contains("\"phase\":\"IDLE\"");
    }

    @Test
    void reassignmentsListIsEmptyOnAQuietCluster() {
        var resp = rest.getForEntity(url("/api/v1/cluster/reassignments"), String.class);
        assertThat(resp.getStatusCode().value()).isEqualTo(200);
        assertThat(resp.getBody()).isEqualTo("[]");
    }

    @Test
    void reassignUnknownTopicIsRefusedWithTheBrokersMessage() {
        var resp = postJson("/api/v1/cluster/reassignments", "{\"topic\":\"nope\",\"partition\":0,\"replicas\":[1]}");
        assertThat(resp.getStatusCode().value()).isEqualTo(400);
        assertThat(resp.getBody()).contains("INVALID_CONFIG");
        assertThat(resp.getBody()).contains("unknown topic");
    }

    @Test
    void selfDecommissionIsRefused() {
        var resp = postJson("/api/v1/cluster/decommission/1", "");
        assertThat(resp.getStatusCode().value()).isEqualTo(400);
        assertThat(resp.getBody()).contains("INVALID_CONFIG");
        assertThat(resp.getBody()).contains("controller");
    }

    @Test
    void cancelWithoutPendingReassignmentIsRefused() {
        var resp = rest.exchange(
                url("/api/v1/cluster/reassignments/nope/0"), HttpMethod.DELETE, HttpEntity.EMPTY, String.class);
        assertThat(resp.getStatusCode().value()).isEqualTo(400);
        assertThat(resp.getBody()).contains("no reassignment pending");
    }

    @Test
    void addBrokerBodyValidationIsBadRequestBeforeReachingTheBroker() {
        var resp = postJson(
                "/api/v1/cluster/add-broker", "{\"broker_id\":0,\"host\":\"\",\"raft_port\":0,\"broker_port\":0}");
        assertThat(resp.getStatusCode().value()).isEqualTo(400);
        assertThat(resp.getBody()).contains("VALIDATION_FAILED");
    }

    @Test
    void rebalanceAnswersAMoveCount() {
        var resp = postJson("/api/v1/cluster/rebalance-leadership", "");
        assertThat(resp.getStatusCode().value()).isEqualTo(200);
        assertThat(resp.getBody()).contains("\"moves_proposed\":0");
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
