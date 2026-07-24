package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Lightweight coverage of the admin CLI happy path. Points the CLI
 * at a fake HTTP server over loopback so we don't have to boot Spring;
 * this exercises URL construction + argument parsing + request dispatch.
 */
class AdminCliIT {

    private com.sun.net.httpserver.HttpServer server;
    private final AtomicReference<String> lastMethod = new AtomicReference<>();
    private final AtomicReference<String> lastPath = new AtomicReference<>();
    private final AtomicReference<String> lastBody = new AtomicReference<>();
    private int port;

    @BeforeEach
    void start() throws Exception {
        server = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/", exchange -> {
            lastMethod.set(exchange.getRequestMethod());
            lastPath.set(exchange.getRequestURI().getPath());
            lastBody.set(new String(exchange.getRequestBody().readAllBytes()));
            String response =
                    "{\"status\":\"ok\",\"echo\":\"" + exchange.getRequestURI().getPath() + "\"}";
            byte[] bytes = response.getBytes();
            int status = "POST".equals(lastMethod.get()) ? 201 : 200;
            exchange.sendResponseHeaders(status, bytes.length);
            try (var os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });
        server.start();
        port = server.getAddress().getPort();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    @Test
    void clusterInfoDispatchesGet() {
        captureStdout(() -> AdminCli.run(new String[] {"--admin", "http://localhost:" + port, "cluster-info"}));
        assertThat(lastMethod.get()).isEqualTo("GET");
        assertThat(lastPath.get()).isEqualTo("/api/v1/cluster");
    }

    @Test
    void topicsListDispatchesGet() {
        captureStdout(() -> AdminCli.run(new String[] {"--admin", "http://localhost:" + port, "topics", "list"}));
        assertThat(lastMethod.get()).isEqualTo("GET");
        assertThat(lastPath.get()).isEqualTo("/api/v1/topics");
    }

    @Test
    void topicsCreateDispatchesPostWithJsonBody() {
        captureStdout(() -> AdminCli.run(new String[] {
            "--admin",
            "http://localhost:" + port,
            "topics",
            "create",
            "--topic",
            "orders",
            "--partitions",
            "3",
            "--rf",
            "1"
        }));
        assertThat(lastMethod.get()).isEqualTo("POST");
        assertThat(lastPath.get()).isEqualTo("/api/v1/topics");
        assertThat(lastBody.get()).contains("\"name\":\"orders\"");
        assertThat(lastBody.get()).contains("\"partitions\":3");
    }

    @Test
    void topicsDeleteDispatchesDelete() {
        captureStdout(() -> AdminCli.run(
                new String[] {"--admin", "http://localhost:" + port, "topics", "delete", "--topic", "audit"}));
        assertThat(lastMethod.get()).isEqualTo("DELETE");
        assertThat(lastPath.get()).isEqualTo("/api/v1/topics/audit");
    }

    @Test
    void groupsDescribeDispatchesGetWithGroupId() {
        captureStdout(() -> AdminCli.run(
                new String[] {"--admin", "http://localhost:" + port, "groups", "describe", "--group", "worker-svc"}));
        assertThat(lastMethod.get()).isEqualTo("GET");
        assertThat(lastPath.get()).isEqualTo("/api/v1/consumer-groups/worker-svc");
    }

    @Test
    void raftDispatchesGet() {
        captureStdout(() -> AdminCli.run(new String[] {"--admin", "http://localhost:" + port, "raft"}));
        assertThat(lastMethod.get()).isEqualTo("GET");
        assertThat(lastPath.get()).isEqualTo("/api/v1/raft");
    }

    @Test
    void clusterMembershipDispatchesGet() {
        captureStdout(
                () -> AdminCli.run(new String[] {"--admin", "http://localhost:" + port, "cluster", "membership"}));
        assertThat(lastMethod.get()).isEqualTo("GET");
        assertThat(lastPath.get()).isEqualTo("/api/v1/cluster/membership");
    }

    @Test
    void clusterAddBrokerDispatchesPostWithSnakeCaseBody() {
        captureStdout(() -> AdminCli.run(new String[] {
            "--admin",
            "http://localhost:" + port,
            "cluster",
            "add-broker",
            "--id",
            "4",
            "--host",
            "broker-4",
            "--raft-port",
            "7001",
            "--broker-port",
            "9092"
        }));
        assertThat(lastMethod.get()).isEqualTo("POST");
        assertThat(lastPath.get()).isEqualTo("/api/v1/cluster/add-broker");
        assertThat(lastBody.get()).contains("\"broker_id\":4");
        assertThat(lastBody.get()).contains("\"host\":\"broker-4\"");
        assertThat(lastBody.get()).contains("\"raft_port\":7001");
        assertThat(lastBody.get()).contains("\"broker_port\":9092");
    }

    @Test
    void clusterDecommissionDispatchesPost() {
        captureStdout(() -> AdminCli.run(
                new String[] {"--admin", "http://localhost:" + port, "cluster", "decommission", "--id", "2"}));
        assertThat(lastMethod.get()).isEqualTo("POST");
        assertThat(lastPath.get()).isEqualTo("/api/v1/cluster/decommission/2");
        assertThat(lastBody.get()).isEmpty();
    }

    @Test
    void clusterReassignDispatchesPostWithReplicaArray() {
        captureStdout(() -> AdminCli.run(new String[] {
            "--admin",
            "http://localhost:" + port,
            "cluster",
            "reassign",
            "--topic",
            "orders",
            "--partition",
            "1",
            "--replicas",
            "1, 2,3"
        }));
        assertThat(lastMethod.get()).isEqualTo("POST");
        assertThat(lastPath.get()).isEqualTo("/api/v1/cluster/reassignments");
        assertThat(lastBody.get()).isEqualTo("{\"topic\":\"orders\",\"partition\":1,\"replicas\":[1,2,3]}");
    }

    @Test
    void clusterReassignmentsDispatchesGet() {
        captureStdout(
                () -> AdminCli.run(new String[] {"--admin", "http://localhost:" + port, "cluster", "reassignments"}));
        assertThat(lastMethod.get()).isEqualTo("GET");
        assertThat(lastPath.get()).isEqualTo("/api/v1/cluster/reassignments");
    }

    @Test
    void clusterCancelReassignmentDispatchesDelete() {
        captureStdout(() -> AdminCli.run(new String[] {
            "--admin",
            "http://localhost:" + port,
            "cluster",
            "cancel-reassignment",
            "--topic",
            "orders",
            "--partition",
            "1"
        }));
        assertThat(lastMethod.get()).isEqualTo("DELETE");
        assertThat(lastPath.get()).isEqualTo("/api/v1/cluster/reassignments/orders/1");
    }

    @Test
    void clusterRebalanceLeadersDispatchesPost() {
        captureStdout(() ->
                AdminCli.run(new String[] {"--admin", "http://localhost:" + port, "cluster", "rebalance-leaders"}));
        assertThat(lastMethod.get()).isEqualTo("POST");
        assertThat(lastPath.get()).isEqualTo("/api/v1/cluster/rebalance-leadership");
    }

    private void captureStdout(Runnable r) {
        var original = System.out;
        System.setOut(new PrintStream(new ByteArrayOutputStream()));
        try {
            r.run();
        } finally {
            System.setOut(original);
        }
    }
}
