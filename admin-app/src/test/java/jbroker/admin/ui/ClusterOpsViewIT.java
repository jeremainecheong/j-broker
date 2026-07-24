package jbroker.admin.ui;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import jbroker.admin.AdminApp;
import jbroker.app.Broker;
import jbroker.app.testkit.BindRetry;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * The overview page's cluster-ops controls, scraped as HTML like
 * {@code UiTopologyPageIT}. Pins three contracts:
 *
 * <ol>
 *   <li>The add-broker modal carries {@code x-cloak} (AlpineCloakIT only
 *       covers topics.html) and the controls render on a quiet cluster —
 *       with no membership chips and no decommission button on the
 *       controller's own row.</li>
 *   <li>The reassignments card renders its empty state.</li>
 *   <li>The rebalance form-post round-trips through the broker and
 *       redirects back to the overview.</li>
 * </ol>
 */
@SpringBootTest(
        classes = AdminApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "jbroker.redis.url=")
class ClusterOpsViewIT {

    private static Broker broker;
    private static Path dataDir;
    private static int brokerPort;

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @BeforeAll
    static void startBroker() throws Exception {
        dataDir = Files.createTempDirectory("ui-cluster-ops");
        broker = BindRetry.startWithBindRetry(() ->
                Broker.start(new Broker.Config(new NodeId(1), dataDir, BindRetry.freePort(), BindRetry.freePort())));
        brokerPort = broker.brokerPort();
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline && broker.role() != Role.LEADER) {
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
    void overviewRendersClusterOpsControls() {
        var resp = rest.getForEntity("http://localhost:" + port + "/", String.class);
        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
        var body = resp.getBody();
        assertThat(body).isNotNull();

        // Add-broker modal: present, cloaked, posting to the UI handler.
        assertThat(body).contains("action=\"/ui/cluster/add-broker\"");
        assertThat(body).contains("x-show=\"showAddBroker\" x-cloak");

        // Rebalance button + reassignments empty state.
        assertThat(body).contains("data-rebalance-leaders");
        assertThat(body).contains("data-testid=\"reassignments\"");
        assertThat(body).contains("No reassignments in flight.");

        // Quiet cluster: no membership chips, and the single broker is the
        // controller so its row must NOT offer decommission.
        assertThat(body).doesNotContain("data-join-phase");
        assertThat(body).doesNotContain("data-decommission-phase");
        assertThat(body).doesNotContain("data-decommission=");
    }

    @Test
    void rebalanceFormPostRoundTripsAndRedirectsHome() {
        var resp = rest.postForEntity(
                "http://localhost:" + port + "/ui/cluster/rebalance-leaders", HttpEntity.EMPTY, String.class);
        assertThat(resp.getStatusCode().is3xxRedirection()).isTrue();
        assertThat(resp.getHeaders().getLocation()).isNotNull();
        assertThat(resp.getHeaders().getLocation().getPath()).isEqualTo("/");
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
