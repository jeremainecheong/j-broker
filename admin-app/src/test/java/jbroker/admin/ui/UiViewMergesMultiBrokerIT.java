package jbroker.admin.ui;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import jbroker.admin.AdminApp;
import jbroker.app.Broker;
import jbroker.app.testkit.TestBrokerCluster;
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
 * Regression coverage for the "overview view bypasses REST-merge" bug
 * surfaced by the 2026-04-24 admin-UI audit: {@code /} used to render peer
 * roles as {@code UNKNOWN} because it called {@code pool.describeCluster()}
 * directly. Each broker's {@code DescribeCluster} stamps peers as UNKNOWN,
 * so the first-successful-broker snapshot shows 2/3 UNKNOWN in a 3-broker
 * cluster. The REST {@code /api/v1/cluster} path fans out + merges
 * self-reported roles; the view now delegates to that.
 *
 * <p>Single-broker ITs don't catch this because broker 1 correctly reports
 * its own role, so the UNKNOWN sentinel never appears. Hence the 3-broker
 * setup here — every peer concretely stamped UNKNOWN in the un-merged path.
 *
 * <p>Complementary coverage for the topic-detail bug (same root cause, but
 * partition-leader placement is deterministic-on-controller, which breaks
 * end-to-end reproduction in a random-election IT) lives in
 * {@link TopicsViewControllerDelegationTest} — a unit test that mocks the
 * REST controller and proves the view calls its typed merge method.
 */
@SpringBootTest(
        classes = AdminApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "jbroker.redis.url=")
class UiViewMergesMultiBrokerIT {

    private static Broker br1;
    private static Broker br2;
    private static Broker br3;
    private static Path d1, d2, d3;
    private static int b1, b2, b3;

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @BeforeAll
    static void bringUp() throws Exception {
        d1 = Files.createTempDirectory("ui-merge-n1");
        d2 = Files.createTempDirectory("ui-merge-n2");
        d3 = Files.createTempDirectory("ui-merge-n3");
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
        // Match the UI's production wiring: admin-app talks to every broker,
        // in order. With the view-controller bypass bug, firstSuccessful
        // resolves to broker 1 and the peers are stamped UNKNOWN.
        registry.add("jbroker.admin.brokers", () -> "localhost:" + b1 + ",localhost:" + b2 + ",localhost:" + b3);
    }

    @Test
    void overviewPageRendersConcreteRolesForEveryBroker() {
        var resp = rest.getForEntity("http://localhost:" + port + "/", String.class);
        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
        var body = resp.getBody();
        assertThat(body).isNotNull();
        // Before the fix, 2/3 peers came back stamped UNKNOWN. The SVG +
        // nodes-table rows both render data-broker-role=${n.role()}, so
        // asserting no `data-broker-role="UNKNOWN"` anywhere catches it.
        assertThat(body)
                .as("nodes table + topology SVG must show concrete roles for every broker")
                .doesNotContain("data-broker-role=\"UNKNOWN\"");
        // Sanity: we should see both a LEADER and a FOLLOWER so the merge
        // actually had something to overlay.
        assertThat(body).contains("data-broker-role=\"LEADER\"");
        assertThat(body).contains("data-broker-role=\"FOLLOWER\"");
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
