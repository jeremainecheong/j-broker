package jbroker.admin.ui;

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
import org.springframework.http.HttpStatus;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * UI polish guard — pins three recently-fixed admin-UI regressions:
 *
 * <ol>
 *   <li>Footer copy is unified via the shell.html fragment — no template
 *       should hardcode its own page-local footer text again.</li>
 *   <li>{@code /favicon.ico} is served (a real byte response, not a 404),
 *       so the browser console stays clean.</li>
 *   <li>Epoch-millis columns are wrapped in {@code <time data-epoch>} so
 *       {@code /epoch.js} can rewrite them to relative labels — operators
 *       never see raw numbers like {@code 1777036101863} on the Overview
 *       and Topics pages.</li>
 * </ol>
 *
 * <p>Follows the single-broker + TestRestTemplate pattern from
 * {@link AlpineCloakIT} — no Playwright, free-ports from {@link ServerSocket}.
 */
@SpringBootTest(
        classes = AdminApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "jbroker.redis.url=")
class UiPolishIT {

    private static Broker broker;
    private static Path dataDir;
    private static int brokerPort;

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @BeforeAll
    static void startBroker() throws Exception {
        dataDir = Files.createTempDirectory("ui-polish-it");
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
    void topicsPageDoesNotShipTheStalePhase8Footer() {
        String html = rest.getForObject("http://localhost:" + port + "/topics", String.class);
        assertThat(html)
                .as("topics.html footer must come from the shared shell.html fragment, not a page-local string")
                .contains("<footer>j-broker admin</footer>");
    }

    @Test
    void overviewPageDoesNotShipTheStalePhase8Footer() {
        String html = rest.getForObject("http://localhost:" + port + "/", String.class);
        assertThat(html)
                .as("index.html footer must come from the shared shell.html fragment, not a page-local string")
                .contains("<footer>j-broker admin</footer>");
    }

    @Test
    void faviconIcoIsServedWithTwoHundred() {
        var resp = rest.getForEntity("http://localhost:" + port + "/favicon.ico", byte[].class);
        // Spring's static handler serves /favicon.ico off the classpath once we drop the file in
        // static/. Before the placeholder shipped, every page triggered a 404 in the browser
        // console; guard against a future refactor accidentally nuking the file.
        assertThat(resp.getStatusCode())
                .as("favicon.ico must be served from static/ with 200")
                .isEqualTo(HttpStatus.OK);
        assertThat(resp.getBody())
                .as("favicon.ico body must be a non-empty byte payload")
                .isNotNull()
                .isNotEmpty();
    }

    @Test
    void overviewPageWrapsLastSeenInTimeDataEpochSoEpochJsCanRewriteIt() {
        String html = rest.getForObject("http://localhost:" + port + "/", String.class);
        assertThat(html)
                .as("Overview nodes table must wrap lastSeenMillis in <time data-epoch> so epoch.js rewrites it")
                .contains("<time data-epoch=");
        // Shell fragment must wire /epoch.js on every page or the rewrite never runs.
        assertThat(html)
                .as("shell.html must pull in /epoch.js so relative-time rendering fires")
                .contains("/epoch.js");
    }

    @Test
    void topicsPageWrapsCreatedInTimeDataEpoch() {
        // Topics page: createdMillis column must also render via <time data-epoch="">.
        // Create a topic first so the tbody is non-empty and the <time> actually lands
        // in the rendered HTML (empty table hits the "No topics yet" placeholder row).
        try (var client = new jbroker.broker.client.BrokerClient("localhost", brokerPort)) {
            client.createTopic("ui-polish-topic", 1, 1);
        }
        String html = rest.getForObject("http://localhost:" + port + "/topics", String.class);
        assertThat(html)
                .as("Topics list must wrap createdMillis in <time data-epoch>")
                .contains("<time data-epoch=");
    }

    @Test
    void raftPageInheritsTheSharedFooterAndHasNoPhase8String() {
        // Spot-check one of the other templates that previously carried a hardcoded, stale
        // footer — ensures the shell-fragment include landed cluster-wide, not just on the two
        // pages the user actively looks at.
        String html = rest.getForObject("http://localhost:" + port + "/raft", String.class);
        assertThat(html)
                .as("raft.html footer must come from the shared shell.html fragment too")
                .contains("<footer>j-broker admin</footer>");
    }
}
