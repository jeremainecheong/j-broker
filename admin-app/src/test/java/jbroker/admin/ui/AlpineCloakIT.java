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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * P14.1 — regression coverage for the Alpine {@code x-cloak} modal flash.
 *
 * <p>Without the CSS rule {@code [x-cloak] { display: none !important; }},
 * every modal that gates visibility on Alpine's {@code x-show} renders
 * visible for ~50–300ms on first paint — or indefinitely if Alpine
 * fails to initialise (slow CDN, CSP block, JS error). The rule is the
 * documented Alpine companion to the attribute; this test pins both
 * sides of the contract:
 *
 * <ol>
 *   <li>{@code app.css} actually ships the {@code [x-cloak]} rule.</li>
 *   <li>{@code topics.html} still carries an {@code x-cloak} attribute
 *       on the modal — so if a future refactor removes it, the rule
 *       becomes dead CSS and we notice.</li>
 * </ol>
 */
@SpringBootTest(
        classes = AdminApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "jbroker.redis.url=")
class AlpineCloakIT {

    private static Broker broker;
    private static Path dataDir;
    private static int brokerPort;

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @BeforeAll
    static void startBroker() throws Exception {
        dataDir = Files.createTempDirectory("p14-1-cloak");
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
    void appCssShipsTheXCloakRule() {
        String css = rest.getForObject("http://localhost:" + port + "/app.css", String.class);
        assertThat(css).isNotNull();
        // The rule must combine the attribute selector with `display: none`
        // somewhere in the same declaration. Matches `[x-cloak] { display:
        // none !important; }` and any equivalent spacing.
        assertThat(css)
                .as("app.css must ship a [x-cloak] rule that hides the element")
                .containsPattern("\\[x-cloak\\]\\s*\\{[^}]*display\\s*:\\s*none");
    }

    @Test
    void topicsPageStillTagsItsCreateModalWithXCloak() {
        String html = rest.getForObject("http://localhost:" + port + "/topics", String.class);
        assertThat(html).isNotNull();
        // Guard against a future refactor silently stripping x-cloak off the
        // modal — if that happens the CSS rule becomes dead weight and we
        // lose the first-paint defence.
        assertThat(html)
                .as("topics.html must still gate its Alpine-driven modal with x-cloak")
                .contains("x-cloak");
    }

    @Test
    void adminAppSelfHostsAlpineAndHtmxInsteadOfCDN() {
        // P14.1 root cause: unpkg.com script loads were failing in Chrome
        // (ORB) which left Alpine + htmx silently dead — modals wouldn't
        // open, health badge stuck on "checking…", sparklines empty. This
        // test pins the self-hosted shape: shell.html links into /vendor/
        // rather than any external origin, and both files 200 from the
        // static classpath.
        String topicsHtml = rest.getForObject("http://localhost:" + port + "/topics", String.class);
        assertThat(topicsHtml)
                .as("shell.html must not load htmx/Alpine from external CDNs")
                .doesNotContain("unpkg.com/htmx")
                .doesNotContain("unpkg.com/alpinejs")
                .doesNotContain("cdn.jsdelivr.net/npm/htmx")
                .doesNotContain("cdn.jsdelivr.net/npm/alpinejs");
        assertThat(topicsHtml)
                .as("shell.html must load self-hosted htmx + Alpine from /vendor/")
                .contains("/vendor/htmx-")
                .contains("/vendor/alpine-");

        String alpine = rest.getForObject("http://localhost:" + port + "/vendor/alpine-3.13.5.min.js", String.class);
        assertThat(alpine).as("self-hosted alpine-3.13.5.min.js must serve").isNotNull();
        assertThat(alpine.length())
                .as("alpine bundle should be ~40KB+ (guard against a truncated stub)")
                .isGreaterThan(10_000);

        String htmxJs = rest.getForObject("http://localhost:" + port + "/vendor/htmx-1.9.12.min.js", String.class);
        assertThat(htmxJs).as("self-hosted htmx-1.9.12.min.js must serve").isNotNull();
        assertThat(htmxJs.length()).as("htmx bundle should be ~40KB+").isGreaterThan(10_000);
    }
}
