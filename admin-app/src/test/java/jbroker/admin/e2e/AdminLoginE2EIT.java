package jbroker.admin.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
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
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.LinkedMultiValueMap;

/**
 * Acceptance, REST-driven end to end: with an operator account
 * configured, anonymous API calls are 401 and anonymous UI hits redirect
 * to {@code /login}; the login form yields a session that reaches the
 * API; a bearer token issued through that session works standalone;
 * revoking the token cuts access immediately; a wrong password stays on
 * the login page.
 */
@SpringBootTest(
        classes = AdminApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "jbroker.redis.url=")
class AdminLoginE2EIT {

    private static final String PASSWORD = "correct horse battery staple";

    private static Broker broker;
    private static Path dataDir;

    @Autowired
    TestRestTemplate rest;

    @DynamicPropertySource
    static void authProperties(DynamicPropertyRegistry registry) {
        var hash = new BCryptPasswordEncoder().encode(PASSWORD);
        registry.add("jbroker.admin.auth.users", () -> "op:" + hash);
        registry.add("jbroker.admin.brokers", () -> "localhost:" + broker.brokerPort());
    }

    @BeforeAll
    static void startBroker() throws Exception {
        dataDir = Files.createTempDirectory("jbroker-admin-login");
        broker = BindRetry.startWithBindRetry(() ->
                Broker.start(new Broker.Config(new NodeId(1), dataDir, BindRetry.freePort(), BindRetry.freePort())));
    }

    @AfterAll
    static void stopBroker() throws IOException {
        if (broker != null) broker.close();
        if (dataDir != null) {
            try (var walk = Files.walk(dataDir)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }

    @Test
    void anonymousApiCallsAre401AndUiRedirectsToLogin() {
        var api = rest.getForEntity("/api/v1/cluster", String.class);
        assertThat(api.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);

        var ui = rest.getForEntity("/", String.class);
        // TestRestTemplate does not follow redirects for 302.
        assertThat(ui.getStatusCode()).isEqualTo(HttpStatus.FOUND);
        assertThat(ui.getHeaders().getLocation()).hasPath("/login");
    }

    @Test
    void staticAssetSuffixesDoNotOpenApiRoutes() {
        // Regression: a suffix allowlist (endsWith ".js"/".css") would let
        // any API route with an attacker-chosen trailing segment bypass
        // auth entirely.
        var byName = rest.getForEntity("/api/v1/topics/orders.js", String.class);
        assertThat(byName.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);

        // Refusal may be 401 (API branch), 302-to-login (UI branch), or
        // 400 (container rejects the raw path) — anything but content.
        var byTraversal = rest.getForEntity("/vendor/../api/v1/cluster", String.class);
        assertThat(byTraversal.getStatusCode().value()).isIn(302, 400, 401);
    }

    @Test
    void wrongPasswordStaysOnTheLoginPage() {
        var resp = postLogin("op", "wrong");
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(resp.getBody()).contains("invalid credentials");
        assertThat(resp.getHeaders().get(HttpHeaders.SET_COOKIE)).isNull();
    }

    @Test
    void loginSessionThenTokenIssueUseAndRevoke() {
        // --- Login: form post sets the session cookie and redirects home.
        var login = postLogin("op", PASSWORD);
        assertThat(login.getStatusCode()).isEqualTo(HttpStatus.FOUND);
        assertThat(login.getHeaders().getLocation()).hasPath("/");
        var cookie = login.getHeaders().getFirst(HttpHeaders.SET_COOKIE);
        assertThat(cookie).isNotNull();

        // --- The session reaches the API.
        var withSession = new HttpHeaders();
        withSession.add(HttpHeaders.COOKIE, cookie);
        var cluster = rest.exchange("/api/v1/cluster", HttpMethod.GET, new HttpEntity<>(withSession), String.class);
        assertThat(cluster.getStatusCode()).isEqualTo(HttpStatus.OK);

        // --- Issue a bearer token through the session.
        var issued =
                rest.exchange("/api/v1/tokens", HttpMethod.POST, new HttpEntity<>(withSession), TokenResponse.class);
        assertThat(issued.getStatusCode()).isEqualTo(HttpStatus.OK);
        String token = issued.getBody().token();
        assertThat(token).isNotBlank();

        // --- The token works without the session.
        var withToken = new HttpHeaders();
        withToken.setBearerAuth(token);
        var viaToken = rest.exchange("/api/v1/cluster", HttpMethod.GET, new HttpEntity<>(withToken), String.class);
        assertThat(viaToken.getStatusCode()).isEqualTo(HttpStatus.OK);

        // --- Revoke: token in the body (never a URL), dead immediately.
        var revokeHeaders = new HttpHeaders();
        revokeHeaders.setBearerAuth(token);
        revokeHeaders.setContentType(MediaType.APPLICATION_JSON);
        var revoke = rest.exchange(
                "/api/v1/tokens/revoke",
                HttpMethod.POST,
                new HttpEntity<>("{\"token\":\"" + token + "\"}", revokeHeaders),
                Void.class);
        assertThat(revoke.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
        var afterRevoke = rest.exchange("/api/v1/cluster", HttpMethod.GET, new HttpEntity<>(withToken), String.class);
        assertThat(afterRevoke.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    /** Deserialization target for the token-issue response. */
    record TokenResponse(String token) {}

    private org.springframework.http.ResponseEntity<String> postLogin(String user, String password) {
        var form = new LinkedMultiValueMap<String, String>();
        form.add("username", user);
        form.add("password", password);
        var headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        return rest.postForEntity("/login", new HttpEntity<>(form, headers), String.class);
    }
}
