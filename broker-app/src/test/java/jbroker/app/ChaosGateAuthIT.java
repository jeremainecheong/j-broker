package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.raft.core.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Acceptance: a chaos port configured with an auth token refuses
 * anonymous and wrong-token requests with 401 — and the refused pause
 * really did not happen — while the correct bearer token drives the
 * endpoint normally.
 */
class ChaosGateAuthIT {

    private static final String TOKEN = "chaos-it-token";

    @Test
    void chaosEndpointsDemandTheBearerToken(@TempDir Path dataDir) throws Exception {
        try (var cluster = TestBrokerCluster.start(
                1, 3, (i, voters, ports) -> new Broker.Config(new NodeId(1), dataDir, ports[0][0], ports[0][1], voters)
                        .withChaosPort(ports[0][2])
                        .withChaosAuthToken(TOKEN))) {
            int chaosPort = cluster.port(0, 2);
            var http = HttpClient.newHttpClient();
            var base = "http://127.0.0.1:" + chaosPort;

            // Anonymous: rejected, and the broker is NOT paused.
            var anon = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(base + "/debug/chaos/pause"))
                            .POST(HttpRequest.BodyPublishers.noBody())
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(anon.statusCode()).isEqualTo(401);

            // Wrong token: same refusal.
            var wrong = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(base + "/debug/chaos/pause"))
                            .header("Authorization", "Bearer nope")
                            .POST(HttpRequest.BodyPublishers.noBody())
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(wrong.statusCode()).isEqualTo(401);

            var state = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(base + "/debug/chaos/state"))
                            .header("Authorization", "Bearer " + TOKEN)
                            .GET()
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(state.statusCode()).isEqualTo(200);
            assertThat(state.body()).contains("\"paused\":false");

            // Correct token: pause applies and the state reflects it.
            var pause = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(base + "/debug/chaos/pause"))
                            .header("Authorization", "Bearer " + TOKEN)
                            .POST(HttpRequest.BodyPublishers.noBody())
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(pause.statusCode()).isEqualTo(200);

            var pausedState = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(base + "/debug/chaos/state"))
                            .header("Authorization", "Bearer " + TOKEN)
                            .GET()
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(pausedState.body()).contains("\"paused\":true");
        }
    }
}
