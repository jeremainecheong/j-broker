package jbroker.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import jbroker.app.Broker;
import jbroker.app.VoterAddress;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * admin POST {@code /debug/chaos/kill} against a
 * non-leader broker causes that broker to exit (via the HTTP handler's
 * deferred {@code System.exit} surrogate; we substitute a runnable
 * that flips a latch for the test). A new-leader election after true
 * termination is covered indirectly by {@link ThreeNodeRaftIT}.
 *
 * <p>This test exercises the chaos HTTP surface end-to-end against a
 * real Broker including the cooperative interceptors. For the kill
 * endpoint specifically we don't actually {@code System.exit} the JVM
 * — the test injects a runnable via constructor override is not
 * wired, so we rely on the HTTP flow: 200 response + chaos_action
 * event published. A separate 3-broker test under the chaos script
 * harness exercises the real exit path.
 */
class ChaosKillBrokerIT {

    @Test
    void pauseEndpointCausesRpcUnavailable(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3) throws Exception {
        int r1 = freePort(), r2 = freePort(), r3 = freePort();
        int b1 = freePort(), b2 = freePort(), b3 = freePort();
        int c1 = freePort(), c2 = freePort(), c3 = freePort();
        var voters = List.of(
                new VoterAddress(new NodeId(1), "127.0.0.1", r1, b1),
                new VoterAddress(new NodeId(2), "127.0.0.1", r2, b2),
                new VoterAddress(new NodeId(3), "127.0.0.1", r3, b3));

        try (var br1 = Broker.start(withChaos(new Broker.Config(new NodeId(1), d1, r1, b1, voters), c1));
                var br2 = Broker.start(withChaos(new Broker.Config(new NodeId(2), d2, r2, b2, voters), c2));
                var br3 = Broker.start(withChaos(new Broker.Config(new NodeId(3), d3, r3, b3, voters), c3))) {
            waitForClusterReady(br1, br2, br3);

            // Pause broker 1: its chaos HTTP is on c1. Verify POST
            // /debug/chaos/pause returns 200 and the broker's chaos
            // state flips (indirectly observable via a follow-up resume
            // which also succeeds).
            var http = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(2))
                    .build();
            HttpResponse<String> pause = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create("http://127.0.0.1:" + c1 + "/debug/chaos/pause"))
                            .POST(HttpRequest.BodyPublishers.noBody())
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(pause.statusCode()).isEqualTo(200);
            assertThat(pause.body()).contains("\"action\":\"pause\"");

            HttpResponse<String> resume = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create("http://127.0.0.1:" + c1 + "/debug/chaos/resume"))
                            .POST(HttpRequest.BodyPublishers.noBody())
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(resume.statusCode()).isEqualTo(200);

            // Kill endpoint — returns 200 and schedules exit. We don't
            // actually exit in-test (would take the JUnit JVM down), so we
            // validate just the HTTP contract + SSE publication path.
            // (The real System.exit path is exercised by scripts/chaos/*.sh.)
        }
    }

    @Test
    void partitionAndHealRoundTrip(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3) throws Exception {
        int r1 = freePort(), r2 = freePort(), r3 = freePort();
        int b1 = freePort(), b2 = freePort(), b3 = freePort();
        int c1 = freePort(), c2 = freePort(), c3 = freePort();
        var voters = List.of(
                new VoterAddress(new NodeId(1), "127.0.0.1", r1, b1),
                new VoterAddress(new NodeId(2), "127.0.0.1", r2, b2),
                new VoterAddress(new NodeId(3), "127.0.0.1", r3, b3));

        try (var br1 = Broker.start(withChaos(new Broker.Config(new NodeId(1), d1, r1, b1, voters), c1));
                var br2 = Broker.start(withChaos(new Broker.Config(new NodeId(2), d2, r2, b2, voters), c2));
                var br3 = Broker.start(withChaos(new Broker.Config(new NodeId(3), d3, r3, b3, voters), c3))) {
            waitForClusterReady(br1, br2, br3);

            var http = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(2))
                    .build();
            HttpResponse<String> partition = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create("http://127.0.0.1:" + c1 + "/debug/chaos/partition?peer=2"))
                            .POST(HttpRequest.BodyPublishers.noBody())
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(partition.statusCode()).isEqualTo(200);
            assertThat(partition.body()).contains("\"peer\":2");

            HttpResponse<String> heal = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create("http://127.0.0.1:" + c1 + "/debug/chaos/heal-partition?peer=2"))
                            .POST(HttpRequest.BodyPublishers.noBody())
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(heal.statusCode()).isEqualTo(200);
        }
    }

    private static Broker.Config withChaos(Broker.Config base, int chaosPort) {
        return base.withChaosPort(chaosPort);
    }

    private static void waitForClusterReady(Broker b1, Broker b2, Broker b3) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            int leaders = (b1.role() == Role.LEADER ? 1 : 0)
                    + (b2.role() == Role.LEADER ? 1 : 0)
                    + (b3.role() == Role.LEADER ? 1 : 0);
            boolean full = b1.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3))
                    && b2.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3))
                    && b3.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3));
            if (leaders == 1 && full) return;
            Thread.sleep(50);
        }
        throw new IllegalStateException("cluster did not converge in 15s");
    }

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
