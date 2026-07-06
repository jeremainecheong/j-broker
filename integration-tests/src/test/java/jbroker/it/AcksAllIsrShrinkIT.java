package jbroker.it;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import jbroker.app.Broker;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * full round-trip for acks=all behaviour under ISR shrink.
 *
 * <p>Unit tests already cover the happy path + shrinks inside the
 * ProduceHandler (see {@link jbroker.broker.ProduceHandlerAcksAllTest}),
 * but there is no IT that exercises the full cluster flow:
 *
 * <ol>
 *   <li>RF=3 topic, ISR={1,2,3}, acks=all produce succeeds.</li>
 *   <li>One follower is paused (chaos pause — all in/outbound blocked
 *       so its ReplicaFetcher stops and the leader's last-fetch for it
 *       goes stale).</li>
 *   <li>Immediately re-issuing acks=all produce must fail with
 *       NOT_ENOUGH_REPLICAS — the paused broker is still in the ISR
 *       (IsrManager hasn't shrunk yet), so HWM cannot advance past the
 *       produced offset within the 5s server-side budget.</li>
 *   <li>After IsrManager's ~10s lag timeout + 2s tick, ISR shrinks to
 *       {leader, surviving-follower}. acks=all produce must now
 *       succeed because HWM is computed over the shrunken ISR.</li>
 *   <li>Resume the paused broker. IsrManager expands ISR back to
 *       {1,2,3}. acks=all produce still succeeds.</li>
 * </ol>
 *
 * <p>This is the first and only IT that verifies the broker returns
 * NOT_ENOUGH_REPLICAS under a real ISR-stuck condition and recovers
 * cleanly once housekeeping restores the ISR to a healthy shape.
 */
class AcksAllIsrShrinkIT {

    // Derived from broker config: IsrManager.lagTimeoutMs=10s + 2s tick
    // cadence, plus generous slack for CI noise.
    private static final long ISR_SHRINK_TIMEOUT_MS = 20_000L;
    private static final long ISR_EXPAND_TIMEOUT_MS = 15_000L;

    @Test
    void acksAllFailsWhileIsrStuckAndRecoversAfterShrink(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        try (var cluster = TestBrokerCluster.start(3, 3, (i, voters, ports) -> new Broker.Config(
                        new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters)
                .withChaosPort(ports[i][2]))) {
            var br1 = cluster.broker(0);
            var br2 = cluster.broker(1);
            var br3 = cluster.broker(2);
            int b1 = cluster.brokerPort(0), b2 = cluster.brokerPort(1), b3 = cluster.brokerPort(2);
            int c1 = cluster.port(0, 2), c2 = cluster.port(1, 2), c3 = cluster.port(2, 2);
            waitForClusterReady(br1, br2, br3);

            var brokers = List.of(br1, br2, br3);
            int[] brokerPorts = {b1, b2, b3};
            int[] chaosPorts = {c1, c2, c3};

            int raftLeaderPort = pickLeaderPort(brokers, brokerPorts);
            try (var client = new BrokerClient("127.0.0.1", raftLeaderPort)) {
                client.createTopic("isr-roundtrip", 1, 3);
            }
            waitForFullIsr(brokers, "isr-roundtrip", 0);

            int partitionLeaderId = partitionLeaderId(brokers, "isr-roundtrip", 0);
            int partitionLeaderPort = brokerPorts[partitionLeaderId - 1];
            int followerToPause = followerOtherThan(partitionLeaderId);
            int followerChaosPort = chaosPorts[followerToPause - 1];

            // Step 1 — healthy baseline: acks=all must succeed.
            try (var client = new BrokerClient("127.0.0.1", partitionLeaderPort)) {
                client.produceAcksAll("isr-roundtrip", 0, bytes("baseline"));
            }

            // Step 2 — pause a follower. Its ReplicaFetcher stops pulling
            // from the leader; the leader's FollowerStateTracker's
            // last-fetch for this broker immediately stops advancing.
            postChaos(followerChaosPort, "/debug/chaos/pause");

            // Step 3 — acks=all must fail NOT_ENOUGH_REPLICAS inside the
            // ~5s server-side budget because IsrManager hasn't ticked yet;
            // the paused broker is still in the ISR and HWM is stuck.
            try (var client = new BrokerClient("127.0.0.1", partitionLeaderPort)) {
                assertThatThrownBy(() -> client.produceAcksAll("isr-roundtrip", 0, bytes("stuck")))
                        .as("acks=all must time out with NOT_ENOUGH_REPLICAS while paused replica remains in ISR")
                        .isInstanceOf(RuntimeException.class)
                        .hasMessageContaining("ISR did not replicate");
            }

            // Step 4 — wait for IsrManager to shrink ISR, then a fresh
            // acks=all should succeed (HWM = min LEO of the shrunken ISR).
            waitForIsrWithout(brokers, "isr-roundtrip", 0, followerToPause, ISR_SHRINK_TIMEOUT_MS);
            try (var client = new BrokerClient("127.0.0.1", partitionLeaderPort)) {
                client.produceAcksAll("isr-roundtrip", 0, bytes("post-shrink"));
            }

            // Step 5 — resume the follower and verify ISR expansion +
            // post-heal produce. When the fetcher catches up to HWM, the
            // IsrManager adds it back in.
            postChaos(followerChaosPort, "/debug/chaos/resume");
            waitForIsrContaining(brokers, "isr-roundtrip", 0, List.of(1, 2, 3), ISR_EXPAND_TIMEOUT_MS);
            try (var client = new BrokerClient("127.0.0.1", partitionLeaderPort)) {
                client.produceAcksAll("isr-roundtrip", 0, bytes("post-heal"));
            }
        }
    }

    // ---------- helpers ----------

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static void postChaos(int chaosPort, String path) throws Exception {
        var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        HttpResponse<String> resp = http.send(
                HttpRequest.newBuilder()
                        .uri(URI.create("http://127.0.0.1:" + chaosPort + path))
                        .POST(HttpRequest.BodyPublishers.noBody())
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new AssertionError(path + " → HTTP " + resp.statusCode() + ": " + resp.body());
        }
    }

    private static int pickLeaderPort(List<Broker> brokers, int[] ports) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            for (int i = 0; i < brokers.size(); i++) {
                if (brokers.get(i).role() == Role.LEADER) return ports[i];
            }
            Thread.sleep(50);
        }
        throw new AssertionError("no Raft leader within 10s");
    }

    private static int partitionLeaderId(List<Broker> brokers, String topic, int partition)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            for (var b : brokers) {
                var state = b.topics().partitionState(topic, partition).orElse(null);
                if (state != null && state.leader() > 0) return state.leader();
            }
            Thread.sleep(50);
        }
        throw new AssertionError("no partition leader for " + topic + "-" + partition + " within 10s");
    }

    private static int followerOtherThan(int partitionLeaderId) {
        // 3-broker cluster: pick any id other than the partition leader.
        for (int id = 1; id <= 3; id++) {
            if (id != partitionLeaderId) return id;
        }
        throw new AssertionError("no follower available (unreachable in 3-broker cluster)");
    }

    private static void waitForFullIsr(List<Broker> brokers, String topic, int partition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            boolean allSee3 = brokers.stream().allMatch(b -> {
                var state = b.topics().partitionState(topic, partition).orElse(null);
                return state != null && state.isr().size() == 3;
            });
            if (allSee3) return;
            Thread.sleep(100);
        }
        throw new AssertionError("ISR did not reach {1,2,3} within 15s for " + topic + "-" + partition);
    }

    private static void waitForIsrWithout(
            List<Broker> brokers, String topic, int partition, int expelledBrokerId, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            // Only the leader's view is authoritative for ISR changes; the
            // followers catch up via Raft apply. Polling any broker is fine
            // once the change is committed.
            boolean shrunkEverywhere = brokers.stream().allMatch(b -> {
                var state = b.topics().partitionState(topic, partition).orElse(null);
                return state != null && !state.isr().contains(expelledBrokerId);
            });
            if (shrunkEverywhere) return;
            Thread.sleep(100);
        }
        throw new AssertionError(
                "ISR did not shrink to exclude broker " + expelledBrokerId + " within " + timeoutMs + "ms");
    }

    private static void waitForIsrContaining(
            List<Broker> brokers, String topic, int partition, List<Integer> expectedIsr, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean everyoneAgrees = brokers.stream().allMatch(b -> {
                var state = b.topics().partitionState(topic, partition).orElse(null);
                return state != null && state.isr().containsAll(expectedIsr);
            });
            if (everyoneAgrees) return;
            Thread.sleep(100);
        }
        throw new AssertionError("ISR did not re-expand to contain " + expectedIsr + " within " + timeoutMs + "ms");
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
}
