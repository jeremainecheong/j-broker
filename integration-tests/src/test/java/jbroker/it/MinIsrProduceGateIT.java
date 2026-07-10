package jbroker.it;

import static org.assertj.core.api.Assertions.assertThat;
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
 * The min.insync.replicas produce gate, end to end (#115 defect 2).
 *
 * <p>The cluster runs with {@code minInsyncReplicas=3} — deliberately, so
 * pausing a single follower is enough to push the committed ISR below the
 * floor while the Raft quorum (2 of 3) stays alive to commit the shrink.
 * (Pausing two followers would starve Raft itself and the ISR change could
 * never commit; the real-world path to a solo ISR is asymmetric channel
 * failure, which the soak covers.)
 *
 * <ol>
 *   <li>RF=3 topic, ISR={1,2,3}: acks=all produce succeeds (3 ≥ 3).</li>
 *   <li>Pause one follower; wait for IsrManager to shrink the ISR to two
 *       members.</li>
 *   <li>acks=all must now fail <b>fast</b> with the pre-append
 *       NOT_ENOUGH_REPLICAS rejection — no 5s replication timeout, and
 *       nothing appended to the log.</li>
 *   <li>Resume the follower; ISR expands back to three; acks=all
 *       succeeds again.</li>
 * </ol>
 */
class MinIsrProduceGateIT {

    // IsrManager lag timeout (10s) + 2s tick + CI slack.
    private static final long ISR_SHRINK_TIMEOUT_MS = 20_000L;
    private static final long ISR_EXPAND_TIMEOUT_MS = 15_000L;

    @Test
    void acksAllRejectedFastWhileIsrBelowFloorAndRecovers(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        try (var cluster = TestBrokerCluster.start(3, 3, (i, voters, ports) -> new Broker.Config(
                        new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters)
                .withChaosPort(ports[i][2])
                .withMinInsyncReplicas(3))) {
            var brokers = List.of(cluster.broker(0), cluster.broker(1), cluster.broker(2));
            int[] brokerPorts = {cluster.brokerPort(0), cluster.brokerPort(1), cluster.brokerPort(2)};
            int[] chaosPorts = {cluster.port(0, 2), cluster.port(1, 2), cluster.port(2, 2)};
            waitForClusterReady(brokers);

            try (var client = new BrokerClient("127.0.0.1", pickLeaderPort(brokers, brokerPorts))) {
                client.createTopic("gate", 1, 3);
            }
            waitForFullIsr(brokers, "gate", 0);

            int partitionLeaderId = partitionLeaderId(brokers, "gate", 0);
            int partitionLeaderPort = brokerPorts[partitionLeaderId - 1];
            int followerToPause = followerOtherThan(partitionLeaderId);
            int followerChaosPort = chaosPorts[followerToPause - 1];

            // Step 1 — full ISR satisfies the floor of 3.
            try (var client = new BrokerClient("127.0.0.1", partitionLeaderPort)) {
                client.produceAcksAll("gate", 0, bytes("baseline"));
            }

            // Step 2 — pause one follower and wait for the committed ISR
            // to shrink below the floor.
            postChaos(followerChaosPort, "/debug/chaos/pause");
            waitForIsrWithout(brokers, "gate", 0, followerToPause, ISR_SHRINK_TIMEOUT_MS);

            // Step 3 — the gate rejects BEFORE appending and well inside
            // the 5s replication budget.
            try (var client = new BrokerClient("127.0.0.1", partitionLeaderPort)) {
                long start = System.nanoTime();
                assertThatThrownBy(() -> client.produceAcksAll("gate", 0, bytes("must-not-land")))
                        .as("acks=all must be rejected pre-append while |ISR| < min.insync.replicas")
                        .isInstanceOf(RuntimeException.class)
                        .hasMessageContaining("min.insync.replicas");
                long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                assertThat(elapsedMs)
                        .as("pre-append rejection must not burn the 5s replication timeout")
                        .isLessThan(3_000L);

                // Nothing appended: only the baseline record is in the log.
                assertThat(client.fetchAll("gate", 0, 1 << 20))
                        .as("rejected produce must leave no trace in the log")
                        .hasSize(1);
            }

            // Step 4 — resume; ISR expands back to 3; produce works again.
            postChaos(followerChaosPort, "/debug/chaos/resume");
            waitForIsrContaining(brokers, "gate", 0, List.of(1, 2, 3), ISR_EXPAND_TIMEOUT_MS);
            try (var client = new BrokerClient("127.0.0.1", partitionLeaderPort)) {
                client.produceAcksAll("gate", 0, bytes("post-heal"));
                assertThat(client.fetchAll("gate", 0, 1 << 20)).hasSize(2);
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
        for (int id = 1; id <= 3; id++) {
            if (id != partitionLeaderId) return id;
        }
        throw new AssertionError("unreachable in a 3-broker cluster");
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

    private static void waitForClusterReady(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            long leaders = brokers.stream().filter(b -> b.role() == Role.LEADER).count();
            boolean full = brokers.stream()
                    .allMatch(b -> b.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3)));
            if (leaders == 1 && full) return;
            Thread.sleep(50);
        }
        throw new IllegalStateException("cluster did not converge in 15s");
    }
}
