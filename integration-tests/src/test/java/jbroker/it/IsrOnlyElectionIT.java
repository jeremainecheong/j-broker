package jbroker.it;

import static org.assertj.core.api.Assertions.assertThat;

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
 * Recreates the #115 loss shape and proves the election fix: a leader whose
 * log is ahead of every follower dies; the partition must go <b>offline
 * with its ISR preserved</b> rather than promote a shorter-log follower,
 * and must recover onto the returning ISR member with zero record loss.
 *
 * <p>Divergence setup uses the asymmetric chaos primitive: both followers
 * block <i>outbound</i> traffic to the partition leader, which starves
 * replica fetch + follower→leader heartbeats while the leader's own
 * outbound Raft appends (and their responses) keep flowing — so the ISR
 * shrink to {leader} can commit through a healthy Raft majority. That is
 * exactly how the soak-v4 DNS wedge produced a solo-ISR leader.
 *
 * <ol>
 *   <li>Topic on the controller (RF=3); baseline record replicated.</li>
 *   <li>Followers block outbound→leader; ISR shrinks to {leader}.</li>
 *   <li>A record lands on the leader only (acks=1 — the acks=all floor
 *       correctly refuses solo-ISR writes, tested elsewhere).</li>
 *   <li>Leader is fully isolated (both directions, both peers). The
 *       remaining majority elects a new Raft leader, whose fencer demotes
 *       the dead broker: <b>leader=-1, ISR preserved</b>. The shorter-log
 *       followers must never be promoted.</li>
 *   <li>Heal everything. The returning ISR member is re-elected leader
 *       and both records — including the leader-only one — are
 *       fetchable. Nothing was truncated away.</li>
 * </ol>
 */
class IsrOnlyElectionIT {

    private static final long ISR_SHRINK_TIMEOUT_MS = 25_000L;
    private static final long OFFLINE_TIMEOUT_MS = 15_000L;
    private static final long RECOVERY_TIMEOUT_MS = 20_000L;
    private static final long CATCHUP_TIMEOUT_MS = 20_000L;

    @Test
    void deadSoloIsrLeaderGoesOfflineThenRecoversWithoutLoss(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        try (var cluster = TestBrokerCluster.start(3, 3, (i, voters, ports) -> new Broker.Config(
                        new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters)
                .withChaosPort(ports[i][2]))) {
            var brokers = List.of(cluster.broker(0), cluster.broker(1), cluster.broker(2));
            int[] brokerPorts = {cluster.brokerPort(0), cluster.brokerPort(1), cluster.brokerPort(2)};
            int[] chaosPorts = {cluster.port(0, 2), cluster.port(1, 2), cluster.port(2, 2)};
            waitForClusterReady(brokers);

            // Create on the current Raft leader (it becomes the partition
            // leader). Startup leadership churn can transiently fence and
            // recover the partition — that now self-heals, so the initial
            // stabilization budget is generous. From here on everything
            // keys off the PARTITION leader, whatever Raft does.
            try (var client = new BrokerClient("127.0.0.1", brokerPorts[raftLeaderId(brokers) - 1])) {
                client.createTopic("election", 1, 3);
            }
            waitForIsrSize(brokers, "election", 0, 3, 45_000L);
            int leaderId = partitionLeaderId(brokers, "election", 0);

            int[] followers = followersOf(leaderId);

            // Step 1 — baseline record reaches all three replicas.
            try (var client = new BrokerClient("127.0.0.1", brokerPorts[leaderId - 1])) {
                client.produceAcksAll("election", 0, bytes("baseline"));
            }

            // Step 2 — starve replication without touching Raft: followers
            // block only their OUTBOUND calls to the leader.
            for (int f : followers) {
                postChaos(chaosPorts[f - 1], "/debug/chaos/partition?peer=" + leaderId + "&direction=outbound");
            }
            waitForIsrExactly(brokers, "election", 0, List.of(leaderId), ISR_SHRINK_TIMEOUT_MS);

            // Step 3 — this record exists on the leader ONLY.
            try (var client = new BrokerClient("127.0.0.1", brokerPorts[leaderId - 1])) {
                client.produce("election", 0, bytes("leader-only"));
            }

            // Step 4 — kill the solo-ISR leader (full isolation). The
            // follower majority elects a new controller, whose fencer
            // must take the partition OFFLINE with the ISR preserved —
            // not promote a shorter-log follower.
            for (int f : followers) {
                postChaos(chaosPorts[leaderId - 1], "/debug/chaos/partition?peer=" + f + "&direction=both");
            }
            var liveBrokers = List.of(brokers.get(followers[0] - 1), brokers.get(followers[1] - 1));
            waitForOfflineWithPreservedIsr(liveBrokers, "election", 0, leaderId, OFFLINE_TIMEOUT_MS);

            // Hold the assertion for a few fencer ticks: the old behavior
            // promoted a follower here almost immediately.
            long holdUntil = System.currentTimeMillis() + 3_000;
            while (System.currentTimeMillis() < holdUntil) {
                for (var b : liveBrokers) {
                    var st = b.topics().partitionState("election", 0).orElseThrow();
                    assertThat(st.leader())
                            .as("shorter-log follower must never be promoted while the ISR member is down")
                            .isEqualTo(-1);
                    assertThat(st.isr()).containsExactly(leaderId);
                }
                Thread.sleep(200);
            }

            // Step 5 — heal everything; the returning ISR member must be
            // re-elected and no record may be lost.
            postChaos(chaosPorts[leaderId - 1], "/debug/chaos/heal-partition");
            for (int f : followers) {
                postChaos(chaosPorts[f - 1], "/debug/chaos/heal-partition");
            }
            waitForLeader(brokers, "election", 0, leaderId, RECOVERY_TIMEOUT_MS);

            try (var client = new BrokerClient("127.0.0.1", brokerPorts[leaderId - 1])) {
                var records = client.fetchAll("election", 0, 1 << 20);
                assertThat(records.stream().map(r -> new String(r, StandardCharsets.UTF_8)))
                        .as("both records survive — including the one only the dead leader held")
                        .containsExactly("baseline", "leader-only");
            }
            // Followers reconcile their LOGS onto the recovered leader
            // (replication is Raft-independent). The ISR-metadata flip back
            // to size 3 is NOT asserted: Raft leadership moved to a
            // follower during the isolation, and a partition leader that
            // is a Raft follower cannot commit ISR flips — its proposals
            // are rejected as non-leader client proposes. Pre-existing
            // gap, tracked separately; the durability claim (all bytes on
            // all replicas) is the stronger assertion anyway.
            for (int f : followers) {
                try (var client = new BrokerClient("127.0.0.1", brokerPorts[f - 1])) {
                    waitForRecordCount(client, "election", 0, 2, CATCHUP_TIMEOUT_MS);
                }
            }
        }
    }

    // ---------- helpers ----------

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static void postChaos(int chaosPort, String pathAndQuery) throws Exception {
        var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        HttpResponse<String> resp = http.send(
                HttpRequest.newBuilder()
                        .uri(URI.create("http://127.0.0.1:" + chaosPort + pathAndQuery))
                        .POST(HttpRequest.BodyPublishers.noBody())
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new AssertionError(pathAndQuery + " → HTTP " + resp.statusCode() + ": " + resp.body());
        }
    }

    private static int raftLeaderId(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            for (int i = 0; i < brokers.size(); i++) {
                if (brokers.get(i).role() == Role.LEADER) return i + 1;
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

    private static int[] followersOf(int leaderId) {
        int[] out = new int[2];
        int idx = 0;
        for (int id = 1; id <= 3; id++) {
            if (id != leaderId) out[idx++] = id;
        }
        return out;
    }

    private static void waitForIsrSize(List<Broker> brokers, String topic, int partition, int size, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean ok = brokers.stream().allMatch(b -> {
                var state = b.topics().partitionState(topic, partition).orElse(null);
                return state != null && state.isr().size() == size;
            });
            if (ok) return;
            Thread.sleep(100);
        }
        throw new AssertionError(
                "ISR did not reach size " + size + " within " + timeoutMs + "ms for " + topic + "-" + partition);
    }

    private static void waitForIsrExactly(
            List<Broker> brokers, String topic, int partition, List<Integer> expected, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean ok = brokers.stream().allMatch(b -> {
                var state = b.topics().partitionState(topic, partition).orElse(null);
                return state != null && state.isr().equals(expected);
            });
            if (ok) return;
            Thread.sleep(100);
        }
        throw new AssertionError("ISR did not become " + expected + " within " + timeoutMs + "ms");
    }

    private static void waitForOfflineWithPreservedIsr(
            List<Broker> liveBrokers, String topic, int partition, int preservedMember, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean ok = liveBrokers.stream().allMatch(b -> {
                var state = b.topics().partitionState(topic, partition).orElse(null);
                return state != null && state.leader() == -1 && state.isr().equals(List.of(preservedMember));
            });
            if (ok) return;
            Thread.sleep(100);
        }
        throw new AssertionError("partition did not go offline with ISR preserved as [" + preservedMember + "] within "
                + timeoutMs + "ms");
    }

    private static void waitForLeader(List<Broker> brokers, String topic, int partition, int expected, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean ok = brokers.stream().allMatch(b -> {
                var state = b.topics().partitionState(topic, partition).orElse(null);
                return state != null && state.leader() == expected;
            });
            if (ok) return;
            Thread.sleep(100);
        }
        throw new AssertionError("partition leadership did not return to " + expected + " within " + timeoutMs + "ms");
    }

    private static void waitForRecordCount(BrokerClient client, String topic, int partition, int count, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        AssertionError last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (client.fetchAll(topic, partition, 1 << 20).size() == count) return;
                last = new AssertionError("record count not yet " + count);
            } catch (RuntimeException e) {
                last = new AssertionError("fetch failed: " + e.getMessage(), e);
            }
            Thread.sleep(200);
        }
        throw last != null ? last : new AssertionError("timed out");
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
