package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Follower re-point acceptance gate: when a partition leader dies and a
 * new leader is promoted from the ISR, the surviving follower must stop
 * fetching from the dead broker, start fetching from the new leader,
 * catch up, and stay in (or rejoin) the ISR — all while the dead broker
 * stays down. Only then can acks=all produces with
 * {@code min.insync.replicas=2} succeed again on the two survivors.
 *
 * <p>Guards two regressions: a replica fetcher that kept pumping the old
 * leader's address after a leader change, and the new leader shrinking a
 * healthy survivor out of the ISR before it had any chance to fetch.
 */
class FollowerRejoinAfterLeaderChangeIT {

    // Shared CI runners elect and replicate several times slower than a
    // laptop; scale every wall-clock wait like the sibling lifecycle ITs.
    private static final int CI_MULT =
            "1".equals(System.getenv("JBROKER_CI")) || "true".equalsIgnoreCase(System.getenv("CI")) ? 4 : 1;

    private static final String TOPIC = "orders";
    private static final int PRE_KILL_RECORDS = 10;
    private static final int RELAY_RECORDS = 5;

    @Test
    void survivingFollowerRepointsToNewLeaderAndAcksAllResumes(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        var cluster = TestBrokerCluster.start(3, 2, (i, voters, ports) -> new Broker.Config(
                        new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters)
                .withMinInsyncReplicas(2)
                // Pin the preferred-leader balancer so the only leader
                // change in this test is the fencer's failover promotion.
                .withBalancerTiming(60_000L, 600_000L));
        try {
            awaitSingleLeader(cluster.brokers());
            awaitRegistryConvergence(cluster.brokers());
            createTopicWithRetry(cluster.brokers(), TOPIC, 1, 3);
            int originalLeaderId = awaitAgreedLeaderWithFullIsr(cluster);

            // Data flowing acks=all against the original leader: each ack
            // proves all three replicas hold the record.
            try (var client = new BrokerClient("127.0.0.1", cluster.brokerPort(originalLeaderId - 1))) {
                for (int i = 0; i < PRE_KILL_RECORDS; i++) {
                    client.produceAcksAll(TOPIC, 0, ("pre-kill-" + i).getBytes(StandardCharsets.UTF_8));
                }
            }

            // Kill the partition leader abruptly and never restart it.
            cluster.broker(originalLeaderId - 1).closeAbruptly();
            var survivors = new java.util.ArrayList<Integer>();
            for (int id = 1; id <= 3; id++) if (id != originalLeaderId) survivors.add(id);

            // A new partition leader emerges on a survivor (liveness
            // staleness + possible Raft re-election + fencer tick).
            int newLeaderId = awaitNewPartitionLeader(cluster, survivors, originalLeaderId, 30_000L * CI_MULT);
            int followerId = survivors.get(0) == newLeaderId ? survivors.get(1) : survivors.get(0);
            var newLeader = cluster.broker(newLeaderId - 1);
            var follower = cluster.broker(followerId - 1);

            // Append fresh records on the new leader with acks=1 (the
            // min-ISR floor only gates acks=all). The follower can only
            // obtain these by fetching from the NEW leader, which makes
            // the catch-up wait below a real re-point check rather than a
            // vacuous pass on the pre-kill tail.
            producePlainWithRetry(cluster.brokerPort(newLeaderId - 1), 10_000L * CI_MULT);

            // The surviving follower re-points its fetcher to the new
            // leader and replicates the post-kill records. With the
            // fetcher stuck on the dead broker its log stays at the
            // pre-kill tail forever.
            awaitFollowerLogCatchUp(newLeader, follower, followerId, 30_000L * CI_MULT);

            // The follower is (still or again) in a 2-member ISR on the
            // new leader; the dead broker stays fenced out.
            awaitIsrExactly(newLeader, List.of(newLeaderId, followerId), 30_000L * CI_MULT);

            // acks=all produces succeed again with the killed broker
            // still down: the two-survivor ISR satisfies the floor of 2
            // and the follower's fetches advance the HWM.
            long firstRecoveredOffset =
                    produceAcksAllWithRetry(cluster.brokerPort(newLeaderId - 1), "post-failover-0", 30_000L * CI_MULT);
            assertThat(firstRecoveredOffset).isGreaterThanOrEqualTo(PRE_KILL_RECORDS + RELAY_RECORDS);
            try (var client = new BrokerClient("127.0.0.1", cluster.brokerPort(newLeaderId - 1))) {
                for (int i = 1; i < 5; i++) {
                    client.produceAcksAll(TOPIC, 0, ("post-failover-" + i).getBytes(StandardCharsets.UTF_8));
                }
            }

            // Every acked record is on both survivors, and the ISR held.
            long leaderLeo = newLeader.logManager().logFor(TOPIC, 0).nextOffset();
            assertThat(follower.logManager().logFor(TOPIC, 0).nextOffset())
                    .as("follower %d log tail after acks=all resumes", followerId)
                    .isEqualTo(leaderLeo);
            assertThat(newLeader.topics().partitionState(TOPIC, 0).orElseThrow().isr())
                    .as("ISR stays at the two survivors")
                    .containsExactlyInAnyOrder(newLeaderId, followerId);
        } finally {
            // Tolerates the closeAbruptly()'d broker.
            cluster.close();
        }
    }

    /** All three brokers agree on a live partition leader with a full 3-member ISR. */
    private static int awaitAgreedLeaderWithFullIsr(TestBrokerCluster cluster) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000L * CI_MULT;
        while (System.currentTimeMillis() < deadline) {
            Integer agreed = null;
            boolean ok = true;
            for (var b : cluster.brokers()) {
                var state = b.topics().partitionState(TOPIC, 0).orElse(null);
                if (state == null || state.leader() <= 0 || state.isr().size() != 3) {
                    ok = false;
                    break;
                }
                if (agreed == null) agreed = state.leader();
                else if (agreed != state.leader()) {
                    ok = false;
                    break;
                }
            }
            if (ok && agreed != null) return agreed;
            Thread.sleep(50);
        }
        throw new AssertionError("no agreed partition leader with a full ISR; states="
                + cluster.brokers().stream()
                        .map(b -> String.valueOf(b.topics().partitionState(TOPIC, 0)))
                        .toList());
    }

    private static int awaitNewPartitionLeader(
            TestBrokerCluster cluster, List<Integer> survivorIds, int originalLeaderId, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        int lastObserved = -1;
        while (System.currentTimeMillis() < deadline) {
            // Only the current Raft leader is guaranteed fresh state.
            for (int id : survivorIds) {
                var b = cluster.broker(id - 1);
                if (b.role() != Role.LEADER) continue;
                var state = b.topics().partitionState(TOPIC, 0).orElse(null);
                if (state == null) continue;
                lastObserved = state.leader();
                if (state.leader() > 0 && state.leader() != originalLeaderId) {
                    return state.leader();
                }
            }
            Thread.sleep(100);
        }
        throw new AssertionError("no new partition leader within " + timeoutMs + "ms; lastObserved=" + lastObserved
                + " original=" + originalLeaderId);
    }

    private static void awaitFollowerLogCatchUp(Broker leader, Broker follower, int followerId, long timeoutMs)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long leaderLeo = -1;
        long followerLeo = -1;
        while (System.currentTimeMillis() < deadline) {
            leaderLeo = leader.logManager().logFor(TOPIC, 0).nextOffset();
            followerLeo = follower.logManager().logFor(TOPIC, 0).nextOffset();
            if (leaderLeo >= PRE_KILL_RECORDS + RELAY_RECORDS && followerLeo >= leaderLeo) return;
            Thread.sleep(100);
        }
        throw new AssertionError("follower " + followerId + " never caught up to the new leader within " + timeoutMs
                + "ms: leaderLeo=" + leaderLeo + " followerLeo=" + followerLeo);
    }

    private static void awaitIsrExactly(Broker onBroker, List<Integer> expected, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            var isr =
                    onBroker.topics().partitionState(TOPIC, 0).map(s -> s.isr()).orElse(List.of());
            if (isr.size() == expected.size() && isr.containsAll(expected)) return;
            Thread.sleep(100);
        }
        throw new AssertionError("ISR did not settle on " + expected + " within " + timeoutMs + "ms; state="
                + onBroker.topics().partitionState(TOPIC, 0));
    }

    /**
     * acks=1 relay records against the freshly promoted leader. Its local
     * state-machine apply of the promotion may lag the controller's commit
     * by a beat (transient NOT_LEADER), so retry each record to the
     * deadline.
     */
    private static void producePlainWithRetry(int brokerPort, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        RuntimeException last = null;
        try (var client = new BrokerClient("127.0.0.1", brokerPort)) {
            int sent = 0;
            while (sent < RELAY_RECORDS && System.currentTimeMillis() < deadline) {
                try {
                    client.produce(TOPIC, 0, ("relay-" + sent).getBytes(StandardCharsets.UTF_8));
                    sent++;
                } catch (RuntimeException e) {
                    last = e;
                    Thread.sleep(100);
                }
            }
            if (sent == RELAY_RECORDS) return;
        }
        throw new AssertionError("relay produce did not finish within " + timeoutMs + "ms", last);
    }

    /**
     * The first acks=all produce after failover may race the ISR flip's
     * propagation to the produce path; retry the same payload until the
     * deadline. Idempotency is not needed: an error response means the
     * record was rejected pre-append or its HWM wait timed out on a
     * not-yet-caught-up ISR — this loop asserts one eventually lands.
     */
    private static long produceAcksAllWithRetry(int brokerPort, String payload, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        RuntimeException last = null;
        try (var client = new BrokerClient("127.0.0.1", brokerPort)) {
            while (System.currentTimeMillis() < deadline) {
                try {
                    return client.produceAcksAll(TOPIC, 0, payload.getBytes(StandardCharsets.UTF_8));
                } catch (RuntimeException e) {
                    last = e;
                    Thread.sleep(200);
                }
            }
        }
        throw new AssertionError("acks=all produce did not succeed within " + timeoutMs + "ms after failover", last);
    }

    private static void createTopicWithRetry(List<Broker> brokers, String topic, int partitions, int rf)
            throws Exception {
        long deadline = System.currentTimeMillis() + 10_000L * CI_MULT;
        Exception last = null;
        while (System.currentTimeMillis() < deadline) {
            var raftLeader = brokers.stream()
                    .filter(b -> b.role() == Role.LEADER)
                    .findFirst()
                    .orElse(null);
            if (raftLeader == null) {
                Thread.sleep(100);
                continue;
            }
            try (var client = new BrokerClient("127.0.0.1", raftLeader.brokerPort())) {
                client.createTopic(topic, partitions, rf);
                return;
            } catch (Exception e) {
                last = e;
                Thread.sleep(100);
            }
        }
        throw new AssertionError("createTopic did not succeed", last);
    }

    private static void awaitSingleLeader(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000L * CI_MULT;
        while (System.currentTimeMillis() < deadline) {
            if (brokers.stream().filter(b -> b.role() == Role.LEADER).count() == 1) return;
            Thread.sleep(50);
        }
        throw new AssertionError("no single Raft leader");
    }

    private static void awaitRegistryConvergence(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000L * CI_MULT;
        while (System.currentTimeMillis() < deadline) {
            if (brokers.stream()
                    .allMatch(b -> b.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3)))) return;
            Thread.sleep(50);
        }
        throw new AssertionError("broker registry did not converge");
    }
}
