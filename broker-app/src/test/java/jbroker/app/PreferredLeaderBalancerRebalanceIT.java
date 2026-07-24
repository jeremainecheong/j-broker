package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Prove {@code PreferredLeaderBalancer} actually proposes a
 * leadership move when an imbalance exists. The balancer's decision
 * logic is unit-tested at {@code PreferredLeaderBalancerTest}, but
 * nothing previously drove a live 3-broker cluster through a
 * leader-different-from-replicas[0] state and watched the ticker fire
 * a {@code PartitionChangeRecord}.
 *
 * <p>Strategy: rather than kill+resume broker 1 (which conflates the
 * fencer and the balancer), we stage the imbalance directly via a
 * test-only {@code PartitionChangeRecord} proposal that keeps leader
 * on the Raft-leader broker but swaps replicas[0] to broker 1 — the
 * exact precondition the balancer reacts to. Balancer timings are
 * compressed to 300ms tick / 300ms stability so the IT finishes in
 * seconds rather than the production 15s+30s.
 */
class PreferredLeaderBalancerRebalanceIT {

    private static final long FAST_BALANCER_TICK_MS = 300L;
    private static final long FAST_BALANCER_STABILITY_MS = 300L;

    @Test
    void balancerMovesLeadershipBackToPreferredReplica(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        var cluster = TestBrokerCluster.start(3, 2, (i, voters, ports) -> new Broker.Config(
                        new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters)
                .withBalancerTiming(FAST_BALANCER_TICK_MS, FAST_BALANCER_STABILITY_MS));
        var br1 = cluster.broker(0);
        var br2 = cluster.broker(1);
        var br3 = cluster.broker(2);
        int b1 = cluster.brokerPort(0), b2 = cluster.brokerPort(1), b3 = cluster.brokerPort(2);
        var allBrokers = new java.util.ArrayList<>(List.of(br1, br2, br3));

        try {
            awaitSingleLeader(allBrokers);
            awaitRegistryConvergence(allBrokers);

            createTopicWithRetry(allBrokers, "ramp", 3, 3);
            awaitPartitionLeaderOnAll(allBrokers, "ramp", 0);
            awaitPartitionLeaderOnAll(allBrokers, "ramp", 1);
            awaitPartitionLeaderOnAll(allBrokers, "ramp", 2);

            var raftLeader = leaderOf(allBrokers);

            // Stage the imbalance deterministically: for every partition,
            // force leader = broker 2 with replicas = [1, 2, 3] and
            // ISR = [1, 2, 3]. This gives leader ≠ replicas[0] AND the
            // preferred replica in ISR — the exact precondition the
            // balancer fires on. We go through the Raft leader's propose
            // path via a test-only helper since the production admin RPC
            // doesn't expose explicit replica placement.
            for (int p = 0; p < 3; p++) {
                var state = raftLeader.topics().partitionState("ramp", p).orElseThrow();
                raftLeader.proposePartitionChangeForTest(
                        "ramp",
                        p,
                        /*leader*/ 2,
                        /*isr*/ List.of(1, 2, 3),
                        /*replicas*/ List.of(1, 2, 3),
                        state.leaderEpoch() + 1,
                        state.partitionEpoch() + 1,
                        5_000);
            }

            // Wait long enough for the balancer tick to observe the
            // imbalance, pass the stability window, and propose the
            // rebalance. Budget covers at least 3 ticks + 2 stability
            // windows; still finishes in well under 3 seconds.
            long budgetMs = 10 * (FAST_BALANCER_TICK_MS + FAST_BALANCER_STABILITY_MS);
            long deadline = System.currentTimeMillis() + budgetMs;

            boolean allOnPreferred = false;
            while (System.currentTimeMillis() < deadline) {
                boolean converged = true;
                for (int p = 0; p < 3; p++) {
                    var state = raftLeader.topics().partitionState("ramp", p).orElse(null);
                    if (state == null || state.leader() != 1) {
                        converged = false;
                        break;
                    }
                }
                if (converged) {
                    allOnPreferred = true;
                    break;
                }
                Thread.sleep(50);
            }

            assertThat(allOnPreferred)
                    .as(
                            "balancer should have converged leadership onto preferred replica (broker 1) for all 3 partitions")
                    .isTrue();

            // Sanity: every broker's local TopicManager eventually sees the
            // rebalance (the Raft leader already sees it by definition of
            // waitForPreferredConvergence). Covers the full propagation.
            long propagationDeadline = System.currentTimeMillis() + 3_000;
            boolean replicated = false;
            while (System.currentTimeMillis() < propagationDeadline) {
                boolean all = true;
                for (var broker : allBrokers) {
                    for (int p = 0; p < 3; p++) {
                        var s = broker.topics().partitionState("ramp", p).orElse(null);
                        if (s == null || s.leader() != 1) {
                            all = false;
                            break;
                        }
                    }
                    if (!all) break;
                }
                if (all) {
                    replicated = true;
                    break;
                }
                Thread.sleep(50);
            }
            assertThat(replicated)
                    .as("rebalance should eventually replicate to every broker's metadata view")
                    .isTrue();

            // End-to-end sanity: produce against the new preferred leader
            // (broker 1) and see it succeed.
            try (var client = new BrokerClient("127.0.0.1", br1.brokerPort())) {
                client.produce("ramp", 0, new byte[] {1, 2, 3});
            }
        } finally {
            for (var b : allBrokers) {
                try {
                    b.close();
                } catch (Exception ignored) {
                    // best-effort
                }
            }
        }
    }

    @Test
    void onDemandRebalanceMovesLeadershipWithoutWaitingOutTheWindow(
            @TempDir Path d1, @TempDir Path d2, @TempDir Path d3) throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        // A ten-minute stability window: the periodic balancer cannot fire
        // within this test, so any convergence is the on-demand call's doing.
        var cluster = TestBrokerCluster.start(3, 2, (i, voters, ports) -> new Broker.Config(
                        new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters)
                .withBalancerTiming(FAST_BALANCER_TICK_MS, 600_000L));
        var allBrokers = new java.util.ArrayList<>(cluster.brokers());

        try {
            awaitSingleLeader(allBrokers);
            awaitRegistryConvergence(allBrokers);
            createTopicWithRetry(allBrokers, "ramp", 3, 3);
            for (int p = 0; p < 3; p++) {
                awaitPartitionLeaderOnAll(allBrokers, "ramp", p);
            }

            var raftLeader = leaderOf(allBrokers);
            for (int p = 0; p < 3; p++) {
                var state = raftLeader.topics().partitionState("ramp", p).orElseThrow();
                raftLeader.proposePartitionChangeForTest(
                        "ramp",
                        p,
                        /*leader*/ 2,
                        /*isr*/ List.of(1, 2, 3),
                        /*replicas*/ List.of(1, 2, 3),
                        state.leaderEpoch() + 1,
                        state.partitionEpoch() + 1,
                        5_000);
            }

            int moved = raftLeader.rebalanceLeadership();
            assertThat(moved).as("one move proposed per imbalanced partition").isEqualTo(3);

            long deadline = System.currentTimeMillis() + 10_000;
            boolean converged = false;
            while (System.currentTimeMillis() < deadline) {
                boolean all = true;
                for (int p = 0; p < 3; p++) {
                    var s = raftLeader.topics().partitionState("ramp", p).orElse(null);
                    if (s == null || s.leader() != 1) {
                        all = false;
                        break;
                    }
                }
                if (all) {
                    converged = true;
                    break;
                }
                Thread.sleep(50);
            }
            assertThat(converged)
                    .as("on-demand rebalance should converge leadership onto broker 1 without the window")
                    .isTrue();
        } finally {
            for (var b : allBrokers) {
                try {
                    b.close();
                } catch (Exception ignored) {
                    // best-effort
                }
            }
        }
    }

    private static Broker leaderOf(List<Broker> brokers) {
        return brokers.stream().filter(b -> b.role() == Role.LEADER).findFirst().orElseThrow();
    }

    private static void createTopicWithRetry(List<Broker> brokers, String topic, int partitions, int rf)
            throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
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
        throw new AssertionError("createTopic did not succeed within 10s", last);
    }

    private static void awaitSingleLeader(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            long leaders = brokers.stream().filter(b -> b.role() == Role.LEADER).count();
            if (leaders == 1) return;
            Thread.sleep(50);
        }
        throw new AssertionError("no single leader within 10s");
    }

    private static void awaitRegistryConvergence(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            boolean allKnow = brokers.stream()
                    .allMatch(b -> b.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3)));
            if (allKnow) return;
            Thread.sleep(50);
        }
        throw new AssertionError("broker registry did not converge within 5s");
    }

    private static void awaitPartitionLeaderOnAll(List<Broker> brokers, String topic, int partition)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            boolean all = brokers.stream()
                    .allMatch(b -> b.topics().partitionState(topic, partition).isPresent());
            if (all) return;
            Thread.sleep(50);
        }
        throw new AssertionError("partition state did not converge within 5s");
    }
}
