package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import jbroker.app.testkit.BindRetry;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.broker.client.BrokerClient;
import jbroker.broker.controller.DecommissionController;
import jbroker.broker.controller.MembershipController;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Cluster-lifecycle gate: a broker dies abruptly and is replaced — a fresh
 * broker joins as a learner and is promoted, the corpse is decommissioned
 * (its replicas drain onto the survivors and the newcomer; its Raft vote is
 * removed) — all while an idempotent acks=all producer streams records.
 * Replacing a dead node must not lose an acked record.
 *
 * <p>The victim is chosen to be neither the Raft controller nor the loaded
 * partition's leader, so the producer's connection stays valid; its death
 * still exercises the fencer (it leads other partitions) and the
 * ISR-shrink path (acks=all stalls until the corpse shrinks out of the
 * loaded partition's ISR, then resumes).
 */
class DeadNodeReplaceIT {

    private static final int CI_MULT =
            "1".equals(System.getenv("JBROKER_CI")) || "true".equalsIgnoreCase(System.getenv("CI")) ? 4 : 1;

    private static final int RECORD_COUNT = 200;

    @Test
    void deadBrokerIsReplacedByAFreshOneWithoutLoss(
            @TempDir Path d1, @TempDir Path d2, @TempDir Path d3, @TempDir Path d4, @TempDir Path spareBase)
            throws Exception {
        var dirs = new Path[] {d1, d2, d3, d4};
        try (var cluster = TestBrokerCluster.start(
                4,
                2,
                (i, voters, ports) ->
                        new Broker.Config(new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters))) {
            var brokers = cluster.brokers();
            awaitSingleLeader(brokers);
            awaitRegistryConvergence(brokers, List.of(1, 2, 3, 4));

            var controller = leaderOf(brokers);
            int controllerId = brokers.indexOf(controller) + 1;
            try (var admin = new BrokerClient("127.0.0.1", controller.brokerPort())) {
                admin.createTopic("replace", 4, /*rf*/ 2);
            }
            awaitTopicSettled(controller, 4);

            int p0Leader = controller
                    .topics()
                    .partitionState("replace", 0)
                    .orElseThrow()
                    .leader();
            int victim = pickVictim(controllerId, p0Leader);
            var p0LeaderBroker = brokers.get(p0Leader - 1);

            var acked = new CopyOnWriteArrayList<String>();
            var failure = new AtomicReference<Throwable>();
            var producer = new Thread(
                    () -> streamRecords(p0LeaderBroker.brokerPort(), RECORD_COUNT, acked, failure), "replace-load");
            producer.start();
            awaitAtLeast(acked, 20, 10_000);

            // The victim dies mid-stream — no graceful drain, no goodbye.
            brokers.get(victim - 1).closeAbruptly();

            // A fresh broker joins as a learner: voters are the original
            // incumbents (itself absent), so it never campaigns.
            var incumbentVoters = incumbentVoterAddrs(cluster);
            var sparePorts = new int[2];
            var spare = BindRetry.startWithBindRetry(() -> {
                var p = BindRetry.freePorts(2);
                var dir = Files.createTempDirectory(spareBase, "spare-");
                var b = Broker.start(new Broker.Config(new NodeId(5), dir, p[0], p[1], incumbentVoters));
                sparePorts[0] = p[0];
                sparePorts[1] = p[1];
                return b;
            });
            try {
                assertThat(controller.requestAddBroker(new NodeId(5), "127.0.0.1", sparePorts[0], sparePorts[1]))
                        .as("controller accepted the join")
                        .isTrue();
                awaitJoinComplete(controller, 60_000L * CI_MULT);

                // Decommission the corpse: its replicas drain onto the
                // survivors and the newcomer, then its vote is removed.
                assertThat(controller.decommissionBroker(victim))
                        .as("controller accepted the decommission of the dead broker")
                        .isTrue();
                awaitDecommissionComplete(controller, victim, 120_000L * CI_MULT);

                producer.join(60_000L * CI_MULT);
                assertThat(producer.isAlive()).as("producer finished").isFalse();
                assertThat(failure.get()).as("producer saw no fatal error").isNull();
                assertThat(acked).hasSize(RECORD_COUNT);

                // Zero loss: every acked record readable, in order, no gaps.
                try (var reader = new BrokerClient("127.0.0.1", p0LeaderBroker.brokerPort())) {
                    var read = reader.fetchAll("replace", 0, 4 << 20).stream()
                            .map(b -> new String(b, StandardCharsets.UTF_8))
                            .toList();
                    assertThat(read).containsExactlyElementsOf(acked);
                }

                // The cluster is whole again: four voters (victim out,
                // newcomer in), every partition at rf=2 off the corpse.
                assertThat(controller.raftVoters())
                        .doesNotContain(new NodeId(victim))
                        .contains(new NodeId(5));
                for (int p = 0; p < 4; p++) {
                    var s = controller.topics().partitionState("replace", p).orElseThrow();
                    assertThat(s.replicas())
                            .as("replace-%d replicas", p)
                            .hasSize(2)
                            .doesNotContain(victim);
                }
            } finally {
                spare.close();
            }
        }
    }

    /** A broker that is neither the Raft controller nor partition 0's leader. */
    private static int pickVictim(int controllerId, int p0Leader) {
        for (int id = 1; id <= 4; id++) {
            if (id == controllerId || id == p0Leader) continue;
            return id;
        }
        throw new AssertionError("no eligible victim");
    }

    private static void streamRecords(
            int brokerPort, int count, List<String> acked, AtomicReference<Throwable> failure) {
        try (var client = new BrokerClient("127.0.0.1", brokerPort)) {
            long producerId = client.initProducerId();
            for (int seq = 0; seq < count; seq++) {
                var value = "rec-" + seq;
                long deadline = System.currentTimeMillis() + 120_000L * CI_MULT;
                while (true) {
                    try {
                        client.idempotentProduceAcksAll(
                                "replace", 0, value.getBytes(StandardCharsets.UTF_8), producerId, 0, seq);
                        acked.add(value);
                        break;
                    } catch (RuntimeException retriable) {
                        // acks=all stalls while the corpse is still in the
                        // ISR; the shrink frees it. Same-sequence retries
                        // dedupe at the broker.
                        if (System.currentTimeMillis() > deadline) throw retriable;
                        sleepQuietly(20);
                    }
                }
            }
        } catch (Throwable t) {
            failure.set(t);
        }
    }

    private static List<VoterAddress> incumbentVoterAddrs(TestBrokerCluster cluster) {
        return List.of(
                new VoterAddress(new NodeId(1), "127.0.0.1", cluster.raftPort(0), cluster.brokerPort(0)),
                new VoterAddress(new NodeId(2), "127.0.0.1", cluster.raftPort(1), cluster.brokerPort(1)),
                new VoterAddress(new NodeId(3), "127.0.0.1", cluster.raftPort(2), cluster.brokerPort(2)),
                new VoterAddress(new NodeId(4), "127.0.0.1", cluster.raftPort(3), cluster.brokerPort(3)));
    }

    private static void awaitJoinComplete(Broker controller, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (controller.membershipProgress().phase() == MembershipController.Phase.DONE) return;
            Thread.sleep(200);
        }
        throw new AssertionError("join did not complete: " + controller.membershipProgress());
    }

    private static void awaitDecommissionComplete(Broker controller, int victim, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean done = controller.decommissionProgress().phase() == DecommissionController.Phase.DONE;
            if (done && !controller.raftVoters().contains(new NodeId(victim))) return;
            if (controller.decommissionProgress().phase() == DecommissionController.Phase.FAILED) {
                throw new AssertionError("decommission failed: " + controller.decommissionProgress());
            }
            Thread.sleep(200);
        }
        throw new AssertionError("decommission did not complete within " + timeoutMs + "ms: "
                + controller.decommissionProgress() + "; assignments="
                + controller.topics().allPartitionAssignments());
    }

    private static void awaitTopicSettled(Broker b, int partitions) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            boolean settled = true;
            for (int p = 0; p < partitions; p++) {
                var s = b.topics().partitionState("replace", p).orElse(null);
                if (s == null || s.isr().size() < 2) {
                    settled = false;
                    break;
                }
            }
            if (settled) return;
            Thread.sleep(50);
        }
        throw new AssertionError("replace topic did not settle within 10s");
    }

    private static void awaitAtLeast(List<String> list, int n, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (list.size() >= n) return;
            Thread.sleep(50);
        }
        throw new AssertionError("only " + list.size() + " records produced within " + timeoutMs + "ms");
    }

    private static void awaitSingleLeader(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (brokers.stream().filter(b -> b.role() == Role.LEADER).count() == 1) return;
            Thread.sleep(50);
        }
        throw new AssertionError("no single leader within 10s");
    }

    private static void awaitRegistryConvergence(List<Broker> brokers, List<Integer> expected)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (brokers.stream()
                    .allMatch(b -> b.brokerRegistry().knownBrokerIds().containsAll(expected))) return;
            Thread.sleep(50);
        }
        throw new AssertionError("broker registry did not converge within 5s");
    }

    private static Broker leaderOf(List<Broker> brokers) {
        return brokers.stream().filter(b -> b.role() == Role.LEADER).findFirst().orElseThrow();
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
