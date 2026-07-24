package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.broker.client.BrokerClient;
import jbroker.broker.controller.DecommissionController;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Cluster-lifecycle gate: a live broker is decommissioned from a 4-broker
 * cluster while an idempotent acks=all producer streams records. Every
 * replica it hosts drains onto the other brokers (replication factor
 * preserved), it is removed from the Raft voter set, and only then is the
 * process shut down — with zero acked-record loss end to end.
 *
 * <p>The decommissioned broker is chosen to be neither the Raft leader (the
 * controller refuses self-decommission) nor the loaded partition's leader
 * (so the producer's connection stays valid; leader-moving drains are
 * covered by the reassignment gate).
 */
class DecommissionUnderLoadIT {

    private static final int CI_MULT =
            "1".equals(System.getenv("JBROKER_CI")) || "true".equalsIgnoreCase(System.getenv("CI")) ? 4 : 1;

    private static final int RECORD_COUNT = 200;

    @Test
    void drainsRemovesVoterAndLosesNothing(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3, @TempDir Path d4)
            throws Exception {
        var dirs = new Path[] {d1, d2, d3, d4};
        try (var cluster = TestBrokerCluster.start(
                4,
                2,
                (i, voters, ports) ->
                        new Broker.Config(new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters))) {
            var brokers = cluster.brokers();
            awaitSingleLeader(brokers);
            awaitRegistryConvergence(brokers);

            var controller = leaderOf(brokers);
            try (var admin = new BrokerClient("127.0.0.1", controller.brokerPort())) {
                // 4 partitions at rf=2 spread 8 replica slots over 4 brokers,
                // so every broker hosts something to drain.
                admin.createTopic("drain", 4, /*rf*/ 2);
            }
            awaitTopicSettled(controller, 4);

            // Produce to partition 0; decommission a broker that neither
            // leads it nor is the Raft controller.
            int p0Leader =
                    controller.topics().partitionState("drain", 0).orElseThrow().leader();
            int controllerId = brokers.indexOf(controller) + 1;
            int leaving = pickVictim(controller, controllerId, p0Leader);
            var leavingBroker = brokers.get(leaving - 1);
            var p0LeaderBroker = brokers.get(p0Leader - 1);

            var acked = new CopyOnWriteArrayList<String>();
            var failure = new AtomicReference<Throwable>();
            var producer = new Thread(
                    () -> streamRecords(p0LeaderBroker.brokerPort(), RECORD_COUNT, acked, failure),
                    "decommission-load");
            producer.start();
            awaitAtLeast(acked, 20, 10_000);

            assertThat(controller.decommissionBroker(leaving))
                    .as("controller accepted the decommission")
                    .isTrue();

            awaitDecommissionComplete(controller, leaving, 60_000L * CI_MULT);

            // Operator's final step: the drained broker shuts down. The
            // producer keeps streaming through it to prove nothing depends
            // on the departed node.
            leavingBroker.close();

            producer.join(30_000L * CI_MULT);
            assertThat(producer.isAlive()).as("producer finished").isFalse();
            assertThat(failure.get()).as("producer saw no fatal error").isNull();
            assertThat(acked).hasSize(RECORD_COUNT);

            try (var reader = new BrokerClient("127.0.0.1", p0LeaderBroker.brokerPort())) {
                var read = reader.fetchAll("drain", 0, 4 << 20).stream()
                        .map(b -> new String(b, StandardCharsets.UTF_8))
                        .toList();
                assertThat(read).containsExactlyElementsOf(acked);
            }

            // Every partition kept rf=2 with no replica on the leaver, and
            // the voter set no longer contains it.
            for (int p = 0; p < 4; p++) {
                var s = controller.topics().partitionState("drain", p).orElseThrow();
                assertThat(s.replicas()).as("drain-%d replicas", p).hasSize(2).doesNotContain(leaving);
            }
            assertThat(controller.raftVoters()).doesNotContain(new NodeId(leaving));
            assertThat(controller.decommissionProgress().phase()).isEqualTo(DecommissionController.Phase.DONE);
        }
    }

    /** A broker that is neither the Raft controller nor partition 0's leader. */
    private static int pickVictim(Broker controller, int controllerId, int p0Leader) {
        for (int id = 1; id <= 4; id++) {
            if (id == controllerId || id == p0Leader) continue;
            return id;
        }
        throw new AssertionError("no eligible broker to decommission");
    }

    private static void streamRecords(
            int brokerPort, int count, List<String> acked, AtomicReference<Throwable> failure) {
        try (var client = new BrokerClient("127.0.0.1", brokerPort)) {
            long producerId = client.initProducerId();
            for (int seq = 0; seq < count; seq++) {
                var value = "rec-" + seq;
                long deadline = System.currentTimeMillis() + 60_000L * CI_MULT;
                while (true) {
                    try {
                        client.idempotentProduceAcksAll(
                                "drain", 0, value.getBytes(StandardCharsets.UTF_8), producerId, 0, seq);
                        acked.add(value);
                        break;
                    } catch (RuntimeException retriable) {
                        // ISR churn while replicas drain surfaces transient
                        // errors; the same sequence dedupes on retry.
                        if (System.currentTimeMillis() > deadline) throw retriable;
                        sleepQuietly(20);
                    }
                }
            }
        } catch (Throwable t) {
            failure.set(t);
        }
    }

    private static void awaitDecommissionComplete(Broker controller, int leaving, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean done = controller.decommissionProgress().phase() == DecommissionController.Phase.DONE;
            if (done && !controller.raftVoters().contains(new NodeId(leaving))) return;
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
                var s = b.topics().partitionState("drain", p).orElse(null);
                if (s == null || s.isr().size() < 2) {
                    settled = false;
                    break;
                }
            }
            if (settled) return;
            Thread.sleep(50);
        }
        throw new AssertionError("drain topic did not settle within 10s");
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

    private static void awaitRegistryConvergence(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (brokers.stream()
                    .allMatch(b -> b.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3, 4)))) {
                return;
            }
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
