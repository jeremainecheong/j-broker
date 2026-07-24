package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.broker.PartitionState;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Cluster-lifecycle gate: a partition's replicas are reassigned on a live
 * 3-broker cluster — the newcomer catches up through the byte-rate throttle,
 * joins the ISR, and the leaving replica is dropped — all while an idempotent
 * acks=all producer streams records. The reassignment must not lose data.
 *
 * <p>The target keeps the current partition leader (drops the other replica,
 * adds the third broker) so the producer's connection stays valid throughout;
 * the leader-moving path is covered by the planner's unit tests.
 */
class ReassignPartitionThrottledIT {

    private static final int CI_MULT =
            "1".equals(System.getenv("JBROKER_CI")) || "true".equalsIgnoreCase(System.getenv("CI")) ? 4 : 1;

    private static final int RECORD_COUNT = 200;
    private static final int RECORD_SIZE = 1024; // 200 KiB total, enough to exercise the throttle

    @Test
    void reassignsAPartitionUnderLoadWithoutLoss(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        try (var cluster = TestBrokerCluster.start(3, 2, (i, voters, ports) -> new Broker.Config(
                        new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters)
                // Meter reassignment catch-up so it cannot saturate the link.
                .withReassignmentThrottleBytesPerSec(1_000_000))) {
            var brokers = List.of(cluster.broker(0), cluster.broker(1), cluster.broker(2));
            awaitSingleLeader(brokers);
            awaitRegistryConvergence(brokers);

            var controller = leaderOf(brokers);
            try (var admin = new BrokerClient("127.0.0.1", controller.brokerPort())) {
                admin.createTopic("moved", 1, /*rf*/ 2);
            }

            // Resolve the partition's placement, then build a target that keeps
            // its leader, drops the other replica, and adds the third broker.
            var placement = awaitPartitionState(controller);
            int leader = placement.leader();
            var replicas = placement.replicas();
            int third = IntStream.of(1, 2, 3)
                    .filter(b -> !replicas.contains(b))
                    .findFirst()
                    .orElseThrow();
            var target = List.of(leader, third);
            var leaderBroker = brokers.get(leader - 1);

            var acked = new CopyOnWriteArrayList<String>();
            var failure = new AtomicReference<Throwable>();
            var producer = new Thread(
                    () -> streamRecords(leaderBroker.brokerPort(), RECORD_COUNT, acked, failure), "reassign-load");
            producer.start();
            awaitAtLeast(acked, 20, 10_000);

            assertThat(controller.reassignPartition("moved", 0, target))
                    .as("controller accepted the reassignment")
                    .isTrue();

            awaitReassignmentComplete(leaderBroker, target, leader, 30_000L * CI_MULT);

            producer.join(30_000L * CI_MULT);
            assertThat(producer.isAlive()).as("producer finished").isFalse();
            assertThat(failure.get()).as("producer saw no fatal error").isNull();
            assertThat(acked).hasSize(RECORD_COUNT);

            // Zero loss: every acked record readable in order on the leader.
            try (var reader = new BrokerClient("127.0.0.1", leaderBroker.brokerPort())) {
                var read = reader.fetchAll("moved", 0, 4 << 20).stream()
                        .map(b -> new String(b, StandardCharsets.UTF_8))
                        .toList();
                assertThat(read).containsExactlyElementsOf(acked);
            }

            // The newcomer replicated the whole partition; the leaving replica dropped.
            var end = leaderBroker.topics().partitionState("moved", 0).orElseThrow();
            assertThat(end.replicas()).containsExactlyInAnyOrderElementsOf(target);
            assertThat(end.isr()).contains(leader, third);
            assertThat(end.leader()).isEqualTo(leader);
            assertThat(brokers.get(third - 1).logManager().logFor("moved", 0).nextOffset())
                    .isEqualTo(RECORD_COUNT);
        }
    }

    private static void streamRecords(
            int brokerPort, int count, List<String> acked, AtomicReference<Throwable> failure) {
        try (var client = new BrokerClient("127.0.0.1", brokerPort)) {
            long producerId = client.initProducerId();
            var pad = "x".repeat(RECORD_SIZE);
            for (int seq = 0; seq < count; seq++) {
                var value = "rec-" + seq + "-" + pad;
                long deadline = System.currentTimeMillis() + 30_000L * CI_MULT;
                while (true) {
                    try {
                        client.idempotentProduceAcksAll(
                                "moved", 0, value.getBytes(StandardCharsets.UTF_8), producerId, 0, seq);
                        acked.add(value);
                        break;
                    } catch (RuntimeException retriable) {
                        if (System.currentTimeMillis() > deadline) throw retriable;
                        sleepQuietly(20);
                    }
                }
            }
        } catch (Throwable t) {
            failure.set(t);
        }
    }

    private static PartitionState awaitPartitionState(Broker b) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            var s = b.topics().partitionState("moved", 0).orElse(null);
            if (s != null && s.replicas().size() == 2 && s.isr().size() == 2) return s;
            Thread.sleep(50);
        }
        throw new AssertionError("partition state for moved-0 did not settle within 10s");
    }

    private static void awaitReassignmentComplete(Broker leaderBroker, List<Integer> target, int leader, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            var s = leaderBroker.topics().partitionState("moved", 0).orElse(null);
            if (s != null
                    && s.replicas().size() == target.size()
                    && s.replicas().containsAll(target)
                    && s.isr().containsAll(target)
                    && s.leader() == leader) {
                return;
            }
            Thread.sleep(100);
        }
        var s = leaderBroker.topics().partitionState("moved", 0).orElse(null);
        throw new AssertionError("reassignment to " + target + " did not complete; state=" + s);
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
                    .allMatch(b -> b.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3)))) {
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
