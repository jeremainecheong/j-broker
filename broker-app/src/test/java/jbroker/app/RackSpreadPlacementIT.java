package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Rack-aware placement end to end: brokers declare racks through config,
 * peers learn them from heartbeats, and topic creation spreads each
 * partition's replicas across the racks — while a rack-blind cluster
 * places exactly as it always has.
 */
class RackSpreadPlacementIT {

    // CI runners fsync + handshake 2–3× slower; scale waits so the gate
    // validates behavior, not laptop-specific timing.
    private static final int CI_MULT =
            "1".equals(System.getenv("JBROKER_CI")) || "true".equalsIgnoreCase(System.getenv("CI")) ? 4 : 1;

    private static final Map<Integer, String> RACKS = Map.of(1, "zone-a", 2, "zone-b", 3, "zone-b");

    @Test
    void topicReplicasSpanBothRacksOfATwoRackCluster(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        try (var cluster = TestBrokerCluster.start(3, 2, (i, voters, ports) -> new Broker.Config(
                        new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters)
                .withRack(RACKS.get(i + 1)))) {
            var brokers = cluster.brokers();
            awaitSingleLeader(brokers);
            awaitRegistryConvergence(brokers, List.of(1, 2, 3));
            var leader = leaderOf(brokers);
            int leaderId = brokers.indexOf(leader) + 1;
            awaitRackView(leader, RACKS);

            try (var admin = new BrokerClient("127.0.0.1", leader.brokerPort())) {
                admin.createTopic("spread", /*partitions*/ 2, /*rf*/ 2);
            }

            var assignments = leader.topics().allPartitionAssignments().stream()
                    .filter(a -> a.topic().equals("spread"))
                    .toList();
            assertThat(assignments).hasSize(2);
            for (var a : assignments) {
                var replicas = a.state().replicas();
                assertThat(replicas).hasSize(2).startsWith(leaderId);
                assertThat(replicas.stream().map(RACKS::get).collect(Collectors.toSet()))
                        .as("partition %d replicas %s span both racks", a.partition(), replicas)
                        .containsExactlyInAnyOrder("zone-a", "zone-b");
            }
        }
    }

    @Test
    void rackBlindClusterPlacesExactlyAsBefore(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3) throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        try (var cluster = TestBrokerCluster.start(
                3,
                2,
                (i, voters, ports) ->
                        new Broker.Config(new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters))) {
            var brokers = cluster.brokers();
            awaitSingleLeader(brokers);
            awaitRegistryConvergence(brokers, List.of(1, 2, 3));
            var leader = leaderOf(brokers);
            int leaderId = brokers.indexOf(leader) + 1;

            // No broker declared a rack, so no registry ever holds one.
            for (var b : brokers) {
                assertThat(b.brokerRegistry().racks()).isEmpty();
            }

            try (var admin = new BrokerClient("127.0.0.1", leader.brokerPort())) {
                admin.createTopic("plain", /*partitions*/ 1, /*rf*/ 2);
            }

            var assignment = leader.topics().allPartitionAssignments().stream()
                    .filter(a -> a.topic().equals("plain"))
                    .findFirst()
                    .orElseThrow();
            var replicas = assignment.state().replicas();
            // The original policy: the creating controller first, one other
            // registered broker second — nothing rack-shaped about it.
            assertThat(replicas).hasSize(2).startsWith(leaderId).doesNotHaveDuplicates();
            assertThat(replicas).allSatisfy(r -> assertThat(r).isIn(1, 2, 3));
        }
    }

    private static void awaitRackView(Broker broker, Map<Integer, String> expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000L * CI_MULT;
        while (System.currentTimeMillis() < deadline) {
            if (broker.brokerRegistry().racks().equals(expected)) return;
            Thread.sleep(50);
        }
        throw new AssertionError("rack view did not converge within budget: expected " + expected + ", got "
                + broker.brokerRegistry().racks());
    }

    private static void awaitSingleLeader(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000L * CI_MULT;
        while (System.currentTimeMillis() < deadline) {
            if (brokers.stream().filter(b -> b.role() == Role.LEADER).count() == 1) return;
            Thread.sleep(50);
        }
        throw new AssertionError("no single leader within budget");
    }

    private static void awaitRegistryConvergence(List<Broker> brokers, List<Integer> expected)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000L * CI_MULT;
        while (System.currentTimeMillis() < deadline) {
            if (brokers.stream()
                    .allMatch(b -> b.brokerRegistry().knownBrokerIds().containsAll(expected))) return;
            Thread.sleep(50);
        }
        throw new AssertionError("broker registry did not converge within budget");
    }

    private static Broker leaderOf(List<Broker> brokers) {
        return brokers.stream().filter(b -> b.role() == Role.LEADER).findFirst().orElseThrow();
    }
}
