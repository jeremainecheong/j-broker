package jbroker.broker.controller;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import jbroker.broker.PartitionAssignment;
import jbroker.broker.PartitionState;
import jbroker.raft.core.Membership;
import jbroker.raft.core.NodeId;
import org.junit.jupiter.api.Test;

class DecommissionControllerTest {

    /** In-memory cluster view; reassignments complete only when the test says so. */
    private static final class FakeCluster implements DecommissionController.ClusterAccess {
        boolean leader = true;
        Set<Integer> live = new HashSet<>(Set.of(1, 2, 3, 4));
        final Map<String, PartitionAssignment> assignments = new HashMap<>();
        final Map<String, List<Integer>> pending = new HashMap<>();
        final List<String> started = new ArrayList<>();
        List<NodeId> voters = new ArrayList<>(List.of(new NodeId(1), new NodeId(2), new NodeId(3)));
        final List<Membership> proposals = new ArrayList<>();

        void put(String topic, int partition, int leaderBroker, List<Integer> replicas) {
            assignments.put(
                    topic + "-" + partition,
                    new PartitionAssignment(
                            topic, partition, new PartitionState(leaderBroker, replicas, replicas, 1, 0)));
        }

        /** Simulate the reassignment engine finishing a pending move. */
        void completePending(String topic, int partition) {
            var key = topic + "-" + partition;
            var target = pending.remove(key);
            assignments.put(
                    key,
                    new PartitionAssignment(topic, partition, new PartitionState(target.get(0), target, target, 2, 0)));
        }

        @Override
        public boolean isLeader() {
            return leader;
        }

        @Override
        public List<PartitionAssignment> assignments() {
            return List.copyOf(assignments.values());
        }

        @Override
        public Set<Integer> liveBrokers() {
            return Set.copyOf(live);
        }

        @Override
        public boolean reassignmentPending(String topic, int partition) {
            return pending.containsKey(topic + "-" + partition);
        }

        @Override
        public boolean startReassignment(String topic, int partition, List<Integer> target) {
            started.add(topic + "-" + partition);
            pending.put(topic + "-" + partition, List.copyOf(target));
            return true;
        }

        @Override
        public List<NodeId> voters() {
            return List.copyOf(voters);
        }

        @Override
        public void proposeMembership(Membership membership) {
            proposals.add(membership);
            voters = new ArrayList<>(membership.voters());
        }
    }

    @Test
    void fullDecommissionDrainsThenRemovesTheVoter() throws Exception {
        var cluster = new FakeCluster();
        cluster.put("t", 0, 1, List.of(1, 2));
        cluster.put("t", 1, 2, List.of(2, 3));
        var c = new DecommissionController(cluster);

        assertThat(c.requestDecommission(2)).isTrue();
        assertThat(c.progress().phase()).isEqualTo(DecommissionController.Phase.DRAINING);
        assertThat(c.progress().remaining()).isEqualTo(2);

        // Tick 1: both reassignments start; a second tick starts nothing new.
        c.runOnce();
        assertThat(cluster.started).containsExactlyInAnyOrder("t-0", "t-1");
        c.runOnce();
        assertThat(cluster.started).hasSize(2);

        // The reassignment engine finishes both moves.
        cluster.completePending("t", 0);
        cluster.completePending("t", 1);
        c.runOnce();
        assertThat(c.progress().phase()).isEqualTo(DecommissionController.Phase.REMOVING_VOTER);

        // Voter removal proposes, then the next tick observes it.
        c.runOnce();
        assertThat(cluster.proposals).hasSize(1);
        assertThat(cluster.proposals.get(0).voters()).doesNotContain(new NodeId(2));
        c.runOnce();
        assertThat(c.progress().phase()).isEqualTo(DecommissionController.Phase.DONE);
        assertThat(c.progress().remaining()).isZero();
    }

    @Test
    void refusedWhenDrainCannotPreserveReplicationFactor() {
        var cluster = new FakeCluster();
        cluster.live = new HashSet<>(Set.of(1, 2, 3));
        cluster.put("t", 0, 1, List.of(1, 2, 3));
        var c = new DecommissionController(cluster);

        assertThat(c.requestDecommission(1)).isFalse();
        assertThat(c.progress().phase()).isEqualTo(DecommissionController.Phase.REFUSED);
        assertThat(c.progress().detail()).contains("t-0");
    }

    @Test
    void secondRequestRefusedWhileOneIsInFlight() {
        var cluster = new FakeCluster();
        cluster.put("t", 0, 1, List.of(1, 2));
        var c = new DecommissionController(cluster);

        assertThat(c.requestDecommission(2)).isTrue();
        assertThat(c.requestDecommission(3)).isFalse();
    }

    @Test
    void leadershipLossAbandonsTheDecommission() throws Exception {
        var cluster = new FakeCluster();
        cluster.put("t", 0, 1, List.of(1, 2));
        var c = new DecommissionController(cluster);
        assertThat(c.requestDecommission(2)).isTrue();

        cluster.leader = false;
        c.runOnce();
        assertThat(c.progress().phase()).isEqualTo(DecommissionController.Phase.FAILED);
    }

    @Test
    void brokerHostingNothingGoesStraightToVoterRemoval() throws Exception {
        var cluster = new FakeCluster();
        cluster.put("t", 0, 1, List.of(1, 2));
        cluster.voters = new ArrayList<>(List.of(new NodeId(1), new NodeId(2), new NodeId(3), new NodeId(4)));
        var c = new DecommissionController(cluster);

        assertThat(c.requestDecommission(4)).isTrue();
        c.runOnce();
        assertThat(c.progress().phase()).isEqualTo(DecommissionController.Phase.REMOVING_VOTER);
        c.runOnce();
        c.runOnce();
        assertThat(c.progress().phase()).isEqualTo(DecommissionController.Phase.DONE);
        assertThat(cluster.started).isEmpty();
    }

    @Test
    void nonVoterBrokerCompletesWithoutAMembershipChange() throws Exception {
        var cluster = new FakeCluster();
        cluster.put("t", 0, 1, List.of(1, 2));
        var c = new DecommissionController(cluster);

        assertThat(c.requestDecommission(4)).isTrue();
        c.runOnce();
        c.runOnce();
        assertThat(c.progress().phase()).isEqualTo(DecommissionController.Phase.DONE);
        assertThat(cluster.proposals).isEmpty();
    }

    @Test
    void candidateLossMidDrainFailsWithTheReason() throws Exception {
        var cluster = new FakeCluster();
        cluster.live = new HashSet<>(Set.of(1, 2, 3));
        cluster.put("t", 0, 1, List.of(1, 2));
        var c = new DecommissionController(cluster);
        assertThat(c.requestDecommission(2)).isTrue();
        c.runOnce();
        assertThat(cluster.started).containsExactly("t-0");

        // The only replacement broker dies and the reassignment record is
        // gone before it completed — the re-plan has nowhere to move t-0.
        cluster.live = new HashSet<>(Set.of(1, 2));
        cluster.pending.clear();
        c.runOnce();
        assertThat(c.progress().phase()).isEqualTo(DecommissionController.Phase.FAILED);
        assertThat(c.progress().detail()).contains("t-0");
    }
}
