package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import jbroker.proto.raft.MetadataRecord;
import jbroker.proto.raft.PartitionChangeRecord;
import org.junit.jupiter.api.Test;

class ReassignmentPlannerTest {

    private static ReassignmentStore.Pending pending(List<Integer> target, List<Integer> original) {
        return new ReassignmentStore.Pending("t", 0, target, original);
    }

    private static PartitionState state(int leader, List<Integer> isr, List<Integer> replicas, int le, int pe) {
        return new PartitionState(leader, isr, replicas, le, pe);
    }

    private static PartitionChangeRecord change(MetadataRecord r) {
        assertThat(r.getKindCase()).isEqualTo(MetadataRecord.KindCase.PARTITION_CHANGE);
        return r.getPartitionChange();
    }

    @Test
    void firstMoveExpandsTheReplicaSetToTheUnion() {
        var s = state(1, List.of(1, 2, 3), List.of(1, 2, 3), 5, 2);
        var out = ReassignmentPlanner.plan(s, pending(List.of(2, 3, 4), List.of(1, 2, 3)));

        var c = change(out.orElseThrow());
        assertThat(c.getReplicasList()).containsExactlyInAnyOrder(1, 2, 3, 4);
        assertThat(c.getLeader()).isEqualTo(1);
        assertThat(c.getIsrList()).containsExactly(1, 2, 3);
        // Replica-set-only change: leader_epoch held, partition_epoch bumped, CAS-guarded.
        assertThat(c.getLeaderEpoch()).isEqualTo(5);
        assertThat(c.getPartitionEpoch()).isEqualTo(3);
        assertThat(c.getPriorLeaderEpoch()).isEqualTo(5);
        assertThat(c.getPriorPartitionEpoch()).isEqualTo(2);
    }

    @Test
    void waitsWhileNewcomersAreNotYetInTheIsr() {
        // Already expanded (replicas = union) but 4 has not caught up.
        var s = state(1, List.of(1, 2, 3), List.of(1, 2, 3, 4), 5, 3);
        assertThat(ReassignmentPlanner.plan(s, pending(List.of(2, 3, 4), List.of(1, 2, 3))))
                .isEmpty();
    }

    @Test
    void contractsAndMovesLeaderOnceEveryTargetReplicaIsInSync() {
        // 4 has caught up; leader 1 is leaving the target {2,3,4}.
        var s = state(1, List.of(1, 2, 3, 4), List.of(1, 2, 3, 4), 5, 4);
        var c = change(ReassignmentPlanner.plan(s, pending(List.of(2, 3, 4), List.of(1, 2, 3)))
                .orElseThrow());

        assertThat(c.getReplicasList()).containsExactly(2, 3, 4);
        assertThat(c.getLeader()).isEqualTo(2);
        assertThat(c.getIsrList()).containsExactly(2, 3, 4); // leader first
        // Leader moved: leader_epoch bumps, partition_epoch resets.
        assertThat(c.getLeaderEpoch()).isEqualTo(6);
        assertThat(c.getPartitionEpoch()).isZero();
        assertThat(c.getPriorLeaderEpoch()).isEqualTo(5);
        assertThat(c.getPriorPartitionEpoch()).isEqualTo(4);
    }

    @Test
    void contractsWithoutMovingLeaderWhenLeaderStays() {
        // Target {1,2,4} keeps leader 1; drops 3, adds 4 (already in sync).
        var s = state(1, List.of(1, 2, 3, 4), List.of(1, 2, 3, 4), 5, 4);
        var c = change(ReassignmentPlanner.plan(s, pending(List.of(1, 2, 4), List.of(1, 2, 3)))
                .orElseThrow());

        assertThat(c.getReplicasList()).containsExactly(1, 2, 4);
        assertThat(c.getLeader()).isEqualTo(1);
        assertThat(c.getIsrList()).containsExactly(1, 2, 4);
        // Leader held: no leader_epoch bump, partition_epoch advances.
        assertThat(c.getLeaderEpoch()).isEqualTo(5);
        assertThat(c.getPartitionEpoch()).isEqualTo(5);
    }

    @Test
    void clearsThePendingEntryOnceTheReplicaSetEqualsTheTarget() {
        var s = state(2, List.of(2, 3, 4), List.of(2, 3, 4), 6, 0);
        var out = ReassignmentPlanner.plan(s, pending(List.of(2, 3, 4), List.of(1, 2, 3)));

        assertThat(out.orElseThrow().getKindCase()).isEqualTo(MetadataRecord.KindCase.PARTITION_REASSIGNMENT);
        assertThat(out.orElseThrow().getPartitionReassignment().getTargetReplicasList())
                .isEmpty();
    }

    @Test
    void cancelRevertsTowardTheOriginalSet() {
        // Cancel points the target back at the original {1,2,3} while expanded.
        var s = state(1, List.of(1, 2, 3, 4), List.of(1, 2, 3, 4), 5, 4);
        var c = change(ReassignmentPlanner.plan(s, pending(List.of(1, 2, 3), List.of(1, 2, 3)))
                .orElseThrow());

        assertThat(c.getReplicasList()).containsExactly(1, 2, 3);
        assertThat(c.getLeader()).isEqualTo(1); // leader stays, still in the reverted set
    }

    @Test
    void aNoOpTargetEqualToCurrentJustClears() {
        var s = state(1, List.of(1, 2, 3), List.of(1, 2, 3), 5, 2);
        var out = ReassignmentPlanner.plan(s, pending(List.of(1, 2, 3), List.of(1, 2, 3)));
        assertThat(out.orElseThrow().getKindCase()).isEqualTo(MetadataRecord.KindCase.PARTITION_REASSIGNMENT);
    }
}
