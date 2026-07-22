package jbroker.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import jbroker.raft.core.Membership;
import jbroker.raft.core.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R4.3 mechanism, proven on a real gRPC cluster: a brand-new node joins a
 * running 3-node cluster as a non-voting learner, catches its log up via the
 * leader's replication, and is then promoted into the voter set — all while
 * the existing cluster keeps a stable leader. This is the Raft-level core of
 * {@code add-broker}; the broker-app orchestration and admin surface layer
 * on top of it.
 */
class LearnerJoinIT {

    private static final NodeId N4 = new NodeId(4);

    @Test
    void aNewNodeJoinsAsLearnerCatchesUpAndIsPromoted(@TempDir Path dir) throws Exception {
        try (var cluster = ClusterHarness.start(dir, 3)) {
            var leader = cluster.waitForLeader(1_000);

            // Build a baseline the newcomer must catch up to.
            final int baseline = 20;
            for (int i = 0; i < baseline; i++) {
                leader.driver().propose(new byte[] {(byte) i});
            }
            cluster.waitForAllApplied(baseline, 15_000);

            // A new node joins as a learner and the leader proposes the
            // add-learner membership change.
            var learner = cluster.joinLearner();
            leader.driver().proposeMembership(new Membership(voters(1, 2, 3), List.of(N4)));

            // The learner catches up to the full committed log — proof it is
            // being replicated to despite not being a voter.
            cluster.waitForNodeApplied(N4, baseline, 20_000);

            // It counts for nothing yet: the leader's voter set is unchanged.
            assertThat(leader.driver().activeVoters()).containsExactlyInAnyOrder(N(1), N(2), N(3));
            assertThat(leader.driver().activeLearners()).containsExactly(N4);

            // Promote it. Once the add-learner change has committed, the leader
            // accepts the promotion; retry until it takes (a still-in-flight
            // change is rejected, leaving membership put — safe).
            promoteWhenReady(cluster, leader);
            assertThat(leader.driver().activeVoters()).containsExactlyInAnyOrder(N(1), N(2), N(3), N4);

            // The promoted node now participates: fresh entries still commit
            // cluster-wide, and the newcomer applies them.
            for (int i = 0; i < 5; i++) {
                leader.driver().propose(new byte[] {(byte) (100 + i)});
            }
            cluster.waitForNodeApplied(N4, baseline + 5, 15_000);

            // The original leadership was never disrupted by the join.
            assertThat(leader.driver().role()).isEqualTo(jbroker.raft.core.Role.LEADER);
        }
    }

    private static void promoteWhenReady(ClusterHarness cluster, ClusterHarness.Node leader)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            if (leader.driver().activeVoters().contains(N4)) return;
            leader.driver().proposeMembership(Membership.ofVoters(voters(1, 2, 3, 4)));
            Thread.sleep(200);
        }
        throw new AssertionError(
                "learner was not promoted within 20s; voters=" + leader.driver().activeVoters());
    }

    private static List<NodeId> voters(int... ids) {
        var out = new java.util.ArrayList<NodeId>();
        for (int id : ids) out.add(new NodeId(id));
        return out;
    }

    private static NodeId N(int id) {
        return new NodeId(id);
    }
}
