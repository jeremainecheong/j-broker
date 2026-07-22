package jbroker.broker.controller;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import jbroker.raft.core.Membership;
import jbroker.raft.core.NodeId;
import org.junit.jupiter.api.Test;

class MembershipControllerTest {

    private static final NodeId N1 = new NodeId(1);
    private static final NodeId N2 = new NodeId(2);
    private static final NodeId N3 = new NodeId(3);
    private static final NodeId N4 = new NodeId(4);

    /** In-memory Raft view whose membership takes effect on append, like the real core. */
    private static final class FakeRaft implements MembershipController.RaftMembershipAccess {
        boolean leader = true;
        List<NodeId> voters = new ArrayList<>(List.of(N1, N2, N3));
        List<NodeId> learners = new ArrayList<>();
        long leaderLastIndex = 100;
        long targetMatch = 0;
        boolean addLearnerCommitted = false;
        final List<Membership> proposals = new ArrayList<>();

        @Override
        public boolean isLeader() {
            return leader;
        }

        @Override
        public List<NodeId> voters() {
            return voters;
        }

        @Override
        public List<NodeId> learners() {
            return learners;
        }

        @Override
        public long leaderLastIndex() {
            return leaderLastIndex;
        }

        @Override
        public OptionalLong matchIndex(NodeId peer) {
            return peer.equals(N4) ? OptionalLong.of(targetMatch) : OptionalLong.empty();
        }

        @Override
        public void proposeMembership(Membership m) {
            proposals.add(m);
            // Append-time: learners take effect immediately; a promotion (voter
            // set grows) only "commits" once the prior add-learner has.
            if (m.learners().contains(N4) && !m.voters().contains(N4)) {
                learners = new ArrayList<>(m.learners());
            } else if (m.voters().contains(N4)) {
                if (addLearnerCommitted) {
                    voters = new ArrayList<>(m.voters());
                    learners = new ArrayList<>();
                }
                // else: rejected as in-flight, no effect.
            }
        }
    }

    @Test
    void fullJoinAddsLearnerCatchesUpThenPromotes() throws Exception {
        var raft = new FakeRaft();
        var c = new MembershipController(raft);

        assertThat(c.requestAddBroker(N4)).isTrue();
        assertThat(c.progress().phase()).isEqualTo(MembershipController.Phase.ADDING_LEARNER);

        // Tick 1: propose add-learner (takes effect on append in the fake).
        c.runOnce();
        assertThat(raft.learners).containsExactly(N4);
        // Tick 2: observes the learner active → CATCHING_UP.
        c.runOnce();
        assertThat(c.progress().phase()).isEqualTo(MembershipController.Phase.CATCHING_UP);

        // Still far behind: stays catching up, reports lag.
        raft.targetMatch = 40;
        c.runOnce();
        assertThat(c.progress().phase()).isEqualTo(MembershipController.Phase.CATCHING_UP);
        assertThat(c.progress().lag()).isEqualTo(60);

        // Caught up within slack → PROMOTING.
        raft.targetMatch = 96;
        c.runOnce();
        assertThat(c.progress().phase()).isEqualTo(MembershipController.Phase.PROMOTING);

        // Promotion is rejected until the add-learner has committed.
        c.runOnce();
        assertThat(raft.voters).doesNotContain(N4);
        assertThat(c.progress().phase()).isEqualTo(MembershipController.Phase.PROMOTING);

        // Once committed, the retry proposes the promotion (append-time: the
        // voter set grows on this tick)...
        raft.addLearnerCommitted = true;
        c.runOnce();
        assertThat(raft.voters).containsExactlyInAnyOrder(N1, N2, N3, N4);
        // ...and the next tick observes the new voter set and finishes — the
        // same one-tick-later observation the real async driver gives.
        c.runOnce();
        assertThat(c.progress().phase()).isEqualTo(MembershipController.Phase.DONE);
        assertThat(c.progress().lag()).isZero();
    }

    @Test
    void refusesASecondJoinWhileOneIsInFlight() throws Exception {
        var raft = new FakeRaft();
        var c = new MembershipController(raft);
        assertThat(c.requestAddBroker(N4)).isTrue();
        c.runOnce();

        assertThat(c.requestAddBroker(new NodeId(5))).isFalse();
        assertThat(c.inFlightTarget()).contains(N4);
    }

    @Test
    void refusesToAddAnExistingVoter() {
        var raft = new FakeRaft();
        var c = new MembershipController(raft);
        assertThat(c.requestAddBroker(N2)).isFalse();
    }

    @Test
    void nonLeaderRefusesAndAbandonsInFlightJoin() throws Exception {
        var raft = new FakeRaft();
        var c = new MembershipController(raft);
        assertThat(c.requestAddBroker(N4)).isTrue();
        c.runOnce();

        raft.leader = false;
        assertThat(c.requestAddBroker(new NodeId(5))).isFalse();
        c.runOnce();
        assertThat(c.progress().phase()).isEqualTo(MembershipController.Phase.FAILED);
    }
}
