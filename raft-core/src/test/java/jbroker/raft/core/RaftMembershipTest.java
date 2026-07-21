package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Single-server membership changes (Raft §4.2).
 *
 * <p>A {@code CONFIG_CHANGE} log entry carries a serialised voter list. Unlike
 * normal entries, a config change takes effect on <strong>append</strong>
 * (not commit): as soon as the leader appends it (or a follower sees it in an
 * AppendEntries batch), {@code voters()} and therefore {@code quorum()} start
 * using the new set. Only one config change may be in flight at a time. A
 * leader that removes itself remains leader until the entry commits, then
 * steps down.
 */
class RaftMembershipTest {

    private static final NodeId N1 = new NodeId(1);
    private static final NodeId N2 = new NodeId(2);
    private static final NodeId N3 = new NodeId(3);
    private static final NodeId N4 = new NodeId(4);

    private static final RaftConfig LEADER_CONFIG = new RaftConfig(
            N1,
            List.of(N1, N2, N3),
            TimeUnit.MILLISECONDS.toNanos(1000),
            TimeUnit.MILLISECONDS.toNanos(500),
            TimeUnit.MILLISECONDS.toNanos(100),
            100);

    private static final RaftConfig FOLLOWER_CONFIG = new RaftConfig(
            N2,
            List.of(N1, N2, N3),
            TimeUnit.MILLISECONDS.toNanos(1000),
            TimeUnit.MILLISECONDS.toNanos(500),
            TimeUnit.MILLISECONDS.toNanos(100),
            100);

    private DefaultRaftCore becomeLeader(Path dir) throws Exception {
        var log = FileRaftLog.open(dir.resolve("log.bin"));
        var state = FilePersistentState.open(dir.resolve("state.bin"));
        var core = new DefaultRaftCore(LEADER_CONFIG, log, state, 0L);
        core.step(new RaftEvent.Tick(TimeUnit.MILLISECONDS.toNanos(5_000)));
        core.step(new RaftEvent.PreVoteResp(N2, Term.ZERO, true));
        core.step(new RaftEvent.VoteResp(N2, new Term(1), true));
        assertThat(core.role()).isEqualTo(Role.LEADER);
        return core;
    }

    @Test
    void proposeConfigChangeAppendsEntryAndActivatesNewVotersImmediately(@TempDir Path dir) throws Exception {
        var core = becomeLeader(dir);
        assertThat(core.activeVoters()).containsExactlyInAnyOrder(N1, N2, N3);

        var effects = core.step(new RaftEvent.ProposeConfigChange(List.of(N1, N2, N3, N4)));

        // A PersistLog with exactly one CONFIG_CHANGE entry was emitted.
        assertThat(effects)
                .filteredOn(e -> e instanceof RaftEffect.PersistLog)
                .hasSize(1)
                .first()
                .satisfies(e -> {
                    var entries = ((RaftEffect.PersistLog) e).entries();
                    assertThat(entries).hasSize(1);
                    assertThat(entries.get(0).type()).isEqualTo(LogEntry.Type.CONFIG_CHANGE);
                });

        // Voters are active immediately on append.
        assertThat(core.activeVoters()).containsExactlyInAnyOrder(N1, N2, N3, N4);

        // AE was sent to the brand-new peer N4 as well as the incumbent peers.
        assertThat(effects)
                .filteredOn(e -> e instanceof RaftEffect.SendAppendEntries)
                .extracting(e -> ((RaftEffect.SendAppendEntries) e).to())
                .containsExactlyInAnyOrder(N2, N3, N4);
    }

    @Test
    void followerActivatesConfigChangeOnAppend(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            var follower = new DefaultRaftCore(FOLLOWER_CONFIG, log, state, 0L);
            assertThat(follower.activeVoters()).containsExactlyInAnyOrder(N1, N2, N3);

            byte[] payload = MembershipCodec.encode(List.of(N1, N2, N3, N4));
            var entry = new LogEntry(1L, new Term(1), LogEntry.Type.CONFIG_CHANGE, payload);
            follower.step(new RaftEvent.AppendEntriesReq(
                    new Term(1), N1, 0L, Term.ZERO, List.of(entry), 0L, TimeUnit.MILLISECONDS.toNanos(100)));

            assertThat(follower.activeVoters()).containsExactlyInAnyOrder(N1, N2, N3, N4);
        }
    }

    @Test
    void rejectsSecondConfigChangeWhileOneInflight(@TempDir Path dir) throws Exception {
        var core = becomeLeader(dir);
        core.step(new RaftEvent.ProposeConfigChange(List.of(N1, N2, N3, N4)));
        // Second change before first commits — must be rejected.
        var effects = core.step(new RaftEvent.ProposeConfigChange(List.of(N1, N2, N4)));

        assertThat(effects).filteredOn(e -> e instanceof RaftEffect.PersistLog).isEmpty();
        assertThat(effects)
                .filteredOn(e -> e instanceof RaftEffect.RejectConfigChange)
                .hasSize(1);
    }

    @Test
    void activeVotersRollBackWhenAppendEntriesTruncatesAConfigChange(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            var follower = new DefaultRaftCore(FOLLOWER_CONFIG, log, state, 0L);

            // Step 1: an AE from term-2 leader brings CONFIG_CHANGE at idx 1
            // adding N4. The follower activates {N1,N2,N3,N4}.
            byte[] addN4 = MembershipCodec.encode(List.of(N1, N2, N3, N4));
            var addEntry = new LogEntry(1L, new Term(2), LogEntry.Type.CONFIG_CHANGE, addN4);
            follower.step(new RaftEvent.AppendEntriesReq(
                    new Term(2), N1, 0L, Term.ZERO, List.of(addEntry), 0L, TimeUnit.MILLISECONDS.toNanos(100)));
            assertThat(follower.activeVoters()).containsExactlyInAnyOrder(N1, N2, N3, N4);

            // Step 2: a new term-3 leader wins an election where N1's entry
            // never propagated elsewhere, and replicates a different entry at
            // idx 1. AE carries a NORMAL entry at idx 1 in term 3.
            var conflictEntry = new LogEntry(1L, new Term(3), LogEntry.Type.NORMAL, new byte[] {0x42});
            follower.step(new RaftEvent.AppendEntriesReq(
                    new Term(3), N3, 0L, Term.ZERO, List.of(conflictEntry), 0L, TimeUnit.MILLISECONDS.toNanos(200)));

            // Without rollback, activeVoters would still be {N1..N4} — making
            // quorum=3 against a cluster that is back to {N1,N2,N3}. After the
            // fix, it's back to the bootstrap set.
            assertThat(follower.activeVoters()).containsExactlyInAnyOrder(N1, N2, N3);
        }
    }

    @Test
    void learnerReceivesReplicationButDoesNotCountTowardCommit(@TempDir Path dir) throws Exception {
        var core = becomeLeader(dir);

        // Add N4 as a non-voting learner: voters stay {N1,N2,N3}.
        var addLearner = core.step(new RaftEvent.ProposeConfigChange(new Membership(List.of(N1, N2, N3), List.of(N4))));
        assertThat(core.activeVoters()).containsExactlyInAnyOrder(N1, N2, N3);
        // The learner is replicated to — AE goes to N4 alongside the voters.
        assertThat(addLearner)
                .filteredOn(e -> e instanceof RaftEffect.SendAppendEntries)
                .extracting(e -> ((RaftEffect.SendAppendEntries) e).to())
                .contains(N4);

        // A normal client entry lands at index 3 (NO_OP idx1, config idx2).
        core.step(new RaftEvent.ClientPropose(new byte[] {0x7}));

        // The learner acking index 3 does NOT advance commit: it is not a voter.
        core.step(new RaftEvent.AppendEntriesResp(N4, new Term(1), true, 0L, Term.ZERO, 3L));
        assertThat(core.commitIndex()).as("learner ack alone cannot commit").isEqualTo(0L);

        // A voter (N2) acking closes the quorum {N1, N2}.
        core.step(new RaftEvent.AppendEntriesResp(N2, new Term(1), true, 0L, Term.ZERO, 3L));
        assertThat(core.commitIndex()).isEqualTo(3L);
    }

    @Test
    void promotingALearnerToVoterLetsItCompleteQuorum(@TempDir Path dir) throws Exception {
        var core = becomeLeader(dir);
        core.step(new RaftEvent.ProposeConfigChange(new Membership(List.of(N1, N2, N3), List.of(N4))));
        // Once the learner has caught up, promote it: voters become {N1,N2,N3,N4}.
        // (One config change at a time — the add-learner entry must commit first.)
        core.step(new RaftEvent.AppendEntriesResp(N2, new Term(1), true, 0L, Term.ZERO, 2L));
        core.step(new RaftEvent.AppendEntriesResp(N3, new Term(1), true, 0L, Term.ZERO, 2L));

        var promote = core.step(new RaftEvent.ProposeConfigChange(List.of(N1, N2, N3, N4)));
        assertThat(promote)
                .filteredOn(e -> e instanceof RaftEffect.RejectConfigChange)
                .isEmpty();
        assertThat(core.activeVoters()).containsExactlyInAnyOrder(N1, N2, N3, N4);
    }

    @Test
    void aLearnerNodeDoesNotStartAnElection(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            // N4 boots knowing it is a learner: voters {N1,N2,N3}, itself a learner.
            var learnerConfig = new RaftConfig(
                    N4,
                    List.of(N1, N2, N3, N4),
                    TimeUnit.MILLISECONDS.toNanos(1000),
                    TimeUnit.MILLISECONDS.toNanos(500),
                    TimeUnit.MILLISECONDS.toNanos(100),
                    100);
            var learner = new DefaultRaftCore(learnerConfig, log, state, 0L);
            byte[] payload = MembershipCodec.encode(new Membership(List.of(N1, N2, N3), List.of(N4)));
            var entry = new LogEntry(1L, new Term(1), LogEntry.Type.CONFIG_CHANGE, payload);
            learner.step(new RaftEvent.AppendEntriesReq(
                    new Term(1), N1, 0L, Term.ZERO, List.of(entry), 0L, TimeUnit.MILLISECONDS.toNanos(100)));
            assertThat(learner.activeVoters()).containsExactlyInAnyOrder(N1, N2, N3);

            // Election timeout fires: a learner must not campaign — no
            // pre-vote requests, and it stays a follower.
            var effects = learner.step(new RaftEvent.Tick(TimeUnit.MILLISECONDS.toNanos(5_000)));

            assertThat(effects)
                    .filteredOn(e -> e instanceof RaftEffect.SendPreVoteReq)
                    .isEmpty();
            assertThat(learner.role()).isEqualTo(Role.FOLLOWER);
        }
    }

    @Test
    void leaderRemovedFromVotersStepsDownAfterCommit(@TempDir Path dir) throws Exception {
        var core = becomeLeader(dir);
        // Leader (N1) proposes its own removal — new voter set is {N2, N3}.
        core.step(new RaftEvent.ProposeConfigChange(List.of(N2, N3)));
        // Leader is still LEADER with the entry uncommitted.
        assertThat(core.role()).isEqualTo(Role.LEADER);
        assertThat(core.activeVoters()).containsExactlyInAnyOrder(N2, N3);

        // New quorum is 2 (of N2+N3). With the on-election NO_OP at index 1,
        // the CONFIG_CHANGE lands at index 2. Acks at matchIndex=2 commit both.
        core.step(new RaftEvent.AppendEntriesResp(N2, new Term(1), true, 0L, Term.ZERO, 2L));
        var effects = core.step(new RaftEvent.AppendEntriesResp(N3, new Term(1), true, 0L, Term.ZERO, 2L));

        assertThat(core.commitIndex()).isEqualTo(2L);
        assertThat(core.role()).isEqualTo(Role.FOLLOWER);
        // A PersistState effect accompanies the step-down.
        assertThat(effects)
                .filteredOn(e -> e instanceof RaftEffect.PersistState)
                .isNotEmpty();
    }
}
