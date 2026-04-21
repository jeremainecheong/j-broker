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
    void leaderRemovedFromVotersStepsDownAfterCommit(@TempDir Path dir) throws Exception {
        var core = becomeLeader(dir);
        // Leader (N1) proposes its own removal — new voter set is {N2, N3}.
        core.step(new RaftEvent.ProposeConfigChange(List.of(N2, N3)));
        // Leader is still LEADER with the entry uncommitted.
        assertThat(core.role()).isEqualTo(Role.LEADER);
        assertThat(core.activeVoters()).containsExactlyInAnyOrder(N2, N3);

        // New quorum is 2 (of N2+N3). Acks from N2 AND N3 commit the entry.
        core.step(new RaftEvent.AppendEntriesResp(N2, new Term(1), true, 0L, Term.ZERO, 1L));
        var effects = core.step(new RaftEvent.AppendEntriesResp(N3, new Term(1), true, 0L, Term.ZERO, 1L));

        assertThat(core.commitIndex()).isEqualTo(1L);
        assertThat(core.role()).isEqualTo(Role.FOLLOWER);
        // A PersistState effect accompanies the step-down.
        assertThat(effects)
                .filteredOn(e -> e instanceof RaftEffect.PersistState)
                .isNotEmpty();
    }
}
