package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Figure-8 optimisation of the AppendEntries backoff: when a follower rejects
 * because of a term mismatch at {@code prevLogIndex}, the response carries the
 * follower's {@code conflictTerm} and the first index of that term in the
 * follower's log. If the leader <em>also</em> has entries at that term, it can
 * jump {@code nextIndex} past its own last entry of that term in one step
 * rather than using the follower's conflict index as a lower bound.
 */
class RaftTermAwareBackoffTest {

    private static final RaftConfig CONFIG = new RaftConfig(
            new NodeId(1),
            List.of(new NodeId(1), new NodeId(2), new NodeId(3)),
            TimeUnit.MILLISECONDS.toNanos(1000),
            TimeUnit.MILLISECONDS.toNanos(500),
            TimeUnit.MILLISECONDS.toNanos(100),
            100);

    @Test
    void leaderWithSameTermJumpsPastItsOwnLastIndexOfThatTerm(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            // Pre-populate leader's log with t1@1, t2@2; make currentTerm=2
            // so the next election puts the leader at term 3.
            log.append(List.of(
                    new LogEntry(1, new Term(1), LogEntry.Type.NORMAL, new byte[] {1}),
                    new LogEntry(2, new Term(2), LogEntry.Type.NORMAL, new byte[] {2})));
            state.update(new Term(2), Optional.empty());

            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            core.step(new RaftEvent.Tick(TimeUnit.MILLISECONDS.toNanos(5_000)));
            core.step(new RaftEvent.VoteResp(new NodeId(2), new Term(3), true));
            assertThat(core.role()).isEqualTo(Role.LEADER);

            // Follower rejects with conflictTerm=t1, conflictIndex=1 (its firstIndexOfTerm).
            // Naive backoff would set nextIndex = conflictIndex = 1 → next AE has
            // prevLogIndex=0. Term-aware backoff jumps past leader's last t1 entry
            // (index 1), so nextIndex = 2 and prevLogIndex = 1 with prevLogTerm = t1.
            var effects =
                    core.step(new RaftEvent.AppendEntriesResp(new NodeId(2), new Term(3), false, 1L, new Term(1), 0L));

            assertThat(effects)
                    .filteredOn(e -> e instanceof RaftEffect.SendAppendEntries)
                    .filteredOn(e -> ((RaftEffect.SendAppendEntries) e).to().equals(new NodeId(2)))
                    .hasSize(1)
                    .first()
                    .satisfies(e -> {
                        var a = (RaftEffect.SendAppendEntries) e;
                        assertThat(a.prevLogIndex()).isEqualTo(1L);
                        assertThat(a.prevLogTerm()).isEqualTo(new Term(1));
                    });
        }
    }

    @Test
    void leaderWithoutConflictTermFallsBackToFollowerConflictIndex(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            // Leader's log: only t3 entries. Follower reports conflictTerm=t2
            // which leader has never seen — leader must defer to follower's
            // conflictIndex rather than scan-and-jump.
            log.append(List.of(
                    new LogEntry(1, new Term(3), LogEntry.Type.NORMAL, new byte[] {1}),
                    new LogEntry(2, new Term(3), LogEntry.Type.NORMAL, new byte[] {2}),
                    new LogEntry(3, new Term(3), LogEntry.Type.NORMAL, new byte[] {3})));
            state.update(new Term(3), Optional.empty());

            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            core.step(new RaftEvent.Tick(TimeUnit.MILLISECONDS.toNanos(5_000)));
            core.step(new RaftEvent.VoteResp(new NodeId(2), new Term(4), true));
            assertThat(core.role()).isEqualTo(Role.LEADER);

            var effects =
                    core.step(new RaftEvent.AppendEntriesResp(new NodeId(2), new Term(4), false, 2L, new Term(2), 0L));

            assertThat(effects)
                    .filteredOn(e -> e instanceof RaftEffect.SendAppendEntries)
                    .filteredOn(e -> ((RaftEffect.SendAppendEntries) e).to().equals(new NodeId(2)))
                    .hasSize(1)
                    .first()
                    .satisfies(e -> {
                        var a = (RaftEffect.SendAppendEntries) e;
                        // fallback: nextIndex = conflictIndex = 2 → prevLogIndex = 1
                        assertThat(a.prevLogIndex()).isEqualTo(1L);
                    });
        }
    }
}
