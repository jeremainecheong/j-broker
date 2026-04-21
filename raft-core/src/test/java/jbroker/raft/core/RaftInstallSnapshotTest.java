package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * InstallSnapshot RPC (Raft §7). When a follower lags so far that the leader
 * has already compacted the log entries the follower needs, the leader sends
 * the current snapshot instead of AppendEntries. The follower replaces its
 * state machine, truncates its log prefix, and advances commitIndex/
 * lastApplied to the snapshot's lastIncludedIndex.
 */
class RaftInstallSnapshotTest {

    private static final NodeId N1 = new NodeId(1);
    private static final NodeId N2 = new NodeId(2);
    private static final NodeId N3 = new NodeId(3);

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

    @Test
    void followerInstallsSnapshotAndAdvancesAppliedIndex(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            var core = new DefaultRaftCore(FOLLOWER_CONFIG, log, state, 0L);

            byte[] snapshotBytes = new byte[] {1, 2, 3, 4};
            var effects = core.step(new RaftEvent.InstallSnapshotReq(
                    new Term(5),
                    N1,
                    /* lastIncludedIndex */ 10L,
                    /* lastIncludedTerm */ new Term(3),
                    snapshotBytes,
                    TimeUnit.MILLISECONDS.toNanos(100)));

            assertThat(effects)
                    .filteredOn(e -> e instanceof RaftEffect.ApplySnapshot)
                    .hasSize(1)
                    .first()
                    .satisfies(e -> {
                        var a = (RaftEffect.ApplySnapshot) e;
                        assertThat(a.lastIncludedIndex()).isEqualTo(10L);
                        assertThat(a.lastIncludedTerm()).isEqualTo(new Term(3));
                        assertThat(a.snapshot()).isEqualTo(snapshotBytes);
                    });
            assertThat(effects)
                    .filteredOn(e -> e instanceof RaftEffect.SendInstallSnapshotResp)
                    .hasSize(1);

            // commitIndex and lastApplied jumped to the snapshot point.
            assertThat(core.commitIndex()).isEqualTo(10L);
            assertThat(core.lastApplied()).isEqualTo(10L);
            // Term was advanced to match the snapshot.
            assertThat(core.currentTerm()).isEqualTo(new Term(5));
            // Log was truncated to the snapshot's prefix.
            assertThat(log.lastIncludedIndex()).isEqualTo(10L);
            assertThat(log.lastIncludedTerm()).isEqualTo(new Term(3));
            assertThat(log.firstIndex()).isEqualTo(11L);
        }
    }

    @Test
    void staleSnapshotWhereSnapIdxAlreadyAppliedIsAckedWithoutRewinding(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            var core = new DefaultRaftCore(FOLLOWER_CONFIG, log, state, 0L);

            // First snapshot advances us to idx 10.
            core.step(new RaftEvent.InstallSnapshotReq(
                    new Term(5), N1, 10L, new Term(3), new byte[] {1, 2, 3}, TimeUnit.MILLISECONDS.toNanos(100)));
            assertThat(core.lastApplied()).isEqualTo(10L);

            // A second, stale snapshot at idx 5 from the same term must not
            // rewind commit/applied and must not emit ApplySnapshot.
            var effects = core.step(new RaftEvent.InstallSnapshotReq(
                    new Term(5), N1, 5L, new Term(2), new byte[] {9, 9, 9}, TimeUnit.MILLISECONDS.toNanos(200)));

            assertThat(core.lastApplied()).isEqualTo(10L);
            assertThat(core.commitIndex()).isEqualTo(10L);
            assertThat(effects).noneMatch(e -> e instanceof RaftEffect.ApplySnapshot);
            assertThat(effects)
                    .filteredOn(e -> e instanceof RaftEffect.SendInstallSnapshotResp)
                    .hasSize(1);
        }
    }

    @Test
    void leaderEmitsInstallSnapshotWhenPeerNextIndexBehindCompaction(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            var core = new DefaultRaftCore(LEADER_CONFIG, log, state, 0L);

            // Pre-seed some log entries, then compact up through index 5.
            log.append(List.of(
                    new LogEntry(1L, new Term(1), LogEntry.Type.NORMAL, new byte[] {1}),
                    new LogEntry(2L, new Term(1), LogEntry.Type.NORMAL, new byte[] {2}),
                    new LogEntry(3L, new Term(1), LogEntry.Type.NORMAL, new byte[] {3}),
                    new LogEntry(4L, new Term(1), LogEntry.Type.NORMAL, new byte[] {4}),
                    new LogEntry(5L, new Term(1), LogEntry.Type.NORMAL, new byte[] {5}),
                    new LogEntry(6L, new Term(1), LogEntry.Type.NORMAL, new byte[] {6})));
            log.truncatePrefix(6L, new Term(1));

            // Become leader.
            core.step(new RaftEvent.Tick(TimeUnit.MILLISECONDS.toNanos(5_000)));
            core.step(new RaftEvent.PreVoteResp(N2, Term.ZERO, true));
            core.step(new RaftEvent.VoteResp(N2, new Term(1), true));
            assertThat(core.role()).isEqualTo(Role.LEADER);

            byte[] snapBytes = new byte[] {9, 9, 9};
            core.installSnapshotBytesForTesting(snapBytes);

            // A follower response saying "my log is short" forces the leader to
            // back off nextIndex[N2] to 1 — which is BEHIND the compaction
            // point (firstIndex=6). The leader must fall back to
            // InstallSnapshot rather than looping forever.
            var effects = core.step(new RaftEvent.AppendEntriesResp(
                    N2, new Term(1), /* success */ false, /* conflictIndex */ 1L, Term.ZERO, 0L));

            assertThat(effects)
                    .filteredOn(e -> e instanceof RaftEffect.SendInstallSnapshot)
                    .hasSize(1)
                    .first()
                    .satisfies(e -> {
                        var s = (RaftEffect.SendInstallSnapshot) e;
                        assertThat(s.to()).isEqualTo(N2);
                        assertThat(s.lastIncludedIndex()).isEqualTo(5L);
                        assertThat(s.lastIncludedTerm()).isEqualTo(new Term(1));
                        assertThat(s.snapshot()).isEqualTo(snapBytes);
                    });
        }
    }
}
