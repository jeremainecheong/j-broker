package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RaftAppendEntriesTest {

    private static final RaftConfig CONFIG = new RaftConfig(
            new NodeId(1),
            List.of(new NodeId(1), new NodeId(2), new NodeId(3)),
            TimeUnit.MILLISECONDS.toNanos(1000),
            TimeUnit.MILLISECONDS.toNanos(500),
            TimeUnit.MILLISECONDS.toNanos(100),
            100);

    @Test
    void acceptsAppendAtEmptyLog(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            var entry = new LogEntry(1, new Term(1), LogEntry.Type.NORMAL, new byte[] {1});
            var effects = core.step(
                    new RaftEvent.AppendEntriesReq(new Term(1), new NodeId(2), 0L, Term.ZERO, List.of(entry), 0L));
            assertThat(effects)
                    .filteredOn(e -> e instanceof RaftEffect.SendAppendEntriesResp)
                    .extracting(e -> ((RaftEffect.SendAppendEntriesResp) e).success())
                    .containsExactly(true);
            assertThat(log.lastIndex()).isEqualTo(1L);
        }
    }

    @Test
    void rejectsAppendFromStaleLeader(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            state.update(new Term(5), java.util.Optional.empty());
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            var effects =
                    core.step(new RaftEvent.AppendEntriesReq(new Term(3), new NodeId(2), 0L, Term.ZERO, List.of(), 0L));
            assertThat(effects)
                    .filteredOn(e -> e instanceof RaftEffect.SendAppendEntriesResp)
                    .extracting(e -> ((RaftEffect.SendAppendEntriesResp) e).success())
                    .containsExactly(false);
        }
    }

    @Test
    void rejectsAppendWithMismatchedPrevLogTerm(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            log.append(List.of(new LogEntry(1, new Term(2), LogEntry.Type.NORMAL, new byte[0])));
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            var effects = core.step(
                    new RaftEvent.AppendEntriesReq(new Term(3), new NodeId(2), 1L, new Term(1), List.of(), 0L));
            assertThat(effects)
                    .filteredOn(e -> e instanceof RaftEffect.SendAppendEntriesResp)
                    .extracting(e -> ((RaftEffect.SendAppendEntriesResp) e).success())
                    .containsExactly(false);
        }
    }

    @Test
    void truncatesConflictingEntriesBeforeAppending(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            log.append(List.of(
                    new LogEntry(1, new Term(1), LogEntry.Type.NORMAL, new byte[] {1}),
                    new LogEntry(2, new Term(1), LogEntry.Type.NORMAL, new byte[] {2}),
                    new LogEntry(3, new Term(1), LogEntry.Type.NORMAL, new byte[] {3})));
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            var newEntry = new LogEntry(2, new Term(2), LogEntry.Type.NORMAL, new byte[] {9});
            core.step(
                    new RaftEvent.AppendEntriesReq(new Term(2), new NodeId(2), 1L, new Term(1), List.of(newEntry), 0L));
            assertThat(log.lastIndex()).isEqualTo(2L);
            assertThat(log.read(2, 1).get(0).payload()).containsExactly(9);
        }
    }

    @Test
    void advancesCommitIndexFromLeaderCommit(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            var entries = List.of(
                    new LogEntry(1, new Term(1), LogEntry.Type.NORMAL, new byte[] {1}),
                    new LogEntry(2, new Term(1), LogEntry.Type.NORMAL, new byte[] {2}));
            var effects =
                    core.step(new RaftEvent.AppendEntriesReq(new Term(1), new NodeId(2), 0L, Term.ZERO, entries, 2L));
            long applied = effects.stream()
                    .filter(e -> e instanceof RaftEffect.ApplyCommitted)
                    .count();
            assertThat(applied).isEqualTo(2);
        }
    }
}
