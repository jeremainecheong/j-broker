package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RaftPersistenceTest {

    private static final RaftConfig CONFIG = new RaftConfig(
            new NodeId(1),
            List.of(new NodeId(1), new NodeId(2), new NodeId(3)),
            TimeUnit.MILLISECONDS.toNanos(1000),
            TimeUnit.MILLISECONDS.toNanos(500),
            TimeUnit.MILLISECONDS.toNanos(100),
            100);

    @Test
    void termAndVoteSurviveRestart(@TempDir Path dir) throws Exception {
        var logPath = dir.resolve("log.bin");
        var statePath = dir.resolve("state.bin");

        try (var log = FileRaftLog.open(logPath);
                var state = FilePersistentState.open(statePath)) {
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            // Drive the pre-vote then promote to real candidate so the term actually bumps.
            core.step(new RaftEvent.Tick(TimeUnit.MILLISECONDS.toNanos(5_000)));
            core.step(new RaftEvent.PreVoteResp(new NodeId(2), Term.ZERO, true));
            assertThat(core.currentTerm()).isEqualTo(new Term(1));
        }

        try (var log = FileRaftLog.open(logPath);
                var state = FilePersistentState.open(statePath)) {
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            assertThat(core.currentTerm()).isEqualTo(new Term(1));
            assertThat(core.role()).isEqualTo(Role.FOLLOWER);
        }
    }

    @Test
    void committedEntriesSurviveRestart(@TempDir Path dir) throws Exception {
        var logPath = dir.resolve("log.bin");
        try (var log = FileRaftLog.open(logPath)) {
            log.append(List.of(
                    new LogEntry(1, new Term(1), LogEntry.Type.NORMAL, new byte[] {1}),
                    new LogEntry(2, new Term(1), LogEntry.Type.NORMAL, new byte[] {2})));
        }
        try (var reopened = FileRaftLog.open(logPath)) {
            assertThat(reopened.lastIndex()).isEqualTo(2L);
        }
    }
}
