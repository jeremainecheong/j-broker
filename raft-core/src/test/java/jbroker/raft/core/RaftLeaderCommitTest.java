package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RaftLeaderCommitTest {

    private static final RaftConfig CONFIG = new RaftConfig(
            new NodeId(1),
            List.of(new NodeId(1), new NodeId(2), new NodeId(3)),
            TimeUnit.MILLISECONDS.toNanos(1000),
            TimeUnit.MILLISECONDS.toNanos(500),
            TimeUnit.MILLISECONDS.toNanos(100),
            100);

    private DefaultRaftCore becomeLeader(Path dir) throws Exception {
        var log = FileRaftLog.open(dir.resolve("log.bin"));
        var state = FilePersistentState.open(dir.resolve("state.bin"));
        var core = new DefaultRaftCore(CONFIG, log, state, 0L);
        core.step(new RaftEvent.Tick(TimeUnit.MILLISECONDS.toNanos(5_000)));
        core.step(new RaftEvent.VoteResp(new NodeId(2), new Term(1), true));
        assertThat(core.role()).isEqualTo(Role.LEADER);
        return core;
    }

    @Test
    void leaderCommitsAfterMajorityAck(@TempDir Path dir) throws Exception {
        var core = becomeLeader(dir);
        core.step(new RaftEvent.ClientPropose(new byte[] {1}));
        var effects = core.step(new RaftEvent.AppendEntriesResp(new NodeId(2), new Term(1), true, 0L, Term.ZERO, 1L));
        assertThat(effects)
                .filteredOn(e -> e instanceof RaftEffect.ApplyCommitted)
                .hasSize(1);
    }

    @Test
    void leaderBacksOffOnFailureWithConflictIndex(@TempDir Path dir) throws Exception {
        var core = becomeLeader(dir);
        core.step(new RaftEvent.ClientPropose(new byte[] {1}));
        core.step(new RaftEvent.ClientPropose(new byte[] {2}));
        core.step(new RaftEvent.ClientPropose(new byte[] {3}));
        var effects = core.step(new RaftEvent.AppendEntriesResp(new NodeId(2), new Term(1), false, 2L, Term.ZERO, 0L));
        assertThat(effects)
                .filteredOn(e -> e instanceof RaftEffect.SendAppendEntries)
                .filteredOn(e -> ((RaftEffect.SendAppendEntries) e).to().equals(new NodeId(2)))
                .hasSize(1)
                .first()
                .satisfies(e -> {
                    var a = (RaftEffect.SendAppendEntries) e;
                    assertThat(a.prevLogIndex()).isEqualTo(1L);
                });
    }
}
