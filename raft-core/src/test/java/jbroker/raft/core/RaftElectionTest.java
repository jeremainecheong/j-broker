package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RaftElectionTest {

    private static final RaftConfig CONFIG = new RaftConfig(
            new NodeId(1),
            List.of(new NodeId(1), new NodeId(2), new NodeId(3)),
            TimeUnit.MILLISECONDS.toNanos(1000),
            TimeUnit.MILLISECONDS.toNanos(500),
            TimeUnit.MILLISECONDS.toNanos(100),
            100);

    @Test
    void grantsVoteToCandidateWithUpToDateLog(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            var effects = core.step(new RaftEvent.VoteReq(new Term(1), new NodeId(2), 0L, Term.ZERO, 0L));
            assertThat(effects)
                    .filteredOn(e -> e instanceof RaftEffect.SendVoteResp)
                    .extracting(e -> ((RaftEffect.SendVoteResp) e).granted())
                    .containsExactly(true);
            assertThat(core.currentTerm()).isEqualTo(new Term(1));
        }
    }

    @Test
    void rejectsSecondVoteInSameTerm(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            core.step(new RaftEvent.VoteReq(new Term(1), new NodeId(2), 0L, Term.ZERO, 0L));
            var effects = core.step(new RaftEvent.VoteReq(new Term(1), new NodeId(3), 0L, Term.ZERO, 0L));
            assertThat(effects)
                    .filteredOn(e -> e instanceof RaftEffect.SendVoteResp)
                    .extracting(e -> ((RaftEffect.SendVoteResp) e).granted())
                    .containsExactly(false);
        }
    }

    @Test
    void rejectsVoteWithStaleLog(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            log.append(List.of(new LogEntry(1, new Term(5), LogEntry.Type.NORMAL, new byte[0])));
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            var effects = core.step(new RaftEvent.VoteReq(new Term(6), new NodeId(2), 0L, new Term(1), 0L));
            assertThat(effects)
                    .filteredOn(e -> e instanceof RaftEffect.SendVoteResp)
                    .extracting(e -> ((RaftEffect.SendVoteResp) e).granted())
                    .containsExactly(false);
        }
    }

    @Test
    void majorityVotesMakeCandidateLeader(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            core.step(new RaftEvent.Tick(TimeUnit.MILLISECONDS.toNanos(5_000)));
            // Timeout enters pre-vote, not candidacy directly.
            assertThat(core.role()).isEqualTo(Role.PRE_CANDIDATE);
            core.step(new RaftEvent.PreVoteResp(new NodeId(2), Term.ZERO, true));
            assertThat(core.role()).isEqualTo(Role.CANDIDATE);
            core.step(new RaftEvent.VoteResp(new NodeId(2), new Term(1), true));
            assertThat(core.role()).isEqualTo(Role.LEADER);
        }
    }
}
