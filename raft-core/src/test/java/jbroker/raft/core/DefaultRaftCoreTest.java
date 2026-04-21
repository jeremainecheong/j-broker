package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DefaultRaftCoreTest {

    private static final RaftConfig CONFIG = new RaftConfig(
            new NodeId(1),
            List.of(new NodeId(1), new NodeId(2), new NodeId(3)),
            TimeUnit.MILLISECONDS.toNanos(1000),
            TimeUnit.MILLISECONDS.toNanos(500),
            TimeUnit.MILLISECONDS.toNanos(100),
            100);

    @Test
    void startsAsFollowerAtTermZero(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            assertThat(core.role()).isEqualTo(Role.FOLLOWER);
            assertThat(core.currentTerm()).isEqualTo(Term.ZERO);
        }
    }

    @Test
    void followerTimingOutEntersPreVoteAndThenRequestsRealVotes(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {

            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            long farFuture = TimeUnit.MILLISECONDS.toNanos(5_000);
            var preVoteEffects = core.step(new RaftEvent.Tick(farFuture));

            // Timeout first enters pre-vote (term unchanged).
            assertThat(core.role()).isEqualTo(Role.PRE_CANDIDATE);
            assertThat(core.currentTerm()).isEqualTo(Term.ZERO);
            assertThat(preVoteEffects)
                    .filteredOn(e -> e instanceof RaftEffect.SendPreVoteReq)
                    .extracting(e -> ((RaftEffect.SendPreVoteReq) e).to())
                    .containsExactlyInAnyOrder(new NodeId(2), new NodeId(3));

            // A granted pre-vote triggers the real election.
            var voteEffects = core.step(new RaftEvent.PreVoteResp(new NodeId(2), Term.ZERO, true));

            assertThat(core.role()).isEqualTo(Role.CANDIDATE);
            assertThat(core.currentTerm()).isEqualTo(new Term(1));
            assertThat(voteEffects)
                    .filteredOn(e -> e instanceof RaftEffect.PersistState)
                    .hasSize(1);
            assertThat(voteEffects)
                    .filteredOn(e -> e instanceof RaftEffect.SendVoteReq)
                    .extracting(e -> ((RaftEffect.SendVoteReq) e).to())
                    .containsExactlyInAnyOrder(new NodeId(2), new NodeId(3));
        }
    }

    @Test
    void leaderSendsHeartbeatAfterBecomingLeader(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            core.step(new RaftEvent.Tick(TimeUnit.MILLISECONDS.toNanos(5_000)));
            core.step(new RaftEvent.PreVoteResp(new NodeId(2), Term.ZERO, true));
            core.step(new RaftEvent.VoteResp(new NodeId(2), new Term(1), true));
            var effects = core.step(new RaftEvent.Tick(TimeUnit.MILLISECONDS.toNanos(5_001)));
            assertThat(effects)
                    .filteredOn(e -> e instanceof RaftEffect.SendAppendEntries)
                    .extracting(e -> ((RaftEffect.SendAppendEntries) e).to())
                    .containsExactlyInAnyOrder(new NodeId(2), new NodeId(3));
        }
    }
}
