package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Graceful leadership transfer via {@code TimeoutNow} (Raft paper §3.10):
 * the current leader picks a target, ensures it is caught up, then sends
 * {@code TimeoutNow}. The target then immediately starts an election at
 * {@code currentTerm + 1}, skipping its own randomised election timeout.
 * Pre-vote is also skipped — the leader has already vouched for this node.
 */
class RaftLeadershipTransferTest {

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
        core.step(new RaftEvent.PreVoteResp(new NodeId(2), Term.ZERO, true));
        core.step(new RaftEvent.VoteResp(new NodeId(2), new Term(1), true));
        assertThat(core.role()).isEqualTo(Role.LEADER);
        return core;
    }

    @Test
    void leaderEmitsSendTimeoutNowOnTransferRequest(@TempDir Path dir) throws Exception {
        var leader = becomeLeader(dir);
        var effects = leader.step(new RaftEvent.TransferLeadership(new NodeId(2)));

        assertThat(effects)
                .filteredOn(e -> e instanceof RaftEffect.SendTimeoutNow)
                .hasSize(1)
                .first()
                .satisfies(e -> {
                    var s = (RaftEffect.SendTimeoutNow) e;
                    assertThat(s.to()).isEqualTo(new NodeId(2));
                    assertThat(s.term()).isEqualTo(new Term(1));
                });
    }

    @Test
    void receivingTimeoutNowSkipsPreVoteAndBecomesCandidateImmediately(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            var effects =
                    core.step(new RaftEvent.TimeoutNow(new NodeId(1), new Term(3), TimeUnit.MILLISECONDS.toNanos(100)));

            // Target bumps its term, becomes CANDIDATE (not PRE_CANDIDATE),
            // broadcasts VoteReq (not PreVoteReq).
            assertThat(core.role()).isEqualTo(Role.CANDIDATE);
            assertThat(core.currentTerm()).isEqualTo(new Term(4));
            assertThat(effects)
                    .filteredOn(e -> e instanceof RaftEffect.SendVoteReq)
                    .hasSize(2);
            assertThat(effects).noneMatch(e -> e instanceof RaftEffect.SendPreVoteReq);
        }
    }

    @Test
    void nonLeaderIgnoresTransferLeadershipRequest(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            var effects = core.step(new RaftEvent.TransferLeadership(new NodeId(2)));
            assertThat(effects).noneMatch(e -> e instanceof RaftEffect.SendTimeoutNow);
        }
    }
}
