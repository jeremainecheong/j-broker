package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pre-vote extension (Ongaro thesis §9.6): before incrementing the term and
 * soliciting real votes, a node broadcasts PreVoteReq with a hypothetical
 * term+1 that does not mutate any state. A partitioned node that rejoins with
 * an inflated view of time cannot then disrupt the cluster — its real
 * election only starts if a majority agrees in advance that its log is
 * current enough to become leader.
 */
class RaftPreVoteTest {

    private static final RaftConfig CONFIG = new RaftConfig(
            new NodeId(1),
            List.of(new NodeId(1), new NodeId(2), new NodeId(3)),
            TimeUnit.MILLISECONDS.toNanos(1000),
            0L,
            TimeUnit.MILLISECONDS.toNanos(100),
            100);

    private static long ms(long millis) {
        return TimeUnit.MILLISECONDS.toNanos(millis);
    }

    @Test
    void electionTimeoutBroadcastsPreVoteAndDoesNotMutateTerm(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            var effects = core.step(new RaftEvent.Tick(ms(5_000)));

            assertThat(effects)
                    .filteredOn(e -> e instanceof RaftEffect.SendPreVoteReq)
                    .hasSize(2);
            assertThat(effects).noneMatch(e -> e instanceof RaftEffect.SendVoteReq);
            assertThat(core.role()).isEqualTo(Role.PRE_CANDIDATE);
            assertThat(core.currentTerm()).isEqualTo(Term.ZERO);
        }
    }

    @Test
    void majorityGrantedPreVotesTriggerRealElection(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            core.step(new RaftEvent.Tick(ms(5_000)));
            // Peer's PreVoteResp carries its own currentTerm (same as ours — 0).
            // Granted at the hypothetical term (which is not echoed back in the response).
            var effects = core.step(new RaftEvent.PreVoteResp(new NodeId(2), Term.ZERO, true));

            assertThat(effects)
                    .filteredOn(e -> e instanceof RaftEffect.SendVoteReq)
                    .hasSize(2);
            assertThat(core.role()).isEqualTo(Role.CANDIDATE);
            assertThat(core.currentTerm()).isEqualTo(new Term(1));
        }
    }

    @Test
    void preVoteDeniedLeavesRoleAndTermUnchanged(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            core.step(new RaftEvent.Tick(ms(5_000)));
            core.step(new RaftEvent.PreVoteResp(new NodeId(2), Term.ZERO, false));
            core.step(new RaftEvent.PreVoteResp(new NodeId(3), Term.ZERO, false));

            assertThat(core.role()).isNotEqualTo(Role.CANDIDATE);
            assertThat(core.role()).isNotEqualTo(Role.LEADER);
            assertThat(core.currentTerm()).isEqualTo(Term.ZERO);
        }
    }

    @Test
    void grantsPreVoteWhenOwnDeadlineElapsedAndLogUpToDate(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            // Initial deadline is 1000ms; at 1500ms it's elapsed.
            var effects = core.step(new RaftEvent.PreVoteReq(new Term(2), new NodeId(2), 0L, Term.ZERO, ms(1500)));

            assertThat(effects)
                    .filteredOn(e -> e instanceof RaftEffect.SendPreVoteResp)
                    .hasSize(1)
                    .first()
                    .extracting(e -> ((RaftEffect.SendPreVoteResp) e).granted())
                    .isEqualTo(true);
            // Crucially, state is unchanged — no term bump, no vote persisted.
            assertThat(core.currentTerm()).isEqualTo(Term.ZERO);
            assertThat(core.role()).isEqualTo(Role.FOLLOWER);
        }
    }

    @Test
    void deniesPreVoteWhenLeaderIsKnownAndDeadlineNotElapsed(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            // Establish a leader via a heartbeat at t=100ms; sets leaderId and
            // re-arms deadline to 100 + 1000 = 1100ms.
            core.step(
                    new RaftEvent.AppendEntriesReq(new Term(1), new NodeId(3), 0L, Term.ZERO, List.of(), 0L, ms(100)));
            // Pre-vote request arrives at t=500ms — we still have an active leader
            // (leaderId set AND deadline not yet elapsed).
            var effects = core.step(new RaftEvent.PreVoteReq(new Term(2), new NodeId(2), 0L, Term.ZERO, ms(500)));

            assertThat(effects)
                    .filteredOn(e -> e instanceof RaftEffect.SendPreVoteResp)
                    .first()
                    .extracting(e -> ((RaftEffect.SendPreVoteResp) e).granted())
                    .isEqualTo(false);
        }
    }

    @Test
    void deniesPreVoteWhenCandidateLogIsStale(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            log.append(List.of(new LogEntry(1, new Term(5), LogEntry.Type.NORMAL, new byte[0])));
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            var effects = core.step(new RaftEvent.PreVoteReq(new Term(6), new NodeId(2), 0L, new Term(1), ms(1500)));

            assertThat(effects)
                    .filteredOn(e -> e instanceof RaftEffect.SendPreVoteResp)
                    .first()
                    .extracting(e -> ((RaftEffect.SendPreVoteResp) e).granted())
                    .isEqualTo(false);
        }
    }
}
