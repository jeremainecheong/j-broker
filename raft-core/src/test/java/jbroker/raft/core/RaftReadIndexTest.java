package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Read-index linearizable reads (Raft §6.4).
 *
 * <p>On {@code ClientRead}, the leader snapshots {@code commitIndex} as the
 * read's {@code readIndex}, broadcasts a heartbeat round, and — after receiving
 * success responses from a quorum at the current term — waits until
 * {@code lastApplied >= readIndex} before serving. If the leader steps down
 * before the quorum arrives, the pending read is aborted rather than returning
 * a potentially stale value.
 *
 * <p>Scope: core-level only. Proto wiring + client RPC come later; here we
 * assert the effects and state transitions of {@link DefaultRaftCore} directly.
 */
class RaftReadIndexTest {

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
    void nonLeaderImmediatelyRejectsClientRead(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            var effects = core.step(new RaftEvent.ClientRead(7L, 100L));

            assertThat(effects)
                    .filteredOn(e -> e instanceof RaftEffect.RejectClientRead)
                    .hasSize(1)
                    .first()
                    .satisfies(e -> {
                        var r = (RaftEffect.RejectClientRead) e;
                        assertThat(r.clientId()).isEqualTo(7L);
                        assertThat(r.requestId()).isEqualTo(100L);
                    });
            assertThat(effects).noneMatch(e -> e instanceof RaftEffect.ServeClientRead);
        }
    }

    /** Pull the heartbeatSeq the leader stamped on the AE it just sent to {@code to}. */
    private static long heartbeatSeqSentTo(java.util.List<RaftEffect> effects, NodeId to) {
        return effects.stream()
                .filter(e -> e instanceof RaftEffect.SendAppendEntries s && s.to().equals(to))
                .map(e -> ((RaftEffect.SendAppendEntries) e).heartbeatSeq())
                .findFirst()
                .orElseThrow();
    }

    @Test
    void leaderServesReadAfterQuorumHeartbeatAck(@TempDir Path dir) throws Exception {
        var core = becomeLeader(dir);

        // Propose + fully commit a write so lastApplied > 0.
        var proposeEffects = core.step(new RaftEvent.ClientPropose(new byte[] {42}));
        long proposeSeqP2 = heartbeatSeqSentTo(proposeEffects, new NodeId(2));
        long proposeSeqP3 = heartbeatSeqSentTo(proposeEffects, new NodeId(3));
        core.step(new RaftEvent.AppendEntriesResp(new NodeId(2), new Term(1), true, 0L, Term.ZERO, 1L, proposeSeqP2));
        core.step(new RaftEvent.AppendEntriesResp(new NodeId(3), new Term(1), true, 0L, Term.ZERO, 1L, proposeSeqP3));
        assertThat(core.commitIndex()).isEqualTo(1L);
        assertThat(core.lastApplied()).isEqualTo(1L);

        // Client read arrives. Expect: fresh heartbeat round emitted, no Serve yet.
        var readEffects = core.step(new RaftEvent.ClientRead(9L, 200L));
        assertThat(readEffects)
                .filteredOn(e -> e instanceof RaftEffect.SendAppendEntries)
                .hasSize(2);
        assertThat(readEffects).noneMatch(e -> e instanceof RaftEffect.ServeClientRead);
        long readSeqP2 = heartbeatSeqSentTo(readEffects, new NodeId(2));

        // One peer acks the fresh heartbeat → quorum reached → serve.
        var ackEffects = core.step(
                new RaftEvent.AppendEntriesResp(new NodeId(2), new Term(1), true, 0L, Term.ZERO, 1L, readSeqP2));
        assertThat(ackEffects)
                .filteredOn(e -> e instanceof RaftEffect.ServeClientRead)
                .hasSize(1)
                .first()
                .satisfies(e -> {
                    var s = (RaftEffect.ServeClientRead) e;
                    assertThat(s.clientId()).isEqualTo(9L);
                    assertThat(s.requestId()).isEqualTo(200L);
                    assertThat(s.readIndex()).isEqualTo(1L);
                });
    }

    @Test
    void staleInFlightAckDoesNotConfirmRead(@TempDir Path dir) throws Exception {
        // Regression for the "simplified read-index is unsafe" finding.
        // Scenario: a peer's AE response for a heartbeat sent BEFORE the
        // ClientRead arrives AFTER the ClientRead registers. Without the
        // barrier-seq gate, it would falsely confirm quorum — even though
        // that peer may have seen a higher-term leader in the meantime.
        var core = becomeLeader(dir);

        // Send some AE, but don't deliver responses yet; they're "in flight".
        var priorEffects = core.step(new RaftEvent.ClientPropose(new byte[] {1}));
        long priorSeqP2 = heartbeatSeqSentTo(priorEffects, new NodeId(2));

        // ClientRead arrives NOW, registering its barrier at the current seq.
        core.step(new RaftEvent.ClientRead(99L, 500L));

        // The in-flight response for the PRIOR heartbeat lands. It must not
        // count toward the read's quorum.
        var lateEffects = core.step(
                new RaftEvent.AppendEntriesResp(new NodeId(2), new Term(1), true, 0L, Term.ZERO, 1L, priorSeqP2));
        assertThat(lateEffects).noneMatch(e -> e instanceof RaftEffect.ServeClientRead);
    }

    @Test
    void readAbortsIfLeaderStepsDownBeforeQuorum(@TempDir Path dir) throws Exception {
        var core = becomeLeader(dir);

        core.step(new RaftEvent.ClientRead(11L, 300L));
        assertThat(core.role()).isEqualTo(Role.LEADER);

        // A peer responds with a higher term → we step down. The pending read
        // must be aborted rather than left to dangle.
        var stepDownEffects =
                core.step(new RaftEvent.AppendEntriesResp(new NodeId(2), new Term(5), false, 0L, Term.ZERO, 0L));

        assertThat(core.role()).isEqualTo(Role.FOLLOWER);
        assertThat(stepDownEffects)
                .filteredOn(e -> e instanceof RaftEffect.RejectClientRead)
                .hasSize(1)
                .first()
                .satisfies(e -> {
                    var r = (RaftEffect.RejectClientRead) e;
                    assertThat(r.clientId()).isEqualTo(11L);
                    assertThat(r.requestId()).isEqualTo(300L);
                });
        assertThat(stepDownEffects).noneMatch(e -> e instanceof RaftEffect.ServeClientRead);
    }

    @Test
    void readWaitsForLastAppliedToCatchUpBeforeServing(@TempDir Path dir) throws Exception {
        var core = becomeLeader(dir);

        // Write proposed but NOT yet committed (only leader's own match advances).
        core.step(new RaftEvent.ClientPropose(new byte[] {1}));
        assertThat(core.commitIndex()).isEqualTo(0L);
        assertThat(core.lastApplied()).isEqualTo(0L);

        // ClientRead sees commitIndex==0, snapshots readIndex=0. Heartbeat
        // quorum lands quickly; read should serve at readIndex=0 immediately.
        var readEffects = core.step(new RaftEvent.ClientRead(13L, 400L));
        long readSeq = heartbeatSeqSentTo(readEffects, new NodeId(2));
        var effects = core.step(
                new RaftEvent.AppendEntriesResp(new NodeId(2), new Term(1), true, 0L, Term.ZERO, 1L, readSeq));
        assertThat(effects)
                .filteredOn(e -> e instanceof RaftEffect.ServeClientRead)
                .hasSize(1)
                .first()
                .satisfies(e -> {
                    var s = (RaftEffect.ServeClientRead) e;
                    assertThat(s.readIndex()).isEqualTo(0L);
                });
    }
}
