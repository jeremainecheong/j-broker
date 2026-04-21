package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that successful AppendEntries and granted VoteReq events re-arm the
 * follower's election deadline relative to the event's timestamp, rather than
 * parking it at a sentinel and waiting for the next Tick. Ticks are discrete
 * — parking-then-resetting on next Tick adds up to one tickInterval of slack
 * to the nominal election timeout.
 */
class RaftElectionDeadlineTest {

    // jitter == 0 so the deadline is deterministic.
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
    void appendEntriesReArmsDeadlineAtExactlyElectionTimeoutAfterReceipt(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);

            // Initial deadline was 0 + 1000ms. Arrive an AE at t=500ms.
            core.step(
                    new RaftEvent.AppendEntriesReq(new Term(1), new NodeId(2), 0L, Term.ZERO, List.of(), 0L, ms(500)));

            // Deadline should now be 500 + 1000 = 1500ms.
            // Tick at 1499ms must not trigger election.
            core.step(new RaftEvent.Tick(ms(1499)));
            assertThat(core.role()).isEqualTo(Role.FOLLOWER);

            // Tick at 1501ms must trigger election-start (pre-vote) exactly on schedule.
            core.step(new RaftEvent.Tick(ms(1501)));
            assertThat(core.role()).isEqualTo(Role.PRE_CANDIDATE);
        }
    }

    @Test
    void grantedVoteReArmsDeadlineAtExactlyElectionTimeoutAfterReceipt(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);

            // Grant a vote at t=500ms.
            core.step(new RaftEvent.VoteReq(new Term(1), new NodeId(2), 0L, Term.ZERO, ms(500)));

            // Deadline should now be 500 + 1000 = 1500ms.
            core.step(new RaftEvent.Tick(ms(1499)));
            assertThat(core.role()).isEqualTo(Role.FOLLOWER);

            core.step(new RaftEvent.Tick(ms(1501)));
            assertThat(core.role()).isEqualTo(Role.PRE_CANDIDATE);
        }
    }
}
