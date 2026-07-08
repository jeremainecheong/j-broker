package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Per-leader at-most-once semantics for client proposals. Each
 * {@link RaftEvent.ClientPropose} carries a {@code clientId} + {@code clientSeq}
 * pair. The leader tracks the highest seq seen per client and short-circuits a
 * duplicate by emitting {@link RaftEffect.DuplicateClientPropose} without
 * re-appending or re-applying.
 *
 * <p>Scope: in-memory, per leader-term. Dedup state is lost on leadership
 * change (rebuilt from log scan on becoming leader). Sufficient to guarantee
 * that duplicate client requests return the cached response, not
 * re-executed, within a single leadership.
 */
class RaftClientDedupTest {

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
    void leaderDeDupesRepeatedClientPropose(@TempDir Path dir) throws Exception {
        var core = becomeLeader(dir);

        var first = core.step(new RaftEvent.ClientPropose(42L, 1L, new byte[] {9}));
        assertThat(first).filteredOn(e -> e instanceof RaftEffect.PersistLog).hasSize(1);
        assertThat(first).noneMatch(e -> e instanceof RaftEffect.DuplicateClientPropose);

        // Same (clientId=42, seq=1) submitted again — must not append.
        var second = core.step(new RaftEvent.ClientPropose(42L, 1L, new byte[] {9}));
        assertThat(second).noneMatch(e -> e instanceof RaftEffect.PersistLog);
        assertThat(second)
                .filteredOn(e -> e instanceof RaftEffect.DuplicateClientPropose)
                .hasSize(1);
    }

    @Test
    void dedupIsPerClient(@TempDir Path dir) throws Exception {
        var core = becomeLeader(dir);

        core.step(new RaftEvent.ClientPropose(42L, 1L, new byte[] {1}));
        // Different client, seq=1 — not a duplicate.
        var effects = core.step(new RaftEvent.ClientPropose(43L, 1L, new byte[] {2}));
        assertThat(effects).filteredOn(e -> e instanceof RaftEffect.PersistLog).hasSize(1);
        assertThat(effects).noneMatch(e -> e instanceof RaftEffect.DuplicateClientPropose);
    }

    @Test
    void clientIdZeroIsNotDeduped(@TempDir Path dir) throws Exception {
        // clientId == 0 means "no dedup" (unauthenticated / legacy producer).
        var core = becomeLeader(dir);

        core.step(new RaftEvent.ClientPropose(0L, 1L, new byte[] {1}));
        var effects = core.step(new RaftEvent.ClientPropose(0L, 1L, new byte[] {1}));
        assertThat(effects).filteredOn(e -> e instanceof RaftEffect.PersistLog).hasSize(1);
        assertThat(effects).noneMatch(e -> e instanceof RaftEffect.DuplicateClientPropose);
    }
}
