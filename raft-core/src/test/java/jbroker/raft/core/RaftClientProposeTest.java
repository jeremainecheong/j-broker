package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RaftClientProposeTest {

    private static final RaftConfig CONFIG = new RaftConfig(
            new NodeId(1),
            List.of(new NodeId(1), new NodeId(2), new NodeId(3)),
            TimeUnit.MILLISECONDS.toNanos(1000),
            TimeUnit.MILLISECONDS.toNanos(500),
            TimeUnit.MILLISECONDS.toNanos(100),
            100);

    @Test
    void followerRejectsClientPropose(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            var effects = core.step(new RaftEvent.ClientPropose(new byte[] {1}));
            assertThat(effects)
                    .filteredOn(e -> e instanceof RaftEffect.RejectClientPropose)
                    .hasSize(1);
        }
    }

    @Test
    void leaderAcceptsAndAppendsClientPropose(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("log.bin"));
                var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            var core = new DefaultRaftCore(CONFIG, log, state, 0L);
            core.step(new RaftEvent.Tick(TimeUnit.MILLISECONDS.toNanos(5_000)));
            core.step(new RaftEvent.VoteResp(new NodeId(2), new Term(1), true));
            var effects = core.step(new RaftEvent.ClientPropose(new byte[] {42}));
            assertThat(log.lastIndex()).isEqualTo(1L);
            assertThat(effects)
                    .filteredOn(e -> e instanceof RaftEffect.PersistLog)
                    .hasSize(1);
        }
    }
}
