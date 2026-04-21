package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class RaftConfigTest {

    private static RaftConfig build(long electionTimeout, long jitter, long heartbeat, int maxEntries) {
        return new RaftConfig(new NodeId(1), List.of(new NodeId(1), new NodeId(2)), electionTimeout, jitter, heartbeat, maxEntries);
    }

    @Test
    void rejectsNegativeElectionJitter() {
        assertThatThrownBy(() -> build(1000, -1, 100, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("electionJitterNanos");
    }

    @Test
    void acceptsZeroElectionJitter() {
        // jitter == 0 means "no randomisation" — must not throw.
        build(1000, 0, 100, 10);
    }
}
