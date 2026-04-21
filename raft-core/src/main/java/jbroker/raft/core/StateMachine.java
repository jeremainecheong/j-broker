package jbroker.raft.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * User state machine that applies committed log entries.
 * Implementations must be deterministic.
 */
public interface StateMachine {
    void apply(LogEntry entry);

    /**
     * Serialise the current state to {@code out}. Called by the raft layer when
     * creating a snapshot for log compaction or bootstrapping a lagging peer.
     * Default implementation throws — implementations that want log compaction
     * must override.
     */
    default void snapshot(OutputStream out) throws IOException {
        throw new UnsupportedOperationException("state machine does not support snapshots");
    }

    /**
     * Replace the current state with the contents of {@code in}. Called by the
     * raft layer when installing a snapshot received from a leader. The state
     * machine must discard whatever it already applied.
     */
    default void restore(InputStream in) throws IOException {
        throw new UnsupportedOperationException("state machine does not support snapshot restore");
    }
}
