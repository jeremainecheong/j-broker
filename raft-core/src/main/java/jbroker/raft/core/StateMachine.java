package jbroker.raft.core;

/**
 * User state machine that applies committed log entries.
 * Implementations must be deterministic.
 */
public interface StateMachine {
    void apply(LogEntry entry);
}
