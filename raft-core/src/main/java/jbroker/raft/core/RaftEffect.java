package jbroker.raft.core;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Outputs produced by a {@link RaftCore#step} call. The driver is
 * responsible for executing them (sending RPCs, persisting, applying).
 */
public sealed interface RaftEffect {

    record SendAppendEntries(
            NodeId to,
            Term term,
            NodeId leaderId,
            long prevLogIndex,
            Term prevLogTerm,
            List<LogEntry> entries,
            long leaderCommit)
            implements RaftEffect {
        public SendAppendEntries {
            Objects.requireNonNull(to);
            Objects.requireNonNull(term);
            Objects.requireNonNull(leaderId);
            Objects.requireNonNull(prevLogTerm);
            entries = List.copyOf(entries);
        }
    }

    record SendAppendEntriesResp(
            NodeId to, Term term, boolean success, long conflictIndex, Term conflictTerm, long matchIndex)
            implements RaftEffect {}

    record SendVoteReq(NodeId to, Term term, NodeId candidateId, long lastLogIndex, Term lastLogTerm)
            implements RaftEffect {}

    record SendVoteResp(NodeId to, Term term, boolean granted) implements RaftEffect {}

    /** Persist log entries; driver must fsync before considering this effect complete. */
    record PersistLog(List<LogEntry> entries) implements RaftEffect {
        public PersistLog {
            entries = List.copyOf(entries);
        }
    }

    /** Truncate log from the given index onward (driver still fsyncs). */
    record TruncateLog(long fromIndex) implements RaftEffect {}

    /** Persist (term, votedFor). Driver must fsync before effect complete. */
    record PersistState(Term term, Optional<NodeId> votedFor) implements RaftEffect {}

    /** Apply a committed entry to the state machine. */
    record ApplyCommitted(LogEntry entry) implements RaftEffect {}

    /** Inform the driver the proposal cannot be accepted (not leader). */
    record RejectClientPropose(Optional<NodeId> knownLeader) implements RaftEffect {}

    record SendPreVoteReq(NodeId to, Term hypotheticalTerm, NodeId candidateId, long lastLogIndex, Term lastLogTerm)
            implements RaftEffect {}

    record SendPreVoteResp(NodeId to, Term term, boolean granted) implements RaftEffect {}

    /**
     * Signals that a {@link RaftEvent.ClientPropose} was recognised as a duplicate
     * of an already-accepted {@code (clientId, clientSeq)} pair on this leader.
     * The entry was not re-appended; the driver should surface the cached
     * success response to the client.
     */
    record DuplicateClientPropose(long clientId, long clientSeq) implements RaftEffect {}

    /** Leader tells {@code to} to start a real election at term+1 immediately. */
    record SendTimeoutNow(NodeId to, Term term) implements RaftEffect {}

    /**
     * Read-index confirmed: driver may now read the state machine and return
     * it to the client. {@code readIndex} is the {@code commitIndex} captured
     * when the read arrived; by the time this effect is emitted,
     * {@code lastApplied >= readIndex} is guaranteed.
     */
    record ServeClientRead(long clientId, long requestId, long readIndex) implements RaftEffect {}

    /**
     * Read-index aborted: either this node is not the leader, or it stepped
     * down before the heartbeat-quorum confirmation completed. Driver should
     * redirect the client to {@code knownLeader} if present.
     */
    record RejectClientRead(long clientId, long requestId, Optional<NodeId> knownLeader) implements RaftEffect {}

    /**
     * Admin-initiated config change was refused — typically because another
     * config change is still in flight, or this node is not the leader.
     */
    record RejectConfigChange(String reason) implements RaftEffect {}

    /** Leader → follower: install full state-machine snapshot. */
    record SendInstallSnapshot(
            NodeId to, Term term, NodeId leaderId, long lastIncludedIndex, Term lastIncludedTerm, byte[] snapshot)
            implements RaftEffect {}

    /** Follower → leader: snapshot install completed (or term bumped). */
    record SendInstallSnapshotResp(NodeId to, Term term) implements RaftEffect {}

    /**
     * Driver should replace the state machine's contents with {@code snapshot}
     * (via {@link StateMachine#restore}). The raft layer has already updated
     * {@code commitIndex} / {@code lastApplied} and truncated the log prefix;
     * the driver only needs to restore the state machine.
     */
    record ApplySnapshot(long lastIncludedIndex, Term lastIncludedTerm, byte[] snapshot) implements RaftEffect {}
}
