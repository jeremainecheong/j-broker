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
}
