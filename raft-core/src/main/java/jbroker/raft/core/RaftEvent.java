package jbroker.raft.core;

import java.util.List;
import java.util.Objects;

/**
 * Input events to a {@link RaftCore}. Sealed so pattern-match switches
 * are exhaustive.
 */
public sealed interface RaftEvent {

    record Tick(long nowNanos) implements RaftEvent {}

    record ClientPropose(byte[] payload) implements RaftEvent {
        public ClientPropose {
            Objects.requireNonNull(payload, "payload");
        }
    }

    record AppendEntriesReq(
            Term term,
            NodeId leaderId,
            long prevLogIndex,
            Term prevLogTerm,
            List<LogEntry> entries,
            long leaderCommit,
            long nowNanos)
            implements RaftEvent {
        public AppendEntriesReq {
            Objects.requireNonNull(term, "term");
            Objects.requireNonNull(leaderId, "leaderId");
            Objects.requireNonNull(prevLogTerm, "prevLogTerm");
            entries = List.copyOf(entries);
        }
    }

    record AppendEntriesResp(
            NodeId from, Term term, boolean success, long conflictIndex, Term conflictTerm, long matchIndex)
            implements RaftEvent {
        public AppendEntriesResp {
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(term, "term");
            Objects.requireNonNull(conflictTerm, "conflictTerm");
        }
    }

    record VoteReq(Term term, NodeId candidateId, long lastLogIndex, Term lastLogTerm, long nowNanos)
            implements RaftEvent {
        public VoteReq {
            Objects.requireNonNull(term, "term");
            Objects.requireNonNull(candidateId, "candidateId");
            Objects.requireNonNull(lastLogTerm, "lastLogTerm");
        }
    }

    record VoteResp(NodeId from, Term term, boolean granted) implements RaftEvent {
        public VoteResp {
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(term, "term");
        }
    }
}
