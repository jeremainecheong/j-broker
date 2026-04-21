package jbroker.raft.core;

import java.util.List;
import java.util.Objects;

/**
 * Input events to a {@link RaftCore}. Sealed so pattern-match switches
 * are exhaustive.
 */
public sealed interface RaftEvent {

    record Tick(long nowNanos) implements RaftEvent {}

    record ClientPropose(long clientId, long clientSeq, byte[] payload) implements RaftEvent {
        public ClientPropose {
            Objects.requireNonNull(payload, "payload");
        }

        /** Convenience for legacy / test call sites that don't track client identity. */
        public ClientPropose(byte[] payload) {
            this(0L, 0L, payload);
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

    record PreVoteReq(Term term, NodeId candidateId, long lastLogIndex, Term lastLogTerm, long nowNanos)
            implements RaftEvent {
        public PreVoteReq {
            Objects.requireNonNull(term, "term");
            Objects.requireNonNull(candidateId, "candidateId");
            Objects.requireNonNull(lastLogTerm, "lastLogTerm");
        }
    }

    record PreVoteResp(NodeId from, Term term, boolean granted) implements RaftEvent {
        public PreVoteResp {
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(term, "term");
        }
    }

    /** Client-/admin-initiated: ask the current leader to hand off to {@code target}. */
    record TransferLeadership(NodeId target) implements RaftEvent {
        public TransferLeadership {
            Objects.requireNonNull(target, "target");
        }
    }

    /** From current leader: start a real election at term+1 immediately, skipping pre-vote. */
    record TimeoutNow(NodeId from, Term term, long nowNanos) implements RaftEvent {
        public TimeoutNow {
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(term, "term");
        }
    }

    /**
     * Linearizable read request. {@code clientId} + {@code requestId} are opaque
     * identifiers the driver uses to correlate the response; they do not affect
     * raft state. {@code clientId==0} is allowed (legacy / unauthenticated).
     */
    record ClientRead(long clientId, long requestId) implements RaftEvent {}
}
