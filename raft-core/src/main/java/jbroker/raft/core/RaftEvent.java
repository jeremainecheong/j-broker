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
            long nowNanos,
            long heartbeatSeq)
            implements RaftEvent {
        public AppendEntriesReq {
            Objects.requireNonNull(term, "term");
            Objects.requireNonNull(leaderId, "leaderId");
            Objects.requireNonNull(prevLogTerm, "prevLogTerm");
            entries = List.copyOf(entries);
        }

        /**
         * Backward-compatible constructor: {@code heartbeatSeq} defaults to 0.
         * Used by transport callers that haven't been wired up for read-index
         * barrier propagation yet. With seq=0, responses to the resulting AE
         * will never satisfy a read-index barrier, so ClientRead times out
         * rather than being served on stale evidence.
         */
        public AppendEntriesReq(
                Term term,
                NodeId leaderId,
                long prevLogIndex,
                Term prevLogTerm,
                List<LogEntry> entries,
                long leaderCommit,
                long nowNanos) {
            this(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit, nowNanos, 0L);
        }
    }

    record AppendEntriesResp(
            NodeId from,
            Term term,
            boolean success,
            long conflictIndex,
            Term conflictTerm,
            long matchIndex,
            long heartbeatSeq)
            implements RaftEvent {
        public AppendEntriesResp {
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(term, "term");
            Objects.requireNonNull(conflictTerm, "conflictTerm");
        }

        /** Backward-compatible constructor: {@code heartbeatSeq} defaults to 0. */
        public AppendEntriesResp(
                NodeId from, Term term, boolean success, long conflictIndex, Term conflictTerm, long matchIndex) {
            this(from, term, success, conflictIndex, conflictTerm, matchIndex, 0L);
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

    /**
     * Admin-initiated single-server membership change: replace the active
     * membership (voters + learners) with {@code membership}. Takes effect on
     * append (Raft §4.2). Rejected by the leader if another config change is
     * still in flight. Learners receive replication but don't count toward
     * quorum until a later change promotes them into the voter set.
     */
    record ProposeConfigChange(Membership membership) implements RaftEvent {
        public ProposeConfigChange {
            Objects.requireNonNull(membership, "membership");
            if (membership.voters().isEmpty()) {
                throw new IllegalArgumentException("voters must be non-empty");
            }
        }

        /** Voter-only change (no learners) — the historical shape. */
        public ProposeConfigChange(List<NodeId> newVoters) {
            this(Membership.ofVoters(newVoters));
        }
    }

    /** Leader → lagging follower: install a full state-machine snapshot. */
    record InstallSnapshotReq(
            Term term, NodeId leaderId, long lastIncludedIndex, Term lastIncludedTerm, byte[] snapshot, long nowNanos)
            implements RaftEvent {
        public InstallSnapshotReq {
            Objects.requireNonNull(term, "term");
            Objects.requireNonNull(leaderId, "leaderId");
            Objects.requireNonNull(lastIncludedTerm, "lastIncludedTerm");
            Objects.requireNonNull(snapshot, "snapshot");
        }
    }

    record InstallSnapshotResp(NodeId from, Term term) implements RaftEvent {
        public InstallSnapshotResp {
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(term, "term");
        }
    }

    /**
     * Admin-/driver-initiated: take a snapshot of the state machine at the
     * current {@code commitIndex}. The core stores {@code bytes} for
     * subsequent {@link #SendInstallSnapshot} effects and compacts the log
     * prefix up to {@code commitIndex}.
     */
    record TakeSnapshot(byte[] bytes) implements RaftEvent {
        public TakeSnapshot {
            Objects.requireNonNull(bytes, "bytes");
        }
    }
}
