package jbroker.raft.core;

import java.util.List;
import java.util.Objects;

/**
 * Static configuration for a {@link RaftCore} instance.
 */
public record RaftConfig(
        NodeId selfId,
        List<NodeId> voters,
        long electionTimeoutNanos,
        long electionJitterNanos,
        long heartbeatIntervalNanos,
        int maxEntriesPerAppend) {

    public RaftConfig {
        Objects.requireNonNull(selfId, "selfId");
        voters = List.copyOf(voters);
        if (voters.isEmpty()) {
            throw new IllegalArgumentException("voters must be non-empty");
        }
        // selfId need NOT be a voter: a node may boot as a non-voting
        // learner of an existing cluster (voters = the incumbent set,
        // itself absent), catch its log up, and be promoted into the voter
        // set by a later CONFIG_CHANGE. Such a node never campaigns — the
        // election gate keys on membership, not on this bootstrap list.
        if (electionTimeoutNanos <= 0 || heartbeatIntervalNanos <= 0) {
            throw new IllegalArgumentException("timeouts must be positive");
        }
        if (electionJitterNanos < 0) {
            throw new IllegalArgumentException("electionJitterNanos must be non-negative");
        }
        if (heartbeatIntervalNanos >= electionTimeoutNanos) {
            throw new IllegalArgumentException("heartbeat must be less than election timeout");
        }
        if (maxEntriesPerAppend <= 0) {
            throw new IllegalArgumentException("maxEntriesPerAppend must be positive");
        }
    }

    public int quorum() {
        return voters.size() / 2 + 1;
    }
}
