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
        if (!voters.contains(selfId)) {
            throw new IllegalArgumentException("voters must include selfId");
        }
        if (electionTimeoutNanos <= 0 || heartbeatIntervalNanos <= 0) {
            throw new IllegalArgumentException("timeouts must be positive");
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
