package jbroker.admin.dto;

public record RaftNodeState(
        int nodeId,
        String role,
        long currentTerm,
        long commitIndex,
        long lastApplied,
        int votedFor,
        long lastLogIndex,
        long lastLogTerm,
        String status) {

    public static RaftNodeState ok(
            int nodeId,
            String role,
            long currentTerm,
            long commitIndex,
            long lastApplied,
            int votedFor,
            long lastLogIndex,
            long lastLogTerm) {
        return new RaftNodeState(
                nodeId, role, currentTerm, commitIndex, lastApplied, votedFor, lastLogIndex, lastLogTerm, "REACHABLE");
    }

    public static RaftNodeState unreachable(String address) {
        return new RaftNodeState(-1, "UNKNOWN", 0, 0, 0, -1, 0, 0, "UNREACHABLE:" + address);
    }
}
