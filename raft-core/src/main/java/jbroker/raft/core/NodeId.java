package jbroker.raft.core;

public record NodeId(int value) {
    public NodeId {
        if (value < 0) {
            throw new IllegalArgumentException("node id must be non-negative: " + value);
        }
    }
}
