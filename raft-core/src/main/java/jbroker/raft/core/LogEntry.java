package jbroker.raft.core;

import java.util.Objects;

public record LogEntry(long index, Term term, Type type, byte[] payload) {

    public enum Type {
        NORMAL,
        NO_OP,
        CONFIG_CHANGE,
    }

    public LogEntry {
        if (index < 0) {
            throw new IllegalArgumentException("index must be non-negative: " + index);
        }
        Objects.requireNonNull(term, "term");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(payload, "payload");
    }
}
