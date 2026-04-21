package jbroker.raft.core;

import java.util.Arrays;
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
        payload = payload.clone();
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LogEntry other)) return false;
        return index == other.index
                && term.equals(other.term)
                && type == other.type
                && Arrays.equals(payload, other.payload);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(index);
        result = 31 * result + term.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }

    @Override
    public String toString() {
        return "LogEntry[index=" + index
                + ", term=" + term
                + ", type=" + type
                + ", payload=" + Arrays.toString(payload)
                + ']';
    }
}
