package jbroker.raft.core;

import java.util.List;
import java.util.Optional;

/**
 * Append-only Raft log. Implementations are responsible for durability;
 * {@link #append} must not return until the entries are fsync'd.
 */
public interface RaftLog {

    /**
     * Returns the index of the last entry, or 0 if the log is empty.
     * Raft uses 1-based indices.
     */
    long lastIndex();

    /**
     * Returns the term of the entry at {@code index}, or empty if the index
     * is out of range.
     */
    Optional<Term> termAt(long index);

    /**
     * Appends the given entries after the current last index. Indices must
     * form a contiguous range starting at {@code lastIndex() + 1}.
     */
    void append(List<LogEntry> entries);

    /**
     * Returns up to {@code maxEntries} entries starting at {@code fromIndex}
     * (inclusive). Returns empty list if {@code fromIndex} > lastIndex.
     */
    List<LogEntry> read(long fromIndex, int maxEntries);

    /**
     * Truncates all entries at {@code index} and beyond. No-op if
     * {@code index} > lastIndex.
     */
    void truncateFrom(long index);
}
