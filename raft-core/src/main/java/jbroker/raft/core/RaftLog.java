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

    /**
     * Lowest index still fully stored in the log. Returns {@code 1} on a log
     * that has never been compacted; after {@link #truncatePrefix} this is
     * the first surviving entry.
     */
    default long firstIndex() {
        return 1L;
    }

    /**
     * Index of the last entry included in the most recent snapshot, or
     * {@code 0} if no snapshot has been taken. Equal to {@code firstIndex - 1}
     * when the log has been compacted.
     */
    default long lastIncludedIndex() {
        return 0L;
    }

    /**
     * Term of the entry at {@link #lastIncludedIndex()}. Needed as the
     * {@code prevLogTerm} when the leader sends AE for an entry whose
     * {@code prevLogIndex} equals the compaction point.
     */
    default Term lastIncludedTerm() {
        return Term.ZERO;
    }

    /**
     * Drop every entry strictly before {@code firstIndex}, recording
     * {@code firstTerm} as the term of the last included entry. No-op if
     * {@code firstIndex <= 1}. After this call, {@link #firstIndex()} is
     * {@code firstIndex}, {@link #lastIncludedIndex()} is {@code firstIndex - 1},
     * and {@link #lastIncludedTerm()} is {@code firstTerm}.
     */
    default void truncatePrefix(long firstIndex, Term firstTerm) {
        throw new UnsupportedOperationException("log does not support prefix truncation");
    }
}
