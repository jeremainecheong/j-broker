package jbroker.sim;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import jbroker.raft.core.LogEntry;
import jbroker.raft.core.RaftLog;
import jbroker.raft.core.Term;

/**
 * In-memory {@link RaftLog} for the deterministic simulator. Mirrors
 * {@code FileRaftLog}'s semantics — 1-based indices, {@code truncatePrefix}
 * drops the prefix and tracks {@code lastIncludedIndex/Term} — but without
 * any file I/O.
 */
public final class InMemoryRaftLog implements RaftLog {

    private final List<LogEntry> entries = new ArrayList<>();
    private long lastIncludedIndex;
    private Term lastIncludedTerm = Term.ZERO;

    @Override
    public long lastIndex() {
        return entries.isEmpty()
                ? lastIncludedIndex
                : entries.get(entries.size() - 1).index();
    }

    @Override
    public long firstIndex() {
        return lastIncludedIndex + 1;
    }

    @Override
    public long lastIncludedIndex() {
        return lastIncludedIndex;
    }

    @Override
    public Term lastIncludedTerm() {
        return lastIncludedTerm;
    }

    @Override
    public Optional<Term> termAt(long idx) {
        if (idx == lastIncludedIndex && lastIncludedIndex > 0) {
            return Optional.of(lastIncludedTerm);
        }
        if (idx < firstIndex() || idx > lastIndex()) {
            return Optional.empty();
        }
        return Optional.of(entries.get((int) (idx - firstIndex())).term());
    }

    @Override
    public void append(List<LogEntry> toAppend) {
        Objects.requireNonNull(toAppend);
        long expected = lastIndex() + 1;
        for (var e : toAppend) {
            if (e.index() != expected) {
                throw new IllegalArgumentException("non-contiguous append: expected " + expected + " got " + e.index());
            }
            entries.add(e);
            expected++;
        }
    }

    @Override
    public List<LogEntry> read(long fromIndex, int maxEntries) {
        if (fromIndex < firstIndex() || fromIndex > lastIndex()) {
            return List.of();
        }
        int start = (int) (fromIndex - firstIndex());
        int end = Math.min(entries.size(), start + maxEntries);
        return List.copyOf(entries.subList(start, end));
    }

    @Override
    public void truncateFrom(long idx) {
        if (idx < firstIndex() || idx > lastIndex()) {
            return;
        }
        int listPos = (int) (idx - firstIndex());
        while (entries.size() > listPos) {
            entries.remove(entries.size() - 1);
        }
    }

    @Override
    public void truncatePrefix(long firstIndex, Term firstTerm) {
        if (firstIndex <= firstIndex()) {
            return;
        }
        this.lastIncludedIndex = firstIndex - 1;
        this.lastIncludedTerm = Objects.requireNonNull(firstTerm);
        entries.removeIf(e -> e.index() < firstIndex);
    }
}
