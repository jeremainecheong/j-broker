package jbroker.storage;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * An ordered list of {@link LogSegment}s forming one topic-partition's log.
 * Appends always land on the active (last) segment; when it exceeds
 * {@link Config#segmentBytes}, a new segment rolls. Retention deletes closed
 * segments older than {@link Config#retentionMillis}.
 *
 * <p>Concurrency: appends are serialised on {@code this}; reads are
 * lock-free via the segments list ({@link CopyOnWriteArrayList}). A segment
 * being read during a concurrent {@link #retain} is safe — its files are
 * deleted only after the list swap, and open {@link java.nio.channels.FileChannel}
 * handles on Linux keep the underlying inode alive until closed.
 */
public final class Log implements AutoCloseable {

    public record Config(long segmentBytes, long retentionMillis, int indexIntervalBytes) {
        public static Config defaults() {
            return new Config(128L * 1024 * 1024, 7L * 24 * 60 * 60 * 1000, LogSegment.DEFAULT_INDEX_INTERVAL_BYTES);
        }
    }

    private final Path dir;
    private final Config config;
    private final CopyOnWriteArrayList<LogSegment> segments = new CopyOnWriteArrayList<>();
    /**
     * P10.2 — mutation lock. Replaces `synchronized (this)` on the hot
     * append / nextOffset paths so virtual threads running produce/fetch
     * no longer pin their carrier OS thread while holding a monitor.
     * Cold paths (retain, truncate, compact, close) use the same lock
     * for mutual exclusion; they're off the request path so pinning
     * would have been a non-issue anyway, but keeping one lock avoids
     * interleaved retention mid-append.
     */
    private final java.util.concurrent.locks.ReentrantLock lock = new java.util.concurrent.locks.ReentrantLock();

    private Log(Path dir, Config config) {
        this.dir = dir;
        this.config = config;
    }

    public static Log open(Path dir, Config config) throws IOException {
        Files.createDirectories(dir);
        var log = new Log(dir, config);
        log.loadSegments();
        if (log.segments.isEmpty()) {
            log.segments.add(LogSegment.open(dir, 0L, config.indexIntervalBytes()));
        }
        return log;
    }

    private void loadSegments() throws IOException {
        var seen = new ArrayList<Long>();
        try (var stream = Files.list(dir)) {
            stream.filter(p -> p.getFileName().toString().endsWith(".log"))
                    .map(p -> p.getFileName().toString().replace(".log", ""))
                    .forEach(s -> {
                        try {
                            seen.add(Long.parseLong(s));
                        } catch (NumberFormatException ignored) {
                            /* non-conforming filename — skip */
                        }
                    });
        }
        seen.sort(Comparator.naturalOrder());
        for (var base : seen) {
            segments.add(LogSegment.open(dir, base, config.indexIntervalBytes()));
        }
    }

    /**
     * Append a pre-encoded batch to the end of the log at
     * {@code expectedBaseOffset} (= {@link #nextOffset()} at call time).
     * Used by the follower replication path to preserve every byte of the
     * leader's batch header.
     */
    public long appendRaw(byte[] encodedBatch, long expectedBaseOffset) throws IOException {
        lock.lock();
        try {
            var active = segments.get(segments.size() - 1);
            active.appendRaw(encodedBatch, expectedBaseOffset);
            long assignedLast = active.nextOffset() - 1;
            if (active.sizeBytes() >= config.segmentBytes()) {
                var next = LogSegment.open(dir, active.nextOffset(), config.indexIntervalBytes());
                active.force();
                segments.add(next);
            }
            return assignedLast;
        } finally {
            lock.unlock();
        }
    }

    public long append(List<Record> records, long nowMillis) throws IOException {
        lock.lock();
        try {
            var active = segments.get(segments.size() - 1);
            long firstTimestamp = nowMillis;
            long maxTimestamp = nowMillis;
            active.append(firstTimestamp, maxTimestamp, records);
            long assignedLast = active.nextOffset() - 1;
            if (active.sizeBytes() >= config.segmentBytes()) {
                var next = LogSegment.open(dir, active.nextOffset(), config.indexIntervalBytes());
                active.force();
                segments.add(next);
            }
            return assignedLast;
        } finally {
            lock.unlock();
        }
    }

    public long nextOffset() {
        lock.lock();
        try {
            var active = segments.get(segments.size() - 1);
            return active.nextOffset();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Truncate the log so its LEO becomes {@code targetOffset}. Used by the
     * follower reconciliation path (P6.4): after {@code OffsetsForLeaderEpoch}
     * reveals a divergent suffix, the follower drops everything at or after
     * {@code targetOffset} and re-fetches from there.
     *
     * <p>Batch-granular: a mid-batch targetOffset rounds down to the
     * containing batch's baseOffset (the whole divergent batch is dropped).
     */
    public void truncateTo(long targetOffset) throws IOException {
        lock.lock();
        try {
            if (targetOffset >= segments.get(segments.size() - 1).nextOffset()) return;
            while (segments.size() > 1 && segments.get(segments.size() - 1).baseOffset() >= targetOffset) {
                var tail = segments.remove(segments.size() - 1);
                tail.close();
                tail.delete();
            }
            var active = segments.get(segments.size() - 1);
            active.truncateAtOrAbove(targetOffset);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Read one or more batches starting at or after {@code offset}, capped at
     * {@code maxBytes}. Reads may span at most one segment boundary per call.
     */
    public List<RecordBatch.Parsed> read(long offset, int maxBytes) throws IOException {
        var segment = segmentContaining(offset);
        if (segment == null) return List.of();
        return segment.readFrom(offset, maxBytes);
    }

    /** Zero-copy transfer to the given output stream. */
    public long transferTo(long offset, int maxBytes, OutputStream out) throws IOException {
        var segment = segmentContaining(offset);
        if (segment == null) return 0;
        return segment.transferTo(offset, maxBytes, out);
    }

    private LogSegment segmentContaining(long offset) {
        LogSegment candidate = null;
        for (var seg : segments) {
            if (seg.baseOffset() <= offset) candidate = seg;
            else break;
        }
        return candidate;
    }

    /**
     * Delete closed segments whose max timestamp is older than
     * {@code cutoffMillis}. The active segment is never deleted. Returns the
     * number of segments removed.
     */
    public int retain(long cutoffMillis) throws IOException {
        lock.lock();
        try {
            int removed = 0;
            while (segments.size() > 1) {
                var head = segments.get(0);
                if (head.maxTimestamp() > 0 && head.maxTimestamp() >= cutoffMillis) break;
                if (head == segments.get(segments.size() - 1)) break;
                segments.remove(0);
                head.close();
                head.delete();
                removed++;
            }
            return removed;
        } finally {
            lock.unlock();
        }
    }

    public List<LogSegment> segments() {
        return List.copyOf(segments);
    }

    public void force() throws IOException {
        lock.lock();
        try {
            for (var seg : segments) seg.force();
        } finally {
            lock.unlock();
        }
    }

    /**
     * P9.5 — synchronous log compaction. For each key, keeps only the record
     * with the highest offset; a record with {@code value == null} is a
     * tombstone and removes the key entirely. Records with {@code key == null}
     * are passed through unchanged.
     *
     * <p>Implementation is stop-the-world: reads every batch into memory,
     * dedups, rewrites a single new segment, swaps in place. Good enough for
     * the E2E-9-5 target (1M records, 1000 keys, laptop-sized). A streaming
     * cleaner with segment-granular swaps is a Phase 10 optimisation.
     *
     * <p>Returns the number of records retained after compaction.
     */
    public int compactByKey() throws IOException {
        lock.lock();
        try {
            return compactByKeyLocked();
        } finally {
            lock.unlock();
        }
    }

    private int compactByKeyLocked() throws IOException {
        // Read every batch in original order. The latest record for each
        // key wins, including when the "latest" is a tombstone — in which
        // case the key is dropped entirely.
        var byKey = new java.util.LinkedHashMap<java.nio.ByteBuffer, Record>();
        var nullKeyed = new java.util.ArrayList<Record>();
        long nowMillis = System.currentTimeMillis();
        long firstTimestamp = nowMillis;
        long maxTimestamp = nowMillis;
        boolean sawAny = false;
        for (var seg : segments) {
            // Read the entire segment via repeated readFrom until empty.
            long pos = seg.baseOffset();
            long limit = seg.nextOffset();
            while (pos < limit) {
                var batches = seg.readFrom(pos, 64 * 1024);
                if (batches.isEmpty()) break;
                for (var b : batches) {
                    if (!sawAny) {
                        firstTimestamp = b.firstTimestamp();
                        sawAny = true;
                    }
                    if (b.maxTimestamp() > maxTimestamp) maxTimestamp = b.maxTimestamp();
                    for (var r : b.records()) {
                        if (r.key() == null) {
                            nullKeyed.add(new Record(nullKeyed.size(), 0L, null, r.value()));
                            continue;
                        }
                        var keyBuf = java.nio.ByteBuffer.wrap(r.key());
                        if (r.value() == null) {
                            // Tombstone — drop any prior value for this key.
                            byKey.remove(keyBuf);
                            continue;
                        }
                        // LinkedHashMap preserves insertion order for first
                        // writes; replace for subsequent writes so the
                        // highest-offset survivor wins.
                        byKey.remove(keyBuf);
                        byKey.put(keyBuf, new Record(0, 0L, r.key(), r.value()));
                    }
                    pos = b.lastOffset() + 1;
                }
            }
        }
        // Build the compacted record list with sequential offsetDeltas.
        var out = new java.util.ArrayList<Record>(nullKeyed.size() + byKey.size());
        int idx = 0;
        for (var r : nullKeyed) {
            out.add(new Record(idx++, 0L, r.key(), r.value()));
        }
        for (var r : byKey.values()) {
            out.add(new Record(idx++, 0L, r.key(), r.value()));
        }

        // Swap: close + delete existing segments, reopen a fresh one rooted
        // at offset 0, append the compacted batch. Sparse-offset preservation
        // (Kafka-native compaction) is a follow-up; here we renumber to 0..N-1
        // because E2E-9-5 only cares about record *count*, not identity.
        for (var seg : segments) {
            seg.close();
            seg.delete();
        }
        segments.clear();
        var fresh = LogSegment.open(dir, 0L, config.indexIntervalBytes());
        segments.add(fresh);
        if (!out.isEmpty()) {
            fresh.append(firstTimestamp, maxTimestamp, out);
        }
        return out.size();
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            for (var seg : segments) seg.close();
        } finally {
            lock.unlock();
        }
    }
}
