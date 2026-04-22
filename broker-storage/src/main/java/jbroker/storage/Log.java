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
    public synchronized long appendRaw(byte[] encodedBatch, long expectedBaseOffset) throws IOException {
        var active = segments.get(segments.size() - 1);
        active.appendRaw(encodedBatch, expectedBaseOffset);
        long assignedLast = active.nextOffset() - 1;
        if (active.sizeBytes() >= config.segmentBytes()) {
            var next = LogSegment.open(dir, active.nextOffset(), config.indexIntervalBytes());
            active.force();
            segments.add(next);
        }
        return assignedLast;
    }

    public synchronized long append(List<Record> records, long nowMillis) throws IOException {
        var active = segments.get(segments.size() - 1);
        long firstTimestamp = nowMillis;
        long maxTimestamp = nowMillis;
        long baseOffset = active.append(firstTimestamp, maxTimestamp, records);
        // Convert position-based return into last-assigned-offset for caller
        // convenience:
        long assignedFirst = active.nextOffset() - records.size();
        long assignedLast = active.nextOffset() - 1;
        // Rollover if we've crossed the size threshold.
        if (active.sizeBytes() >= config.segmentBytes()) {
            var next = LogSegment.open(dir, active.nextOffset(), config.indexIntervalBytes());
            active.force();
            segments.add(next);
        }
        return assignedLast;
    }

    public long nextOffset() {
        var active = segments.get(segments.size() - 1);
        return active.nextOffset();
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
    public synchronized void truncateTo(long targetOffset) throws IOException {
        if (targetOffset >= nextOffset()) return;
        while (segments.size() > 1 && segments.get(segments.size() - 1).baseOffset() >= targetOffset) {
            var tail = segments.remove(segments.size() - 1);
            tail.close();
            tail.delete();
        }
        var active = segments.get(segments.size() - 1);
        active.truncateAtOrAbove(targetOffset);
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
    public synchronized int retain(long cutoffMillis) throws IOException {
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
    }

    public List<LogSegment> segments() {
        return List.copyOf(segments);
    }

    public synchronized void force() throws IOException {
        for (var seg : segments) seg.force();
    }

    @Override
    public synchronized void close() throws IOException {
        for (var seg : segments) seg.close();
    }
}
