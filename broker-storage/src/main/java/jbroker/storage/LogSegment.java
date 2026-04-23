package jbroker.storage;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * One {@code .log} segment plus its sparse {@code .index} and
 * {@code .timeindex} sidecars. Append-only; reads return raw record-batch
 * bytes that callers can decode via {@link RecordBatch#decode(ByteBuffer)}.
 *
 * <p>Filenames: {@code <20-digit-baseOffset>.log}, {@code .index},
 * {@code .timeindex} — Kafka-style, zero-padded for lexicographic ordering.
 */
public final class LogSegment implements AutoCloseable {

    public static final int DEFAULT_INDEX_INTERVAL_BYTES = 4 * 1024;

    /**
     * P10.2 — serialise all IO-touching methods via a ReentrantLock
     * rather than an intrinsic monitor. Blocking FileChannel ops held
     * under `synchronized` pin a virtual thread's carrier OS thread;
     * ReentrantLock does not.
     */
    private final java.util.concurrent.locks.ReentrantLock lock = new java.util.concurrent.locks.ReentrantLock();

    private final long baseOffset;
    private final Path logPath;
    private final FileChannel logChannel;
    private final OffsetIndex offsetIndex;
    private final TimeIndex timeIndex;
    private final int indexIntervalBytes;

    private long nextOffset; // absolute next offset to assign
    private int bytesSinceLastIndex;
    private long maxTimestamp;

    private LogSegment(
            long baseOffset,
            Path logPath,
            FileChannel logChannel,
            OffsetIndex offsetIndex,
            TimeIndex timeIndex,
            int indexIntervalBytes,
            long nextOffset,
            long maxTimestamp) {
        this.baseOffset = baseOffset;
        this.logPath = logPath;
        this.logChannel = logChannel;
        this.offsetIndex = offsetIndex;
        this.timeIndex = timeIndex;
        this.indexIntervalBytes = indexIntervalBytes;
        this.nextOffset = nextOffset;
        this.maxTimestamp = maxTimestamp;
    }

    public static LogSegment open(Path dir, long baseOffset) throws IOException {
        return open(dir, baseOffset, DEFAULT_INDEX_INTERVAL_BYTES);
    }

    public static LogSegment open(Path dir, long baseOffset, int indexIntervalBytes) throws IOException {
        Files.createDirectories(dir);
        String name = filenameBase(baseOffset);
        var logPath = dir.resolve(name + ".log");
        var idxPath = dir.resolve(name + ".index");
        var timeIdxPath = dir.resolve(name + ".timeindex");

        var logChannel =
                FileChannel.open(logPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        var offsetIndex = OffsetIndex.open(idxPath, baseOffset);
        var timeIndex = TimeIndex.open(timeIdxPath, baseOffset);

        // Compute nextOffset and maxTimestamp by scanning forward — cheap for
        // the active segment (only called on restart); closed segments are
        // treated as immutable after one scan.
        long nextOff = baseOffset;
        long maxTs = -1L;
        long size = logChannel.size();
        long pos = 0;
        while (pos < size) {
            int toMap = (int) Math.min(size - pos, Integer.MAX_VALUE);
            var mapped = logChannel.map(FileChannel.MapMode.READ_ONLY, pos, toMap);
            while (mapped.remaining() >= RecordBatch.BATCH_OVERHEAD) {
                int markPos = mapped.position();
                try {
                    var parsed = RecordBatch.decode(mapped);
                    nextOff = parsed.lastOffset() + 1;
                    if (parsed.maxTimestamp() > maxTs) maxTs = parsed.maxTimestamp();
                } catch (IllegalArgumentException e) {
                    // Torn / corrupt tail — truncate here.
                    logChannel.truncate(pos + markPos);
                    mapped.limit(markPos);
                    break;
                }
            }
            pos += mapped.position();
            if (mapped.remaining() < RecordBatch.BATCH_OVERHEAD && pos < size) {
                // Trailing partial frame — truncate.
                logChannel.truncate(pos);
                break;
            }
            if (!mapped.hasRemaining()) {
                // Consumed everything in this map.
                continue;
            }
            break;
        }

        return new LogSegment(
                baseOffset, logPath, logChannel, offsetIndex, timeIndex, indexIntervalBytes, nextOff, maxTs);
    }

    public long baseOffset() {
        return baseOffset;
    }

    public long nextOffset() {
        return nextOffset;
    }

    public long sizeBytes() throws IOException {
        lock.lock();
        try {
            return logChannel.size();
        } finally {
            lock.unlock();
        }
    }

    public long maxTimestamp() {
        return maxTimestamp;
    }

    public Path logPath() {
        return logPath;
    }

    /**
     * Append a pre-encoded batch byte-for-byte. The batch's {@code baseOffset}
     * must equal this segment's {@link #nextOffset()} — used by the follower
     * replication path (P6.4) to preserve the leader's baseSequence,
     * producerId, producerEpoch, partitionLeaderEpoch, and timestamps.
     *
     * <p>Verifies the batch's header matches {@code expectedBaseOffset} and
     * throws {@link IllegalArgumentException} otherwise.
     */
    public int appendRaw(byte[] encodedBatch, long expectedBaseOffset) throws IOException {
        lock.lock();
        try {
            return appendRawLocked(encodedBatch, expectedBaseOffset);
        } finally {
            lock.unlock();
        }
    }

    private int appendRawLocked(byte[] encodedBatch, long expectedBaseOffset) throws IOException {
        if (expectedBaseOffset != nextOffset) {
            throw new IllegalArgumentException(
                    "expectedBaseOffset " + expectedBaseOffset + " != segment nextOffset " + nextOffset);
        }
        var view = ByteBuffer.wrap(encodedBatch);
        long batchBaseOffset = view.getLong();
        if (batchBaseOffset != expectedBaseOffset) {
            throw new IllegalArgumentException(
                    "batch baseOffset " + batchBaseOffset + " != expectedBaseOffset " + expectedBaseOffset);
        }
        // Peek the header fields we need for the indexes.
        // Layout: baseOffset(8) batchLength(4) partitionLeaderEpoch(4) magic(1)
        //         crc(4) attributes(2) lastOffsetDelta(4) firstTimestamp(8) maxTimestamp(8) ...
        view.position(8 + 4 + 4 + 1 + 4 + 2); // skip to lastOffsetDelta
        int lastOffsetDelta = view.getInt();
        view.getLong(); // firstTimestamp (unused here)
        long maxTs = view.getLong();

        int pos = (int) logChannel.size();
        var toWrite = ByteBuffer.wrap(encodedBatch);
        while (toWrite.hasRemaining()) {
            logChannel.write(toWrite, pos + (encodedBatch.length - toWrite.remaining()));
        }

        if (bytesSinceLastIndex >= indexIntervalBytes || offsetIndex.size() == 0) {
            offsetIndex.append(expectedBaseOffset, pos);
            timeIndex.append(maxTs, expectedBaseOffset);
            bytesSinceLastIndex = 0;
        }
        bytesSinceLastIndex += encodedBatch.length;
        nextOffset = expectedBaseOffset + lastOffsetDelta + 1;
        if (maxTs > this.maxTimestamp) this.maxTimestamp = maxTs;
        return pos;
    }

    /**
     * Append a single batch. The batch's {@code baseOffset} is expected to be
     * this segment's {@link #nextOffset()}. Returns the position at which the
     * batch was written.
     */
    public int append(long firstTimestamp, long maxTimestamp, List<Record> records) throws IOException {
        lock.lock();
        try {
            return appendLocked(firstTimestamp, maxTimestamp, records);
        } finally {
            lock.unlock();
        }
    }

    private int appendLocked(long firstTimestamp, long maxTimestamp, List<Record> records) throws IOException {
        if (records.isEmpty()) throw new IllegalArgumentException("records must be non-empty");
        long baseOff = nextOffset;
        int size = RecordBatch.estimatedSize(records);
        var buf = ByteBuffer.allocate(size);
        int written = RecordBatch.encode(buf, baseOff, 0, firstTimestamp, maxTimestamp, -1L, (short) -1, -1, records);
        buf.flip();
        int pos = (int) logChannel.size();
        while (buf.hasRemaining()) {
            logChannel.write(buf, pos + (written - buf.remaining()));
        }

        if (bytesSinceLastIndex >= indexIntervalBytes || offsetIndex.size() == 0) {
            offsetIndex.append(baseOff, pos);
            timeIndex.append(maxTimestamp, baseOff);
            bytesSinceLastIndex = 0;
        }
        bytesSinceLastIndex += written;
        int lastDelta = records.get(records.size() - 1).offsetDelta();
        nextOffset = baseOff + lastDelta + 1;
        if (maxTimestamp > this.maxTimestamp) this.maxTimestamp = maxTimestamp;
        return pos;
    }

    /**
     * Read all batches starting at or after {@code startOffset}. Returns raw
     * bytes; the caller decodes via {@link RecordBatch#decode(ByteBuffer)}.
     * {@code maxBytes} bounds the total returned length.
     */
    public List<RecordBatch.Parsed> readFrom(long startOffset, int maxBytes) throws IOException {
        lock.lock();
        try {
            return readFromLocked(startOffset, maxBytes);
        } finally {
            lock.unlock();
        }
    }

    private List<RecordBatch.Parsed> readFromLocked(long startOffset, int maxBytes) throws IOException {
        var entry = offsetIndex.lookup(startOffset);
        int fromPos = entry.map(e -> e.position()).orElse(0);
        long size = logChannel.size();
        if (fromPos >= size) return List.of();
        int toRead = (int) Math.min(size - fromPos, maxBytes);
        var buf = ByteBuffer.allocate(toRead);
        logChannel.read(buf, fromPos);
        buf.flip();

        var out = new ArrayList<RecordBatch.Parsed>();
        while (buf.remaining() >= RecordBatch.BATCH_OVERHEAD) {
            int mark = buf.position();
            try {
                var parsed = RecordBatch.decode(buf);
                if (parsed.lastOffset() >= startOffset) {
                    out.add(parsed);
                }
            } catch (IllegalArgumentException e) {
                buf.position(mark);
                break;
            }
        }
        return out;
    }

    /**
     * Zero-copy read: transfer {@code maxBytes} of raw log bytes starting at
     * the index position for {@code startOffset} directly to {@code out}.
     */
    public long transferTo(long startOffset, int maxBytes, OutputStream out) throws IOException {
        lock.lock();
        try {
            var entry = offsetIndex.lookup(startOffset);
            long fromPos = entry.map(e -> (long) e.position()).orElse(0L);
            long size = logChannel.size();
            if (fromPos >= size) return 0;
            long toRead = Math.min(size - fromPos, maxBytes);
            return logChannel.transferTo(fromPos, toRead, Channels.newChannel(out));
        } finally {
            lock.unlock();
        }
    }

    public void force() throws IOException {
        lock.lock();
        try {
            forceLocked();
        } finally {
            lock.unlock();
        }
    }

    private void forceLocked() throws IOException {
        var event = new jbroker.storage.jfr.FsyncDurationEvent();
        if (event.shouldCommit()) {
            long before = logChannel.size();
            long startNanos = System.nanoTime();
            logChannel.force(true);
            offsetIndex.force();
            timeIndex.force();
            event.baseOffset = baseOffset;
            event.sizeBytes = before;
            event.durationNanos = System.nanoTime() - startNanos;
            event.commit();
        } else {
            logChannel.force(true);
            offsetIndex.force();
            timeIndex.force();
        }
    }

    /**
     * Truncate this segment so the resulting {@link #nextOffset()} equals
     * {@code targetOffset}. Scans batches and cuts at the first batch whose
     * {@code baseOffset >= targetOffset}. If no such batch exists, this is
     * a no-op. If {@code targetOffset <= baseOffset}, the segment is
     * emptied.
     */
    public synchronized void truncateAtOrAbove(long targetOffset) throws IOException {
        if (targetOffset <= baseOffset) {
            logChannel.truncate(0);
            offsetIndex.truncateAll();
            timeIndex.truncateAll();
            nextOffset = baseOffset;
            bytesSinceLastIndex = 0;
            maxTimestamp = 0L;
            return;
        }
        if (targetOffset >= nextOffset) return;
        long size = logChannel.size();
        if (size == 0) return;
        var buf = ByteBuffer.allocate((int) size);
        int read = 0;
        while (read < size) {
            int n = logChannel.read(buf, read);
            if (n <= 0) break;
            read += n;
        }
        buf.flip();

        int cutPos = 0;
        long newNextOffset = baseOffset;
        long newMaxTs = 0L;
        while (buf.remaining() >= RecordBatch.BATCH_OVERHEAD) {
            int startPos = buf.position();
            RecordBatch.Parsed parsed;
            try {
                parsed = RecordBatch.decode(buf);
            } catch (IllegalArgumentException e) {
                break;
            }
            if (parsed.baseOffset() >= targetOffset) {
                break;
            }
            cutPos = buf.position();
            newNextOffset = parsed.baseOffset() + parsed.lastOffsetDelta() + 1;
            if (parsed.maxTimestamp() > newMaxTs) newMaxTs = parsed.maxTimestamp();
            if (cutPos - startPos <= 0) break; // defensive
        }
        logChannel.truncate(cutPos);
        offsetIndex.truncateAtOrAbove(targetOffset);
        timeIndex.truncateAtOrAbove(targetOffset);
        nextOffset = newNextOffset;
        maxTimestamp = newMaxTs;
        bytesSinceLastIndex = 0;
    }

    @Override
    public synchronized void close() throws IOException {
        // P9.2 — fsync on close so a clean shutdown durably persists the tail
        // of the active segment. Previously the active segment relied on
        // the OS page cache to flush post-process-exit, which is unsafe for
        // kill -9. FsyncDurationEvent also fires here, which is what the
        // E2E-9-2 teardown observes.
        try {
            force();
        } finally {
            logChannel.close();
            offsetIndex.close();
            timeIndex.close();
        }
    }

    /** Delete the segment's three files. Must be called after {@link #close}. */
    public void delete() throws IOException {
        String name = filenameBase(baseOffset);
        Path dir = logPath.getParent();
        Files.deleteIfExists(dir.resolve(name + ".log"));
        Files.deleteIfExists(dir.resolve(name + ".index"));
        Files.deleteIfExists(dir.resolve(name + ".timeindex"));
    }

    public static String filenameBase(long baseOffset) {
        return String.format("%020d", baseOffset);
    }

    public OffsetIndex offsetIndex() {
        return offsetIndex;
    }

    public TimeIndex timeIndex() {
        return timeIndex;
    }
}
