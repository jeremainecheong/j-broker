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

    public synchronized long sizeBytes() throws IOException {
        return logChannel.size();
    }

    public long maxTimestamp() {
        return maxTimestamp;
    }

    public Path logPath() {
        return logPath;
    }

    /**
     * Append a single batch. The batch's {@code baseOffset} is expected to be
     * this segment's {@link #nextOffset()}. Returns the position at which the
     * batch was written.
     */
    public synchronized int append(long firstTimestamp, long maxTimestamp, List<Record> records) throws IOException {
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
    public synchronized List<RecordBatch.Parsed> readFrom(long startOffset, int maxBytes) throws IOException {
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
    public synchronized long transferTo(long startOffset, int maxBytes, OutputStream out) throws IOException {
        var entry = offsetIndex.lookup(startOffset);
        long fromPos = entry.map(e -> (long) e.position()).orElse(0L);
        long size = logChannel.size();
        if (fromPos >= size) return 0;
        long toRead = Math.min(size - fromPos, maxBytes);
        return logChannel.transferTo(fromPos, toRead, Channels.newChannel(out));
    }

    public synchronized void force() throws IOException {
        logChannel.force(true);
        offsetIndex.force();
        timeIndex.force();
    }

    @Override
    public synchronized void close() throws IOException {
        logChannel.close();
        offsetIndex.close();
        timeIndex.close();
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
