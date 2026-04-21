package jbroker.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

/**
 * Sparse (timestamp, relative-offset) index. Entries are
 * {@code (timestamp: int64, relativeOffset: int32)} = 12 bytes. Binary
 * search returns the greatest entry whose timestamp {@code ≤} the query.
 */
public final class TimeIndex implements AutoCloseable {

    public static final int ENTRY_BYTES = 12;

    private final FileChannel channel;
    private final long baseOffset;
    private int entryCount;

    private TimeIndex(FileChannel channel, long baseOffset, int entryCount) {
        this.channel = channel;
        this.baseOffset = baseOffset;
        this.entryCount = entryCount;
    }

    public static TimeIndex open(Path path, long baseOffset) throws IOException {
        var channel =
                FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        long size = channel.size();
        long aligned = (size / ENTRY_BYTES) * ENTRY_BYTES;
        if (aligned != size) channel.truncate(aligned);
        return new TimeIndex(channel, baseOffset, (int) (aligned / ENTRY_BYTES));
    }

    public synchronized void append(long timestamp, long offset) throws IOException {
        long rel = offset - baseOffset;
        if (rel < 0 || rel > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("relative offset out of int32 range: " + rel);
        }
        var buf = ByteBuffer.allocate(ENTRY_BYTES).order(ByteOrder.BIG_ENDIAN);
        buf.putLong(timestamp);
        buf.putInt((int) rel);
        buf.flip();
        channel.write(buf, (long) entryCount * ENTRY_BYTES);
        entryCount++;
    }

    /**
     * Return the largest offset whose timestamp is {@code ≤ targetTimestamp}.
     * Empty if the index is empty or every timestamp is larger.
     */
    public synchronized Optional<Long> lookupOffset(long targetTimestamp) throws IOException {
        if (entryCount == 0) return Optional.empty();
        int lo = 0, hi = entryCount - 1;
        int found = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            long ts = tsAt(mid);
            if (ts <= targetTimestamp) {
                found = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        if (found < 0) return Optional.empty();
        return Optional.of(baseOffset + relAt(found));
    }

    public synchronized int size() {
        return entryCount;
    }

    public synchronized void force() throws IOException {
        channel.force(true);
    }

    public synchronized void truncate(int newCount) throws IOException {
        if (newCount < 0 || newCount > entryCount) return;
        channel.truncate((long) newCount * ENTRY_BYTES);
        entryCount = newCount;
    }

    @Override
    public synchronized void close() throws IOException {
        channel.close();
    }

    private long tsAt(int i) throws IOException {
        var buf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN);
        channel.read(buf, (long) i * ENTRY_BYTES);
        buf.flip();
        return buf.getLong();
    }

    private int relAt(int i) throws IOException {
        var buf = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN);
        channel.read(buf, (long) i * ENTRY_BYTES + Long.BYTES);
        buf.flip();
        return buf.getInt();
    }
}
