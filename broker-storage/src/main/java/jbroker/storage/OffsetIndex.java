package jbroker.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

/**
 * Sparse offset→physical-position index. Entries are fixed-size
 * {@code (relativeOffset: int32, position: int32)}. Binary-searchable;
 * returns the greatest entry whose offset {@code ≤} the query (the usual
 * "floor" semantics).
 *
 * <p>Indexing is sparse: callers append every {@code indexIntervalBytes}.
 * Relative offsets let the backing file stay {@code int32}-sized even for
 * large segments.
 */
public final class OffsetIndex implements AutoCloseable {

    public static final int ENTRY_BYTES = 8;

    private final FileChannel channel;
    private final long baseOffset;
    private int entryCount;

    private OffsetIndex(FileChannel channel, long baseOffset, int entryCount) {
        this.channel = channel;
        this.baseOffset = baseOffset;
        this.entryCount = entryCount;
    }

    public static OffsetIndex open(Path path, long baseOffset) throws IOException {
        var channel =
                FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        long size = channel.size();
        long aligned = (size / ENTRY_BYTES) * ENTRY_BYTES;
        if (aligned != size) channel.truncate(aligned);
        return new OffsetIndex(channel, baseOffset, (int) (aligned / ENTRY_BYTES));
    }

    public synchronized void append(long offset, int position) throws IOException {
        long rel = offset - baseOffset;
        if (rel < 0 || rel > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("relative offset out of int32 range: " + rel);
        }
        var buf = ByteBuffer.allocate(ENTRY_BYTES).order(ByteOrder.BIG_ENDIAN);
        buf.putInt((int) rel);
        buf.putInt(position);
        buf.flip();
        channel.write(buf, (long) entryCount * ENTRY_BYTES);
        entryCount++;
    }

    /**
     * Return the greatest ({@code offset, position}) in the index with
     * {@code offset <= target}. Empty if the target is below {@link #baseOffset}.
     */
    public synchronized Optional<Entry> lookup(long target) throws IOException {
        if (entryCount == 0 || target < baseOffset) return Optional.empty();
        int lo = 0, hi = entryCount - 1;
        int found = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            long midOff = baseOffset + relAt(mid);
            if (midOff <= target) {
                found = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        if (found < 0) return Optional.empty();
        return Optional.of(readEntry(found));
    }

    public synchronized int size() {
        return entryCount;
    }

    public synchronized void force() throws IOException {
        channel.force(true);
    }

    private int relAt(int i) throws IOException {
        var buf = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN);
        channel.read(buf, (long) i * ENTRY_BYTES);
        buf.flip();
        return buf.getInt();
    }

    private Entry readEntry(int i) throws IOException {
        var buf = ByteBuffer.allocate(ENTRY_BYTES).order(ByteOrder.BIG_ENDIAN);
        channel.read(buf, (long) i * ENTRY_BYTES);
        buf.flip();
        int rel = buf.getInt();
        int pos = buf.getInt();
        return new Entry(baseOffset + rel, pos);
    }

    public synchronized void truncate(int newCount) throws IOException {
        if (newCount < 0 || newCount > entryCount) return;
        channel.truncate((long) newCount * ENTRY_BYTES);
        entryCount = newCount;
    }

    /** Drop every entry whose offset is {@code >= targetOffset}. */
    public synchronized void truncateAtOrAbove(long targetOffset) throws IOException {
        int keep = 0;
        for (int i = 0; i < entryCount; i++) {
            long off = baseOffset + relAt(i);
            if (off >= targetOffset) break;
            keep = i + 1;
        }
        truncate(keep);
    }

    /** Drop every entry. */
    public synchronized void truncateAll() throws IOException {
        truncate(0);
    }

    @Override
    public synchronized void close() throws IOException {
        channel.close();
    }

    public record Entry(long offset, int position) {}
}
