package jbroker.raft.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Append-only file-backed Raft log for Phase 1.
 *
 * <p>Frame format per entry:
 *
 * <pre>
 *   int32  length         // size of the remaining entry payload below
 *   int64  index
 *   int64  term
 *   int32  type (ordinal)
 *   int32  payloadLen
 *   byte[] payload
 * </pre>
 *
 * <p>No compression, no CRC — this is a Phase 1 log; the production log
 * format (record batches v2) lands in Phase 4.
 */
public final class FileRaftLog implements RaftLog, AutoCloseable {

    private static final int MAX_FRAME_PAYLOAD_BYTES = 64 * 1024 * 1024; // 64 MiB sanity cap

    private final FileChannel channel;
    private final List<LogEntry> index;

    private FileRaftLog(FileChannel channel, List<LogEntry> index) {
        this.channel = channel;
        this.index = index;
    }

    public static FileRaftLog open(Path path) throws IOException {
        var channel =
                FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        var index = rehydrate(channel);
        return new FileRaftLog(channel, index);
    }

    private static List<LogEntry> rehydrate(FileChannel channel) throws IOException {
        var entries = new ArrayList<LogEntry>();
        channel.position(0);
        long size = channel.size();
        var lenBuf = ByteBuffer.allocate(Integer.BYTES);
        while (channel.position() < size) {
            long frameStart = channel.position();
            lenBuf.clear();
            if (channel.read(lenBuf) < Integer.BYTES) {
                channel.truncate(frameStart);
                break;
            }
            lenBuf.flip();
            int payloadLen = lenBuf.getInt();
            if (payloadLen < 0 || payloadLen > MAX_FRAME_PAYLOAD_BYTES) {
                // Corrupt length prefix — treat as torn frame and stop.
                channel.truncate(frameStart);
                break;
            }
            if (channel.position() + payloadLen > size) {
                // torn frame — truncate at frame start and stop
                channel.truncate(frameStart);
                break;
            }
            var entryBuf = ByteBuffer.allocate(payloadLen);
            int read = channel.read(entryBuf);
            if (read < payloadLen) {
                channel.truncate(frameStart);
                break;
            }
            entryBuf.flip();
            long idx = entryBuf.getLong();
            long term = entryBuf.getLong();
            int typeOrdinal = entryBuf.getInt();
            int plen = entryBuf.getInt();
            var bytes = new byte[plen];
            entryBuf.get(bytes);
            entries.add(new LogEntry(idx, new Term(term), LogEntry.Type.values()[typeOrdinal], bytes));
        }
        return entries;
    }

    @Override
    public synchronized long lastIndex() {
        return index.isEmpty() ? 0L : index.get(index.size() - 1).index();
    }

    @Override
    public synchronized Optional<Term> termAt(long idx) {
        if (idx < 1 || idx > lastIndex()) {
            return Optional.empty();
        }
        return Optional.of(index.get((int) (idx - 1)).term());
    }

    @Override
    public synchronized void append(List<LogEntry> entries) {
        Objects.requireNonNull(entries, "entries");
        long expected = lastIndex() + 1;
        for (var entry : entries) {
            if (entry.index() != expected) {
                throw new IllegalArgumentException(
                        "non-contiguous append: expected index " + expected + " got " + entry.index());
            }
            expected++;
        }
        try {
            channel.position(channel.size());
            for (var entry : entries) {
                writeFrame(entry);
                index.add(entry);
            }
            channel.force(true);
        } catch (IOException e) {
            throw new IllegalStateException("failed to append", e);
        }
    }

    private void writeFrame(LogEntry entry) throws IOException {
        byte[] payload = entry.payload();
        int payloadSize = Long.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES + payload.length;
        var buf = ByteBuffer.allocate(Integer.BYTES + payloadSize);
        buf.putInt(payloadSize);
        buf.putLong(entry.index());
        buf.putLong(entry.term().value());
        buf.putInt(entry.type().ordinal());
        buf.putInt(payload.length);
        buf.put(payload);
        buf.flip();
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
    }

    @Override
    public synchronized List<LogEntry> read(long fromIndex, int maxEntries) {
        if (fromIndex < 1 || fromIndex > lastIndex()) {
            return List.of();
        }
        int start = (int) (fromIndex - 1);
        int end = Math.min(index.size(), start + maxEntries);
        return List.copyOf(index.subList(start, end));
    }

    @Override
    public synchronized void truncateFrom(long idx) {
        if (idx < 1 || idx > lastIndex()) {
            return;
        }
        var survivors = new ArrayList<>(index.subList(0, (int) (idx - 1)));
        try {
            channel.truncate(0);
            channel.position(0);
            index.clear();
            for (var e : survivors) {
                writeFrame(e);
                index.add(e);
            }
            channel.force(true);
        } catch (IOException e) {
            throw new IllegalStateException("failed to truncate", e);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        channel.close();
    }
}
