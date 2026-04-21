package jbroker.raft.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
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
    private final Path metaPath; // null for test seam
    private long lastIncludedIndex;
    private Term lastIncludedTerm;

    private FileRaftLog(FileChannel channel, List<LogEntry> index, Path metaPath) {
        this.channel = channel;
        this.index = index;
        this.metaPath = metaPath;
        this.lastIncludedIndex = 0L;
        this.lastIncludedTerm = Term.ZERO;
    }

    public static FileRaftLog open(Path path) throws IOException {
        var channel =
                FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        var metaPath = path.resolveSibling(path.getFileName() + ".meta");
        var entries = rehydrate(channel);
        var log = new FileRaftLog(channel, entries, metaPath);
        log.loadMeta();
        return log;
    }

    /** Test-only seam: allows injecting a fault-injecting {@link FileChannel}. */
    static FileRaftLog openWithChannel(FileChannel channel) throws IOException {
        var index = rehydrate(channel);
        return new FileRaftLog(channel, index, null);
    }

    private void loadMeta() throws IOException {
        if (metaPath == null || !Files.exists(metaPath)) {
            return;
        }
        byte[] bytes = Files.readAllBytes(metaPath);
        if (bytes.length < Long.BYTES * 2) {
            return;
        }
        var buf = ByteBuffer.wrap(bytes);
        this.lastIncludedIndex = buf.getLong();
        this.lastIncludedTerm = new Term(buf.getLong());
    }

    private void persistMeta() throws IOException {
        if (metaPath == null) {
            return;
        }
        var buf = ByteBuffer.allocate(Long.BYTES * 2);
        buf.putLong(lastIncludedIndex);
        buf.putLong(lastIncludedTerm.value());
        Files.write(
                metaPath,
                buf.array(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.SYNC);
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
        return index.isEmpty() ? lastIncludedIndex : index.get(index.size() - 1).index();
    }

    @Override
    public synchronized long firstIndex() {
        return lastIncludedIndex + 1;
    }

    @Override
    public synchronized long lastIncludedIndex() {
        return lastIncludedIndex;
    }

    @Override
    public synchronized Term lastIncludedTerm() {
        return lastIncludedTerm;
    }

    @Override
    public synchronized Optional<Term> termAt(long idx) {
        if (idx == lastIncludedIndex && lastIncludedIndex > 0) {
            return Optional.of(lastIncludedTerm);
        }
        if (idx < firstIndex() || idx > lastIndex()) {
            return Optional.empty();
        }
        return Optional.of(index.get((int) (idx - firstIndex())).term());
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
        int indexSizeBefore = index.size();
        long channelSizeBefore = -1L;
        try {
            channelSizeBefore = channel.size();
            channel.position(channelSizeBefore);
            for (var entry : entries) {
                writeFrame(entry);
                index.add(entry);
            }
            channel.force(true);
        } catch (IOException e) {
            // Roll back: keep the in-memory index consistent with what's on
            // disk so subsequent operations don't observe entries whose bytes
            // were never durably written.  Best-effort truncate back to the
            // pre-append length; if the channel itself is dead, we can't do
            // better than leaving rehydrate to clean up on reopen.
            while (index.size() > indexSizeBefore) {
                index.remove(index.size() - 1);
            }
            if (channelSizeBefore >= 0) {
                try {
                    channel.truncate(channelSizeBefore);
                } catch (IOException ignored) {
                    // channel likely unusable; rehydrate will truncate torn frames on reopen
                }
            }
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
        if (fromIndex < firstIndex() || fromIndex > lastIndex()) {
            return List.of();
        }
        int start = (int) (fromIndex - firstIndex());
        int end = Math.min(index.size(), start + maxEntries);
        return List.copyOf(index.subList(start, end));
    }

    @Override
    public synchronized void truncateFrom(long idx) {
        if (idx < firstIndex() || idx > lastIndex()) {
            return;
        }
        int listPos = (int) (idx - firstIndex());
        var survivors = new ArrayList<>(index.subList(0, listPos));
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
    public synchronized void truncatePrefix(long firstIndex, Term firstTerm) {
        if (firstIndex <= firstIndex()) {
            return;
        }
        long newLastIncluded = firstIndex - 1;
        // Keep entries at absolute index >= firstIndex.
        var survivors = new ArrayList<LogEntry>();
        for (var e : index) {
            if (e.index() >= firstIndex) {
                survivors.add(e);
            }
        }
        try {
            channel.truncate(0);
            channel.position(0);
            index.clear();
            for (var e : survivors) {
                writeFrame(e);
                index.add(e);
            }
            channel.force(true);
            this.lastIncludedIndex = newLastIncluded;
            this.lastIncludedTerm = Objects.requireNonNull(firstTerm, "firstTerm");
            persistMeta();
        } catch (IOException e) {
            throw new IllegalStateException("failed to truncate prefix", e);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        channel.close();
    }
}
