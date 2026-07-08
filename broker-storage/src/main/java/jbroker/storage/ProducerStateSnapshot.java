package jbroker.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Infrastructure-only {@code producer-state} snapshot file. The replication layer will
 * populate the map; this class provides atomic write/read and a simple binary
 * format. Layout:
 *
 * <pre>
 *  version       int32 (0)
 *  count         int32
 *  count * {
 *      producerId:        int64
 *      producerEpoch:     int16
 *      lastSequence:      int32
 *      lastOffset:        int64
 *  }
 * </pre>
 */
public final class ProducerStateSnapshot {

    public record Entry(long producerId, short producerEpoch, int lastSequence, long lastOffset) {}

    private final Map<Long, Entry> entries = new LinkedHashMap<>();

    public void put(Entry e) {
        entries.put(e.producerId(), e);
    }

    public Map<Long, Entry> entries() {
        return Map.copyOf(entries);
    }

    public void writeTo(Path path) throws IOException {
        var tmp = path.resolveSibling(path.getFileName() + ".tmp");
        var buf = ByteBuffer.allocate(4 + 4 + entries.size() * (8 + 2 + 4 + 8));
        buf.putInt(0);
        buf.putInt(entries.size());
        for (var e : entries.values()) {
            buf.putLong(e.producerId());
            buf.putShort(e.producerEpoch());
            buf.putInt(e.lastSequence());
            buf.putLong(e.lastOffset());
        }
        buf.flip();
        Files.write(
                tmp,
                java.util.Arrays.copyOf(buf.array(), buf.limit()),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE,
                StandardOpenOption.SYNC);
        Files.move(
                tmp,
                path,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    public static ProducerStateSnapshot readFrom(Path path) throws IOException {
        var snapshot = new ProducerStateSnapshot();
        if (!Files.exists(path)) return snapshot;
        var bytes = Files.readAllBytes(path);
        var buf = ByteBuffer.wrap(bytes);
        int version = buf.getInt();
        if (version != 0) return snapshot;
        int count = buf.getInt();
        for (int i = 0; i < count; i++) {
            long pid = buf.getLong();
            short pe = buf.getShort();
            int seq = buf.getInt();
            long off = buf.getLong();
            snapshot.put(new Entry(pid, pe, seq, off));
        }
        return snapshot;
    }
}
