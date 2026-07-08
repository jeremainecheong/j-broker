package jbroker.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Persists the {@code (epoch, startOffset)} pairs that delimit leader epochs
 * in this partition's log. The replication layer uses this to honour the
 * {@code OffsetsForLeaderEpoch} RPC when a follower rejoins after a partition
 * change. The format matches Kafka's simple line-oriented checkpoint file:
 *
 * <pre>
 *  version      (always 0)
 *  entryCount
 *  epoch0 offset0
 *  epoch1 offset1
 *  ...
 * </pre>
 *
 * <p>Scope here: infrastructure only — storage for the pairs plus
 * atomic temp-file-then-rename for crash safety. The RPC + usage land later.
 */
public final class LeaderEpochCheckpoint {

    private final Path path;
    private final List<Entry> entries = new ArrayList<>();

    private LeaderEpochCheckpoint(Path path) {
        this.path = path;
    }

    public record Entry(int epoch, long startOffset) {}

    public static LeaderEpochCheckpoint open(Path path) throws IOException {
        var cp = new LeaderEpochCheckpoint(path);
        if (Files.exists(path)) cp.load();
        return cp;
    }

    private void load() throws IOException {
        var lines = Files.readAllLines(path);
        if (lines.isEmpty()) return;
        if (!lines.get(0).trim().equals("0")) return;
        if (lines.size() < 2) return;
        int count;
        try {
            count = Integer.parseInt(lines.get(1).trim());
        } catch (NumberFormatException e) {
            return;
        }
        for (int i = 0; i < count && 2 + i < lines.size(); i++) {
            var parts = lines.get(2 + i).trim().split("\\s+");
            if (parts.length != 2) continue;
            entries.add(new Entry(Integer.parseInt(parts[0]), Long.parseLong(parts[1])));
        }
    }

    public synchronized void assign(int epoch, long startOffset) throws IOException {
        // Dedup: entries must be monotonic in both epoch and offset.
        if (!entries.isEmpty()) {
            var last = entries.get(entries.size() - 1);
            if (last.epoch() >= epoch || last.startOffset() >= startOffset) return;
        }
        entries.add(new Entry(epoch, startOffset));
        flush();
    }

    public synchronized List<Entry> entries() {
        return List.copyOf(entries);
    }

    /** Largest epoch whose {@code startOffset <= offset}. */
    public synchronized Optional<Entry> epochFor(long offset) {
        Entry found = null;
        for (var e : entries) {
            if (e.startOffset() <= offset) found = e;
            else break;
        }
        return Optional.ofNullable(found);
    }

    private void flush() throws IOException {
        var tmp = path.resolveSibling(path.getFileName() + ".tmp");
        var sb = new StringBuilder();
        sb.append("0\n").append(entries.size()).append('\n');
        for (var e : entries) {
            sb.append(e.epoch()).append(' ').append(e.startOffset()).append('\n');
        }
        Files.writeString(
                tmp,
                sb.toString(),
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
}
