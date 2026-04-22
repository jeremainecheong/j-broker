package jbroker.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Owns multiple {@link Log}s — one per topic-partition directory. Spawns a
 * single background thread that periodically invokes {@code retain} on each
 * log so expired segments get cleaned up without callers having to schedule
 * it themselves.
 *
 * <p>Layout: {@code <rootDir>/<topic>-<partition>/00000...log} per standard
 * Kafka convention.
 */
public final class LogManager implements AutoCloseable {

    public record Config(long segmentBytes, long retentionMillis, int indexIntervalBytes, long cleanerIntervalMillis) {
        public static Config defaults() {
            return new Config(
                    128L * 1024 * 1024, 7L * 24 * 60 * 60 * 1000, LogSegment.DEFAULT_INDEX_INTERVAL_BYTES, 60_000);
        }
    }

    private final Path rootDir;
    private final Config config;
    private final ConcurrentHashMap<String, Log> logs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LeaderEpochCheckpoint> epochCheckpoints = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleaner;

    public LogManager(Path rootDir, Config config) throws IOException {
        this.rootDir = rootDir;
        this.config = config;
        Files.createDirectories(rootDir);
        this.cleaner = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = Thread.ofVirtual().unstarted(r);
            t.setName("log-cleaner");
            return t;
        });
        this.cleaner.scheduleAtFixedRate(
                this::runCleaner,
                config.cleanerIntervalMillis(),
                config.cleanerIntervalMillis(),
                TimeUnit.MILLISECONDS);
    }

    public Log logFor(String topic, int partition) throws IOException {
        String key = topic + "-" + partition;
        var existing = logs.get(key);
        if (existing != null) return existing;
        synchronized (logs) {
            existing = logs.get(key);
            if (existing != null) return existing;
            var dir = rootDir.resolve(key);
            var logConfig =
                    new Log.Config(config.segmentBytes(), config.retentionMillis(), config.indexIntervalBytes());
            var log = Log.open(dir, logConfig);
            logs.put(key, log);
            return log;
        }
    }

    /**
     * Per-partition {@link LeaderEpochCheckpoint}. Lazily opened alongside
     * the partition directory; reused across calls. Used by the P6.4
     * follower reconciliation path via {@code OffsetsForLeaderEpoch}.
     */
    public LeaderEpochCheckpoint leaderEpochCheckpoint(String topic, int partition) throws IOException {
        String key = topic + "-" + partition;
        var existing = epochCheckpoints.get(key);
        if (existing != null) return existing;
        synchronized (epochCheckpoints) {
            existing = epochCheckpoints.get(key);
            if (existing != null) return existing;
            // Ensure the partition directory exists even if no log is open yet.
            var dir = rootDir.resolve(key);
            Files.createDirectories(dir);
            var cp = LeaderEpochCheckpoint.open(dir.resolve("leader-epoch-checkpoint"));
            epochCheckpoints.put(key, cp);
            return cp;
        }
    }

    /**
     * Phase 8 — evict every {@code (topic, *)} entry from the in-memory cache,
     * close the underlying {@link Log} handles, and best-effort delete the
     * on-disk partition directories. Used by
     * {@link jbroker.broker.MetadataStateMachine} on {@code DeleteTopicRecord}
     * so a topic recreated with the same name does not pick up a stale
     * {@code Log} handle whose offsets point at the old data.
     *
     * <p>Best-effort on disk: we swallow {@link IOException}s on individual
     * segment deletes so a deleted topic does not block the apply path. If
     * the directory outlives this call, the next restart's Log recovery
     * still treats it as a fresh partition once metadata says the topic
     * exists again — but the data-leak window is closed by evicting the
     * cache entry.
     */
    public synchronized void deleteTopicDir(String topic) {
        var prefix = topic + "-";
        var toRemove = new java.util.ArrayList<String>();
        for (var key : logs.keySet()) {
            if (key.startsWith(prefix)) toRemove.add(key);
        }
        for (var key : toRemove) {
            var log = logs.remove(key);
            if (log != null) {
                try {
                    log.close();
                } catch (IOException ignored) {
                    // best-effort — cleaner tick still iterates live logs only
                }
            }
            var dir = rootDir.resolve(key);
            deleteDirectoryTree(dir);
        }
        // Mirror epoch-checkpoint eviction so the leader-epoch file doesn't
        // survive topic recreation.
        var ckptRemove = new java.util.ArrayList<String>();
        for (var key : epochCheckpoints.keySet()) {
            if (key.startsWith(prefix)) ckptRemove.add(key);
        }
        for (var key : ckptRemove) {
            epochCheckpoints.remove(key);
        }
    }

    private static void deleteDirectoryTree(Path dir) {
        if (!Files.exists(dir)) return;
        try (var stream = Files.walk(dir)) {
            stream.sorted((a, b) -> b.getNameCount() - a.getNameCount()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best-effort — orphan files get cleaned up by operator
                    // or on the next broker restart's segment scan.
                }
            });
        } catch (IOException ignored) {
            // best-effort
        }
    }

    private void runCleaner() {
        long cutoff = System.currentTimeMillis() - config.retentionMillis();
        for (var log : logs.values()) {
            try {
                log.retain(cutoff);
            } catch (IOException ignored) {
                /* log line in a later observability pass */
            }
        }
    }

    @Override
    public void close() throws IOException {
        cleaner.shutdown();
        try {
            cleaner.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        for (var log : logs.values()) {
            log.close();
        }
    }
}
