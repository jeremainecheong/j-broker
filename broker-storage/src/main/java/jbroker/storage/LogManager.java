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
     * the partition directory; reused across calls. Used by the
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
     * Evict every {@code (topic, *)} entry from the in-memory cache,
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

    /** Topic → compact flag. Populated by the broker when a topic is created. */
    private final ConcurrentHashMap<String, Boolean> compactTopics = new ConcurrentHashMap<>();

    public void markTopicCompact(String topic, boolean compact) {
        compactTopics.put(topic, compact);
    }

    /** Synchronous compaction trigger, primarily for tests + admin tooling. */
    public int compactLogNow(String topic, int partition) throws IOException {
        return logFor(topic, partition).compactByKey();
    }

    /**
     * Synchronous compaction that no-ops cleanly when this broker
     * has no open log for {@code (topic, partition)}. Lets the admin
     * {@code ForceCompactPartition} RPC fan out to every broker and have
     * non-hosting brokers return without creating stray partition dirs.
     *
     * <p>Returns {@code OptionalInt.empty()} when the log isn't open
     * locally, else the survivor count from {@link Log#compactByKey()}.
     */
    public java.util.OptionalInt compactLogNowIfPresent(String topic, int partition) throws IOException {
        var log = logs.get(topic + "-" + partition);
        if (log == null) return java.util.OptionalInt.empty();
        return java.util.OptionalInt.of(log.compactByKey());
    }

    private void runCleaner() {
        long cutoff = System.currentTimeMillis() - config.retentionMillis();
        for (var entry : logs.entrySet()) {
            var log = entry.getValue();
            try {
                log.retain(cutoff);
                // Also compact logs belonging to compact-policy topics.
                // Key on the topic portion of the "<topic>-<partition>" map key;
                // the partition suffix follows the last dash on a numeric.
                String key = entry.getKey();
                int dash = key.lastIndexOf('-');
                if (dash <= 0) continue;
                String topic = key.substring(0, dash);
                if (Boolean.TRUE.equals(compactTopics.get(topic))) {
                    log.compactByKey();
                }
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
