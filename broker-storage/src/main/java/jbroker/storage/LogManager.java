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
 * Kafka convention, plus a {@link FormatVersion#FILE_NAME} marker at the
 * root recording the on-disk format. Opening a root written by a newer
 * broker throws — see {@link FormatVersion}.
 */
public final class LogManager implements AutoCloseable {

    /**
     * Cluster-default storage settings. {@code retentionMillis} and
     * {@code retentionBytes} accept negative values meaning unlimited.
     * Per-topic overrides resolve through the {@link TopicLogConfigResolver}.
     */
    public record Config(
            long segmentBytes,
            long retentionMillis,
            long retentionBytes,
            int indexIntervalBytes,
            long cleanerIntervalMillis) {

        /** Pre-size-retention shape: {@code retentionBytes} defaults to unlimited. */
        public Config(long segmentBytes, long retentionMillis, int indexIntervalBytes, long cleanerIntervalMillis) {
            this(segmentBytes, retentionMillis, -1L, indexIntervalBytes, cleanerIntervalMillis);
        }

        public static Config defaults() {
            return new Config(
                    128L * 1024 * 1024, 7L * 24 * 60 * 60 * 1000, -1L, LogSegment.DEFAULT_INDEX_INTERVAL_BYTES, 60_000);
        }
    }

    /**
     * A topic's effective storage settings, as resolved by the broker layer
     * (explicit topic config, else cluster default). Negative retention
     * values mean unlimited — compacted topics resolve to unlimited so the
     * cleaner never deletes a key's latest value. Negative flush values
     * disable the respective trigger (the default durability model: fsync
     * on segment roll, replication in between).
     */
    public record TopicLogConfig(
            long segmentBytes, long retentionMillis, long retentionBytes, long flushMessages, long flushMillis) {

        /** Pre-flush-policy shape: both flush triggers off. */
        public TopicLogConfig(long segmentBytes, long retentionMillis, long retentionBytes) {
            this(segmentBytes, retentionMillis, retentionBytes, -1L, -1L);
        }
    }

    /**
     * Resolves a topic's effective log config. Empty means the topic is
     * unknown to the resolver; the cluster-default {@link Config} applies.
     */
    @FunctionalInterface
    public interface TopicLogConfigResolver {
        java.util.Optional<TopicLogConfig> resolve(String topic);
    }

    private final Path rootDir;
    private final Config config;
    private final ConcurrentHashMap<String, Log> logs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LeaderEpochCheckpoint> epochCheckpoints = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleaner;

    private volatile TopicLogConfigResolver topicConfigResolver = topic -> java.util.Optional.empty();

    /** Wire the broker's topic catalogue in. Cleaner ticks + log opens resolve through it. */
    public void setTopicLogConfigResolver(TopicLogConfigResolver resolver) {
        this.topicConfigResolver = java.util.Objects.requireNonNull(resolver);
    }

    private TopicLogConfig effectiveConfig(String topic) {
        return topicConfigResolver
                .resolve(topic)
                .orElseGet(() ->
                        new TopicLogConfig(config.segmentBytes(), config.retentionMillis(), config.retentionBytes()));
    }

    /**
     * Flush-tick cadence. Also the freshness bound for pushing per-topic
     * config to open logs — much tighter than the cleaner interval so
     * flush.ms values in the low seconds actually mean what they say.
     */
    static final long FLUSH_TICK_MILLIS = 1_000L;

    /**
     * Push each open log's effective config and apply the flush age
     * trigger. Runs every {@link #FLUSH_TICK_MILLIS}; iterating the open
     * logs with volatile reads is cheap, and no fsync happens unless a
     * policy is set and due.
     */
    private void runFlushTick() {
        long nowMillis = System.currentTimeMillis();
        for (var entry : logs.entrySet()) {
            String key = entry.getKey();
            int dash = key.lastIndexOf('-');
            if (dash <= 0) continue;
            var log = entry.getValue();
            var effective = effectiveConfig(key.substring(0, dash));
            log.reconfigureSegmentBytes(effective.segmentBytes());
            log.reconfigureFlushPolicy(effective.flushMessages(), effective.flushMillis());
            try {
                log.flushIfDue(nowMillis);
            } catch (IOException ignored) {
                /* log line in a later observability pass */
            }
        }
    }

    /**
     * The data dir's marker version as of the last {@link FormatVersion}
     * interaction. Fast-path read for the control-append gate; only ever
     * moves forward, under {@link #formatLock}.
     */
    private volatile int diskFormat;

    private final Object formatLock = new Object();

    public LogManager(Path rootDir, Config config) throws IOException {
        this.rootDir = rootDir;
        this.config = config;
        Files.createDirectories(rootDir);
        // Refuse before spawning anything: a data dir written by a newer
        // broker must fail the open, not be reinterpreted.
        this.diskFormat = FormatVersion.check(rootDir);
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
        long flushTick = Math.min(FLUSH_TICK_MILLIS, config.cleanerIntervalMillis());
        this.cleaner.scheduleAtFixedRate(this::runFlushTick, flushTick, flushTick, TimeUnit.MILLISECONDS);
    }

    public Log logFor(String topic, int partition) throws IOException {
        String key = topic + "-" + partition;
        var existing = logs.get(key);
        if (existing != null) return existing;
        synchronized (logs) {
            existing = logs.get(key);
            if (existing != null) return existing;
            var dir = rootDir.resolve(key);
            var effective = effectiveConfig(topic);
            var logConfig =
                    new Log.Config(effective.segmentBytes(), effective.retentionMillis(), config.indexIntervalBytes());
            var log = Log.open(dir, logConfig);
            log.setControlAppendGate(this::ensureControlWritable);
            logs.put(key, log);
            return log;
        }
    }

    /**
     * Gate run before any log under this root writes its first control
     * batch: a data dir still marked format 1 (written before control
     * batches existed) is re-stamped to {@link FormatVersion#TRANSACTIONS}
     * so a rolling downgrade to a binary that would misread markers as
     * application data refuses at open. Idempotent and cheap once stamped.
     */
    public void ensureControlWritable() throws IOException {
        if (diskFormat >= FormatVersion.TRANSACTIONS) return;
        synchronized (formatLock) {
            if (diskFormat >= FormatVersion.TRANSACTIONS) return;
            diskFormat = FormatVersion.ensureAtLeast(rootDir, FormatVersion.TRANSACTIONS);
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
        long nowMillis = System.currentTimeMillis();
        for (var entry : logs.entrySet()) {
            var log = entry.getValue();
            try {
                // Key on the topic portion of the "<topic>-<partition>" map key;
                // the partition suffix follows the last dash on a numeric.
                String key = entry.getKey();
                int dash = key.lastIndexOf('-');
                if (dash <= 0) continue;
                String topic = key.substring(0, dash);
                var effective = effectiveConfig(topic);
                // (Config pushes to live logs ride the faster flush tick.)
                long cutoff =
                        effective.retentionMillis() < 0 ? Long.MIN_VALUE : nowMillis - effective.retentionMillis();
                log.retain(cutoff, effective.retentionBytes());
                // Also compact logs belonging to compact-policy topics.
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
