package jbroker.broker;

import java.util.Map;

/**
 * Catalogue entry for a single topic. {@code internal} hides
 * the topic from {@code Admin.ListTopics}, and {@code compact} marks the
 * topic for read-time compaction during recovery walks. The
 * opaque {@code config} map is surfaced by the admin REST API.
 */
public record TopicDescription(
        String topic,
        int partitions,
        int replicationFactor,
        long createdMillis,
        boolean internal,
        boolean compact,
        Map<String, String> config) {

    /**
     * Per-topic override for the acks=all durability floor. Validated by
     * {@code AdminHandler} at create/update time: integer, ≥ 1, ≤ the
     * topic's replication factor.
     */
    public static final String MIN_INSYNC_REPLICAS_CONFIG = "min.insync.replicas";

    /**
     * Per-topic override for the largest serialized batch a produce may
     * carry. Validated by {@code AdminHandler}: integer, ≥ 1, ≤
     * {@link #MAX_MESSAGE_BYTES_HARD_CAP}.
     */
    public static final String MAX_MESSAGE_BYTES_CONFIG = "max.message.bytes";

    /**
     * Absolute ceiling for any {@value #MAX_MESSAGE_BYTES_CONFIG} override —
     * aligned with gRPC's 4 MiB default inbound frame limit doubled; larger
     * batches would need transport reconfiguration on every hop.
     */
    public static final int MAX_MESSAGE_BYTES_HARD_CAP = 8 * 1024 * 1024;

    /**
     * Per-topic time-retention override, in milliseconds. Validated by
     * {@code AdminHandler}: {@code -1} (unlimited) or ≥ 1. Ignored on
     * compacted topics — deleting a key's latest value would break the
     * compaction contract.
     */
    public static final String RETENTION_MS_CONFIG = "retention.ms";

    /**
     * Per-topic size-retention override: the byte budget the partition log
     * converges to. Validated by {@code AdminHandler}: {@code -1}
     * (unlimited) or ≥ 1. Ignored on compacted topics.
     */
    public static final String RETENTION_BYTES_CONFIG = "retention.bytes";

    /**
     * Per-topic segment roll threshold, in bytes. Validated by
     * {@code AdminHandler}: within
     * [{@link #SEGMENT_BYTES_FLOOR}, {@link #SEGMENT_BYTES_HARD_CAP}].
     */
    public static final String SEGMENT_BYTES_CONFIG = "segment.bytes";

    /** Smallest sane roll threshold — below this every batch rolls a segment. */
    public static final long SEGMENT_BYTES_FLOOR = 1024;

    /**
     * Per-topic flush count trigger: fsync the active segment every N
     * appended records. Validated by {@code AdminHandler}: {@code -1}
     * (off, the default durability model — fsync on roll + replication)
     * or ≥ 1.
     */
    public static final String FLUSH_MESSAGES_CONFIG = "flush.messages";

    /**
     * Per-topic flush age trigger, in milliseconds: fsync the active
     * segment once unflushed data is older than this. Validated by
     * {@code AdminHandler}: {@code -1} (off) or ≥ 1.
     */
    public static final String FLUSH_MS_CONFIG = "flush.ms";

    /**
     * Ceiling for any {@value #SEGMENT_BYTES_CONFIG} override. Segment file
     * positions are tracked as ints, and the roll check runs after the
     * append, so a segment can exceed the threshold by one max-size batch —
     * 1 GiB leaves that arithmetic far from the 2 GiB int limit.
     */
    public static final long SEGMENT_BYTES_HARD_CAP = 1024L * 1024 * 1024;

    public TopicDescription {
        config = config == null ? Map.of() : Map.copyOf(config);
    }

    /**
     * The acks=all floor this topic actually enforces.
     *
     * <p>An explicit {@value #MIN_INSYNC_REPLICAS_CONFIG} config wins
     * verbatim — the operator said so, and it was validated against the
     * replication factor when set. Without one, the cluster default
     * applies, clamped to the replication factor so single-replica dev
     * topics stay usable with acks=all under the strict cluster default
     * (min(2, 1) = 1). A malformed stored value must not brick the
     * produce path, so it falls back to the clamped cluster default.
     */
    public int effectiveMinInsyncReplicas(int clusterDefault) {
        var explicit = config.get(MIN_INSYNC_REPLICAS_CONFIG);
        if (explicit != null) {
            try {
                return Math.max(1, Integer.parseInt(explicit.trim()));
            } catch (NumberFormatException e) {
                // Validated at admin time; fall through.
            }
        }
        return Math.max(1, Math.min(clusterDefault, replicationFactor));
    }

    /**
     * The produce-batch size limit this topic actually enforces: the
     * explicit {@value #MAX_MESSAGE_BYTES_CONFIG} override when present
     * (validated at set time), else the cluster default. A malformed
     * stored value falls back to the cluster default rather than failing
     * the produce path.
     */
    public int effectiveMaxMessageBytes(int clusterDefault) {
        var explicit = config.get(MAX_MESSAGE_BYTES_CONFIG);
        if (explicit != null) {
            try {
                return Math.max(1, Integer.parseInt(explicit.trim()));
            } catch (NumberFormatException e) {
                // Validated at admin time; fall through.
            }
        }
        return clusterDefault;
    }

    /**
     * The time-retention window this topic actually enforces, in
     * milliseconds: the explicit {@value #RETENTION_MS_CONFIG} override when
     * present (validated at set time; {@code -1} means unlimited), else the
     * cluster default. A malformed stored value falls back to the cluster
     * default rather than failing the cleaner.
     */
    public long effectiveRetentionMillis(long clusterDefault) {
        return effectiveLongConfig(RETENTION_MS_CONFIG, clusterDefault);
    }

    /**
     * The size-retention budget this topic actually enforces, in bytes:
     * the explicit {@value #RETENTION_BYTES_CONFIG} override when present
     * ({@code -1} means unlimited), else the cluster default.
     */
    public long effectiveRetentionBytes(long clusterDefault) {
        return effectiveLongConfig(RETENTION_BYTES_CONFIG, clusterDefault);
    }

    /**
     * The segment roll threshold this topic actually uses, in bytes: the
     * explicit {@value #SEGMENT_BYTES_CONFIG} override when present
     * (clamped to {@link #SEGMENT_BYTES_FLOOR} against malformed stored
     * values), else the cluster default.
     */
    public long effectiveSegmentBytes(long clusterDefault) {
        long value = effectiveLongConfig(SEGMENT_BYTES_CONFIG, clusterDefault);
        return Math.max(SEGMENT_BYTES_FLOOR, value);
    }

    /** The flush count trigger ({@code -1} = off): explicit override, else the cluster default. */
    public long effectiveFlushMessages(long clusterDefault) {
        return effectiveLongConfig(FLUSH_MESSAGES_CONFIG, clusterDefault);
    }

    /** The flush age trigger in ms ({@code -1} = off): explicit override, else the cluster default. */
    public long effectiveFlushMillis(long clusterDefault) {
        return effectiveLongConfig(FLUSH_MS_CONFIG, clusterDefault);
    }

    private long effectiveLongConfig(String key, long clusterDefault) {
        var explicit = config.get(key);
        if (explicit != null) {
            try {
                return Long.parseLong(explicit.trim());
            } catch (NumberFormatException e) {
                // Validated at admin time; fall through.
            }
        }
        return clusterDefault;
    }

    /** Back-compat constructor: empty config map. */
    public TopicDescription(
            String topic,
            int partitions,
            int replicationFactor,
            long createdMillis,
            boolean internal,
            boolean compact) {
        this(topic, partitions, replicationFactor, createdMillis, internal, compact, Map.of());
    }

    /** Back-compat constructor: defaults internal + compact + config. */
    public TopicDescription(String topic, int partitions, int replicationFactor, long createdMillis) {
        this(topic, partitions, replicationFactor, createdMillis, false, false, Map.of());
    }

    /** Returns a copy of this description with {@code config} merged in (put-wins). */
    public TopicDescription withMergedConfig(Map<String, String> overlay) {
        if (overlay == null || overlay.isEmpty()) return this;
        var merged = new java.util.HashMap<>(config);
        merged.putAll(overlay);
        return new TopicDescription(
                topic, partitions, replicationFactor, createdMillis, internal, compact, Map.copyOf(merged));
    }
}
