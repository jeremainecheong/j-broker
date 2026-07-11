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
