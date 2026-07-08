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

    public TopicDescription {
        config = config == null ? Map.of() : Map.copyOf(config);
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
