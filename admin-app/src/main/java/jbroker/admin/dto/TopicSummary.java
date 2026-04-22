package jbroker.admin.dto;

import java.util.Map;

public record TopicSummary(
        String name, int partitions, int replicationFactor, boolean internal, boolean compact, long createdMillis) {

    public static TopicSummary of(jbroker.proto.broker.TopicDescription t) {
        return new TopicSummary(
                t.getTopic(),
                t.getPartitions(),
                t.getReplicationFactor(),
                t.getInternal(),
                t.getCompact(),
                t.getCreatedMillis());
    }

    /** For admin-level diagnostics pages the full config is also worth showing. */
    public static record WithConfig(TopicSummary summary, Map<String, String> config) {
        public WithConfig {
            config = config == null ? Map.of() : Map.copyOf(config);
        }
    }
}
