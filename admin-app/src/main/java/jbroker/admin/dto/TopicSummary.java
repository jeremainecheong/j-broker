package jbroker.admin.dto;

import java.util.Map;

public record TopicSummary(
        String name, int partitions, int replicationFactor, boolean internal, boolean compact, long createdMillis) {

    public static TopicSummary of(jbroker.proto.broker.TopicDescription t) {
        // `compact` on the proto is the creation-time flag — it's derived
        // from `internal` on createTopic and never updated when config
        // overlays later flip `cleanup.policy`. Operators who set
        // `cleanup.policy=compact` via the admin UI or REST expect the
        // Topics list to reflect that, so OR the config overlay in here
        // at display time. If either signal says "compact", show Yes.
        boolean effectiveCompact =
                t.getCompact() || "compact".equals(t.getConfigMap().get("cleanup.policy"));
        return new TopicSummary(
                t.getTopic(),
                t.getPartitions(),
                t.getReplicationFactor(),
                t.getInternal(),
                effectiveCompact,
                t.getCreatedMillis());
    }

    /** For admin-level diagnostics pages the full config is also worth showing. */
    public static record WithConfig(TopicSummary summary, Map<String, String> config) {
        public WithConfig {
            config = config == null ? Map.of() : Map.copyOf(config);
        }
    }
}
