package jbroker.broker.metrics;

import java.util.Map;

/**
 * immutable per-partition metrics entry consumed by
 * {@link jbroker.broker.MetadataServiceHandler#describeMetrics}. The
 * {@code replicationLagBytes} map keys by follower broker id; values are
 * (leaderLEO − followerLEO) in records, converted to an approximate byte
 * count by the caller. Leader entries are omitted from the map.
 */
public record PartitionMetricsSnapshot(
        String topic,
        int partition,
        int isrSize,
        long hwm,
        long leaderLogEndOffset,
        Map<Integer, Long> replicationLagBytes) {

    public PartitionMetricsSnapshot {
        replicationLagBytes = Map.copyOf(replicationLagBytes);
    }
}
