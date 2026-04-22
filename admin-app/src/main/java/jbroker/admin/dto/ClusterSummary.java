package jbroker.admin.dto;

import java.util.List;

/**
 * JSON shape returned by {@code GET /api/v1/cluster}. Mirrors
 * {@code jbroker.proto.broker.DescribeClusterResponse} with controller id
 * surfaced (-1 if no leader observed yet).
 */
public record ClusterSummary(int controllerId, long term, long metadataOffset, List<NodeInfo> nodes) {
    public ClusterSummary {
        nodes = List.copyOf(nodes);
    }
}
