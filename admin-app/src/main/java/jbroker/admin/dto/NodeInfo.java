package jbroker.admin.dto;

/**
 * JSON shape returned by {@code /api/v1/nodes} and embedded in
 * {@code ClusterSummary}. Matches the PRD §8.7 cluster-node response.
 */
public record NodeInfo(int brokerId, String host, int port, String role, boolean alive, long lastSeenMillis) {}
