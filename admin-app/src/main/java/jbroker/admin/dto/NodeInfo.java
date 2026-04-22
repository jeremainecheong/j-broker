package jbroker.admin.dto;

/**
 * JSON shape returned by {@code /api/v1/nodes} and embedded in
 * {@code ClusterSummary}. Matches thecluster-node response.
 */
public record NodeInfo(int brokerId, String host, int port, String role, boolean alive, long lastSeenMillis) {}
