package jbroker.admin.dto;

/**
 * Tiny JSON payload polled by the cluster-health badge in the top nav.
 * {@code status} is one of {@code "green"}, {@code "yellow"}, {@code "red"};
 * {@code reason} is a short human-readable sentence the UI uses as the tooltip.
 *
 * <p>Derivation (kept intentionally cheap — this endpoint is hit every 5s
 * from every open browser):
 *
 * <ul>
 *   <li>{@code red} — {@code controllerId ≤ 0} (no Raft majority) or
 *       fewer than {@code floor(nodes.size()/2) + 1} brokers reporting alive.
 *   <li>{@code yellow} — majority up but at least one broker is dead.
 *   <li>{@code green} — controller known and every registered broker is alive.
 * </ul>
 *
 * <p>Per-partition ISR coverage would be a more sensitive signal but
 * requires a topic-list + partition-state fan-out; we intentionally scope
 * v1.1 to cluster-level liveness and let the topic detail page handle
 * partition-level health visualisation.
 */
public record HealthBadge(String status, String reason) {
    public static HealthBadge from(ClusterSummary cluster) {
        int total = cluster.nodes().size();
        if (total == 0) {
            // Distinct from "below majority": a registered-but-dead cluster is
            // a different operator signal than "no cluster yet".
            return new HealthBadge("red", "no brokers registered");
        }
        int alive = (int) cluster.nodes().stream().filter(NodeInfo::alive).count();
        int quorum = (total / 2) + 1;
        boolean hasController = cluster.controllerId() > 0;

        if (!hasController || alive < quorum) {
            return new HealthBadge(
                    "red",
                    !hasController
                            ? "no Raft leader — metadata plane unavailable"
                            : alive + "/" + total + " brokers alive — below majority (" + quorum + ")");
        }
        if (alive < total) {
            return new HealthBadge("yellow", alive + "/" + total + " brokers alive — majority up but degraded");
        }
        return new HealthBadge("green", total + "/" + total + " brokers alive — controller " + cluster.controllerId());
    }
}
