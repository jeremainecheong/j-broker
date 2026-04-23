package jbroker.admin.api;

import java.util.ArrayList;
import jbroker.admin.client.BrokerAdminClientPool;
import jbroker.admin.dto.ClusterSummary;
import jbroker.admin.dto.HealthBadge;
import jbroker.admin.dto.NodeInfo;
import jbroker.admin.dto.RestError;
import jbroker.proto.broker.BrokerInfo;
import jbroker.proto.broker.DescribeClusterResponse;
import jbroker.proto.common.ErrorCode;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * {@code /api/v1/cluster} + {@code /api/v1/nodes} — the cluster-overview
 * surface consumed by the cluster topology UI page (P8.7) and the
 * {@code admin cluster-info} CLI command (P8.9).
 *
 * <p>Reads are first-success-among-brokers via {@link BrokerAdminClientPool}.
 * Every broker's metadata replica returns the same answer (modulo in-flight
 * Raft commits), so taking the first reachable one keeps latency low even
 * when one broker is dead.
 */
@RestController
@RequestMapping("/api/v1")
public class ClusterController {

    private final BrokerAdminClientPool pool;

    public ClusterController(BrokerAdminClientPool pool) {
        this.pool = pool;
    }

    @GetMapping("/cluster")
    public ClusterSummary cluster() {
        var resp = pool.describeCluster();
        return toSummary(resp);
    }

    @GetMapping("/nodes")
    public java.util.List<NodeInfo> nodes() {
        return cluster().nodes();
    }

    /**
     * P11.2 — tiny payload polled by every admin page's cluster-health badge
     * (every 5s via htmx). Derived from the existing {@code /cluster} response;
     * kept as a separate route so browser intermediaries cache it independently
     * and so a flaky admin-client-pool fallback doesn't knock the whole
     * cluster page out when all we need is a green/yellow/red hint.
     */
    @GetMapping("/health/badge")
    public HealthBadge healthBadge() {
        try {
            return HealthBadge.from(cluster());
        } catch (Exception e) {
            // Describe fan-out failed across every broker — we can't tell if
            // it's a network blip or a dead cluster; render red with a reason
            // the user can act on.
            return new HealthBadge("red", "admin unable to reach any broker: " + e.getMessage());
        }
    }

    @GetMapping("/nodes/{id}")
    public ResponseEntity<?> node(@PathVariable("id") int id) {
        var nodes = cluster().nodes();
        for (var n : nodes) {
            if (n.brokerId() == id) return ResponseEntity.ok(n);
        }
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(RestError.of("UNKNOWN_NODE", "no broker with id " + id));
    }

    public static ClusterSummary toSummary(DescribeClusterResponse resp) {
        if (resp.getError() != ErrorCode.OK) {
            throw new IllegalStateException(
                    "describeCluster returned error " + resp.getError().name());
        }
        var nodes = new ArrayList<NodeInfo>();
        for (BrokerInfo bi : resp.getNodesList()) {
            nodes.add(new NodeInfo(
                    bi.getBrokerId(), bi.getHost(), bi.getPort(), bi.getRole(), bi.getAlive(), bi.getLastSeenMillis()));
        }
        return new ClusterSummary(resp.getControllerId(), resp.getCurrentTerm(), resp.getMetadataOffset(), nodes);
    }
}
