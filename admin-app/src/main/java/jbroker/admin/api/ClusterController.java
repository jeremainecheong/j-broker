package jbroker.admin.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        // P11.9 — fan out so every broker can report its OWN role.
        // Broker-core's DescribeCluster stamps "UNKNOWN" for peers it doesn't
        // self-track; first-successful-only therefore shows 2/3 UNKNOWN in a
        // 3-broker cluster. Merging self-reports across responses gives every
        // broker its authoritative role.
        List<DescribeClusterResponse> all;
        try {
            all = pool.allSuccessful(c -> c.describeCluster());
        } catch (IllegalStateException unreachable) {
            throw unreachable;
        }
        return toSummary(mergeSelfReportedRoles(all));
    }

    /**
     * Picks the first OK response as the primary (ISR lists + controller id +
     * broker address maps are identical across replicas, modulo in-flight
     * Raft applies) and overlays the role field using per-broker self-reports.
     *
     * <p>Each broker's response concretely reports only its OWN role and
     * stamps every peer as {@code UNKNOWN}. So for each response we find the
     * node whose self-role is a concrete value and use that as the merge
     * contribution. Empty-role values and {@code UNKNOWN} don't overwrite
     * concrete ones, so the merge is order-independent.
     */
    static DescribeClusterResponse mergeSelfReportedRoles(List<DescribeClusterResponse> responses) {
        DescribeClusterResponse primary = null;
        for (var r : responses) {
            if (r.getError() == ErrorCode.OK) {
                primary = r;
                break;
            }
        }
        if (primary == null) {
            // Every response was an error — let the caller handle via toSummary's
            // error branch (throws IllegalStateException).
            return responses.get(0);
        }

        Map<Integer, String> selfRoles = new HashMap<>();
        for (var r : responses) {
            if (r.getError() != ErrorCode.OK) continue;
            for (var node : r.getNodesList()) {
                String role = node.getRole();
                if (role == null || role.isEmpty() || "UNKNOWN".equals(role)) continue;
                // A concrete role on a node means this responder IS that node
                // (broker-core only stamps concrete roles for self). Trust it.
                selfRoles.merge(node.getBrokerId(), role, (existing, incoming) -> existing);
            }
        }

        var builder = primary.toBuilder();
        builder.clearNodes();
        for (var node : primary.getNodesList()) {
            String better = selfRoles.get(node.getBrokerId());
            if (better != null && !better.equals(node.getRole())) {
                builder.addNodes(node.toBuilder().setRole(better).build());
            } else {
                builder.addNodes(node);
            }
        }
        return builder.build();
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
