package jbroker.admin.ui;

import jbroker.admin.api.ClusterController;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * Chaos control plane UI. Renders a live topology SVG + per-broker
 * action grid. State is fetched client-side via {@code /api/v1/cluster}
 * and {@code /api/v1/chaos/state} on a 3s poll so kill / pause / partition
 * actions are visible in real time without a page reload.
 *
 * <p>Uses {@link ClusterController#cluster()} (the merged fan-out) for
 * the initial render so every broker's role appears correctly on first
 * paint — the single-broker {@code pool.describeCluster()} shortcut
 * stamps peers as UNKNOWN in a 3-broker cluster (UI audit #1 root cause).
 */
@Controller
public class ChaosViewController {

    private final ClusterController clusterController;

    public ChaosViewController(ClusterController clusterController) {
        this.clusterController = clusterController;
    }

    @GetMapping("/chaos")
    public String chaos(Model model) {
        model.addAttribute("cluster", clusterController.cluster());
        return "chaos";
    }
}
