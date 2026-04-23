package jbroker.admin.ui;

import jbroker.admin.api.ClusterController;
import jbroker.admin.client.BrokerAdminClientPool;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * P11.6 — chaos control plane UI. Renders a per-broker card grid with
 * kill/pause/resume buttons plus a partition control, all wired through
 * htmx POSTs to the existing {@code /api/v1/chaos/...} REST surface.
 *
 * <p>Closes the P9.4 deferred item: the REST layer + broker-side chaos
 * HTTP servers have been live since P9.3, but until now the only way to
 * exercise them was curl. With this page the cluster's failure injection
 * is interactive — kill a leader from the browser, watch SSE-driven
 * events in the right rail update, see the cluster-health pill flip to
 * yellow then green as the system heals.
 */
@Controller
public class ChaosViewController {

    private final BrokerAdminClientPool pool;

    public ChaosViewController(BrokerAdminClientPool pool) {
        this.pool = pool;
    }

    @GetMapping("/chaos")
    public String chaos(Model model) {
        var summary = ClusterController.toSummary(pool.describeCluster());
        model.addAttribute("cluster", summary);
        return "chaos";
    }
}
