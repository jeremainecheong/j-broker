package jbroker.admin.ui;

import jbroker.admin.api.ClusterController;
import jbroker.admin.client.BrokerAdminClientPool;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * Thymeleaf view controller for the cluster topology landing page.
 * Renders the cluster overview including an SVG topology diagram where
 * every broker appears as a circle positioned on a ring; the controller
 * node gets a distinct fill so E2E HTML-assertions can locate it via
 * {@code data-broker-role="LEADER"}.
 */
@Controller
public class ClusterViewController {

    private final BrokerAdminClientPool pool;

    public ClusterViewController(BrokerAdminClientPool pool) {
        this.pool = pool;
    }

    @GetMapping("/")
    public String index(Model model) {
        var resp = pool.describeCluster();
        model.addAttribute("cluster", ClusterController.toSummary(resp));
        return "index";
    }
}
