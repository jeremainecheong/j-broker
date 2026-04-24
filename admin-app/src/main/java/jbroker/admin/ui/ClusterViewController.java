package jbroker.admin.ui;

import jbroker.admin.api.ClusterController;
import jbroker.admin.api.ConsumerGroupsController;
import jbroker.admin.api.TopicsController;
import jbroker.admin.dto.ClusterSummary;
import jbroker.admin.dto.HealthBadge;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * P11.3 — cluster overview landing page. Builds on top of the old topology
 * view by adding an alert strip + summary-card grid + throughput
 * sparklines. The old topology (SVG + node table) stays as the bottom
 * section of the page so every cluster signal is visible on one screen.
 *
 * <p>Delegates to {@link ClusterController#cluster()} (the merged fan-out)
 * so every broker's role lands concretely on first paint — broker-core's
 * {@code DescribeCluster} stamps peers as {@code UNKNOWN} in a multi-broker
 * cluster, so calling {@code pool.describeCluster()} directly would show
 * 2/3 UNKNOWN on the nodes table. The REST controller's merge overlays
 * self-reports across every responder.
 *
 * <p>Topic + group counts come from best-effort calls to the same REST
 * surfaces the /topics and /groups pages use — catching exceptions so a
 * flaky broker doesn't knock the overview page off the air.
 */
@Controller
public class ClusterViewController {

    private final ClusterController clusterController;
    private final TopicsController topics;
    private final ConsumerGroupsController groups;

    public ClusterViewController(
            ClusterController clusterController, TopicsController topics, ConsumerGroupsController groups) {
        this.clusterController = clusterController;
        this.topics = topics;
        this.groups = groups;
    }

    @GetMapping("/")
    public String index(Model model) {
        ClusterSummary cluster = clusterController.cluster();
        model.addAttribute("cluster", cluster);
        model.addAttribute("badge", HealthBadge.from(cluster));
        model.addAttribute("topicCount", safeTopicCount());
        model.addAttribute("groupCount", safeGroupCount());
        return "index";
    }

    private int safeTopicCount() {
        try {
            return topics.list().size();
        } catch (Exception e) {
            return -1;
        }
    }

    private int safeGroupCount() {
        try {
            return groups.list().size();
        } catch (Exception e) {
            return -1;
        }
    }
}
