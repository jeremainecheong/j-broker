package jbroker.admin.ui;

import jbroker.admin.api.ClusterController;
import jbroker.admin.api.ClusterLifecycleController;
import jbroker.admin.api.ConsumerGroupsController;
import jbroker.admin.api.TopicsController;
import jbroker.admin.dto.ClusterSummary;
import jbroker.admin.dto.HealthBadge;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * Cluster overview landing page. Builds on top of the old topology
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
 * flaky broker doesn't knock the overview page off the air. Membership
 * progress + pending reassignments come from
 * {@link ClusterLifecycleController}'s typed accessors with the same
 * best-effort contract, and the cluster-ops modal forms POST back through
 * the {@code /ui/cluster/*} handlers below.
 */
@Controller
public class ClusterViewController {

    private final ClusterController clusterController;
    private final TopicsController topics;
    private final ConsumerGroupsController groups;
    private final ClusterLifecycleController lifecycle;

    public ClusterViewController(
            ClusterController clusterController,
            TopicsController topics,
            ConsumerGroupsController groups,
            ClusterLifecycleController lifecycle) {
        this.clusterController = clusterController;
        this.topics = topics;
        this.groups = groups;
        this.lifecycle = lifecycle;
    }

    @GetMapping("/")
    public String index(Model model) {
        ClusterSummary cluster = clusterController.cluster();
        model.addAttribute("cluster", cluster);
        model.addAttribute("badge", HealthBadge.from(cluster));
        model.addAttribute("topicCount", safeTopicCount());
        model.addAttribute("groupCount", safeGroupCount());
        model.addAttribute("membership", lifecycle.membershipView().orElse(null));
        model.addAttribute("reassignments", lifecycle.reassignmentViews());
        return "index";
    }

    // ---- Cluster-ops form handlers. Delegate to the REST controller
    // (controller routing + refusal mapping live there) and redirect back
    // to the overview; refusals surface on reload via the membership chips
    // and the reassignments table, matching how /ui/topics handles errors. ----

    @PostMapping("/ui/cluster/add-broker")
    public String addBroker(
            @RequestParam int brokerId,
            @RequestParam String host,
            @RequestParam int raftPort,
            @RequestParam int brokerPort) {
        lifecycle.addBroker(new ClusterLifecycleController.AddBrokerBody(brokerId, host, raftPort, brokerPort));
        return "redirect:/";
    }

    @PostMapping("/ui/cluster/decommission/{id}")
    public String decommission(@PathVariable("id") int id) {
        lifecycle.decommission(id);
        return "redirect:/";
    }

    @PostMapping("/ui/cluster/reassignments/{topic}/{partition}/cancel")
    public String cancelReassignment(@PathVariable("topic") String topic, @PathVariable("partition") int partition) {
        lifecycle.cancelReassignment(topic, partition);
        return "redirect:/";
    }

    @PostMapping("/ui/cluster/rebalance-leaders")
    public String rebalanceLeaders() {
        lifecycle.rebalanceLeadership();
        return "redirect:/";
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
