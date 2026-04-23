package jbroker.admin.ui;

import jbroker.admin.api.ClusterController;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * P11.2 — HTML fragment counterpart to {@code GET /api/v1/health/badge}. The
 * top-nav's htmx poll swaps this fragment's outerHTML every 5s so the pill
 * keeps its colour + tooltip in sync with cluster state.
 *
 * <p>We deliberately split REST vs view: {@code /api/v1/health/badge} returns
 * JSON for scripts + future admin CLIs; {@code /ui/health/badge} returns a
 * Thymeleaf {@code <span>} the browser can drop directly into the DOM.
 */
@Controller
public class HealthBadgeViewController {

    private final ClusterController cluster;

    public HealthBadgeViewController(ClusterController cluster) {
        this.cluster = cluster;
    }

    @GetMapping("/ui/health/badge")
    public String badge(Model model) {
        // Thymeleaf 3.1 requires parameters be passed by name rather than by
        // position; omitting "badge=" yields an IllegalArgumentException about
        // synthetic parameter names.
        model.addAttribute("badge", cluster.healthBadge());
        return "fragments/shell :: badge(badge=${badge})";
    }
}
