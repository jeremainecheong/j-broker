package jbroker.admin.ui;

import jakarta.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import jbroker.admin.api.TopicsController;
import jbroker.admin.client.BrokerAdminClient;
import jbroker.admin.client.BrokerAdminClientPool;
import jbroker.admin.dto.TopicSummary;
import jbroker.broker.ErrorCodes;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * Thymeleaf view controller for topic list + detail pages and the
 * create/delete POST handlers the modal forms submit to.
 *
 * <p>The detail page delegates to
 * {@link TopicsController#describeTopic(String)} (merged fan-out) so every
 * partition card renders real HWM/LEO instead of the follower-sentinel
 * fallback. Calling {@code pool.firstSuccessful} directly here would pin
 * the response to whichever broker answers first — fine if that broker
 * happens to lead every partition, wrong otherwise (see / 2026-04-24
 * admin-UI audit root cause).
 */
@Controller
public class TopicsViewController {

    private final BrokerAdminClientPool pool;
    private final TopicsController topicsController;

    public TopicsViewController(BrokerAdminClientPool pool, TopicsController topicsController) {
        this.pool = pool;
        this.topicsController = topicsController;
    }

    @GetMapping("/topics")
    public String list(Model model) {
        var resp = pool.firstSuccessful(BrokerAdminClient::listTopics);
        var out = new ArrayList<TopicSummary>();
        for (var t : resp.getTopicsList()) {
            out.add(TopicSummary.of(t));
        }
        model.addAttribute("topics", out);
        return "topics";
    }

    @GetMapping("/topics/{name}")
    public String detail(@PathVariable("name") String name, Model model, HttpServletResponse httpResponse) {
        var detail = topicsController.describeTopic(name);
        if (detail.isEmpty()) {
            httpResponse.setStatus(404);
            model.addAttribute("topicName", name);
            return "topic-not-found";
        }
        model.addAttribute("topic", detail.get());
        return "topic-detail";
    }

    @PostMapping("/ui/topics")
    public String create(
            @RequestParam String name,
            @RequestParam(defaultValue = "1") int partitions,
            @RequestParam(defaultValue = "1") int replicationFactor) {
        pool.firstNonNotLeader(
                c -> c.createTopic(name, partitions, replicationFactor, Map.of()),
                r -> r.hasError() ? r.getError().getCode() : 0,
                ErrorCodes.NOT_LEADER);
        return "redirect:/topics";
    }

    @PostMapping("/ui/topics/{name}/delete")
    public String delete(@PathVariable("name") String name) {
        pool.firstNonNotLeader(
                c -> c.deleteTopic(name), r -> r.hasError() ? r.getError().getCode() : 0, ErrorCodes.NOT_LEADER);
        return "redirect:/topics";
    }

    @SuppressWarnings("unused")
    private record Unused(List<Integer> l) {}
}
