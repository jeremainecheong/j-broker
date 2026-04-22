package jbroker.admin.ui;

import jakarta.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import jbroker.admin.client.BrokerAdminClient;
import jbroker.admin.client.BrokerAdminClientPool;
import jbroker.admin.dto.TopicDetail;
import jbroker.admin.dto.TopicSummary;
import jbroker.broker.ErrorCodes;
import jbroker.proto.broker.DescribeTopicPartitionsResponse;
import jbroker.proto.broker.PartitionStateInfo;
import jbroker.proto.common.ErrorCode;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * Thymeleaf view controller for topic list + detail pages and the
 * create/delete POST handlers the modal forms submit to.
 */
@Controller
public class TopicsViewController {

    private final BrokerAdminClientPool pool;

    public TopicsViewController(BrokerAdminClientPool pool) {
        this.pool = pool;
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
        var resp = pool.firstSuccessful(c -> c.describeTopicPartitions(name));
        if (resp.getError() == ErrorCode.UNKNOWN) {
            httpResponse.setStatus(404);
            model.addAttribute("topicName", name);
            return "topic-not-found";
        }
        model.addAttribute("topic", toDetail(resp));
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

    private TopicDetail toDetail(DescribeTopicPartitionsResponse r) {
        var parts = new ArrayList<TopicDetail.PartitionState>();
        for (PartitionStateInfo ps : r.getPartitionStatesList()) {
            parts.add(new TopicDetail.PartitionState(
                    ps.getPartition(),
                    ps.getLeader(),
                    ps.getIsrList(),
                    ps.getReplicasList(),
                    ps.getLeaderEpoch(),
                    ps.getPartitionEpoch(),
                    ps.getHighWatermark(),
                    ps.getLogEndOffset()));
        }
        return new TopicDetail(
                r.getTopic(),
                r.getPartitions(),
                r.getReplicationFactor(),
                r.getInternal(),
                r.getCompact(),
                r.getCreatedMillis(),
                r.getConfigMap(),
                parts);
    }

    @SuppressWarnings("unused")
    private record Unused(List<Integer> l) {}
}
