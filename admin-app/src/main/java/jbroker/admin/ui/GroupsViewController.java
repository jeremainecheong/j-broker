package jbroker.admin.ui;

import io.grpc.StatusRuntimeException;
import jakarta.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import jbroker.admin.client.BrokerAdminClient;
import jbroker.admin.client.BrokerAdminClientPool;
import jbroker.admin.dto.ConsumerGroupDetail;
import jbroker.admin.dto.ConsumerGroupSummary;
import jbroker.proto.broker.DescribeConsumerGroupResponse;
import jbroker.proto.broker.OffsetReset;
import jbroker.proto.common.ErrorCode;
import jbroker.proto.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * Thymeleaf view controller for consumer-group list + detail pages. The
 * read-side logic mirrors {@code ConsumerGroupsController} (which serves
 * the same data as JSON on {@code /api/v1/consumer-groups}); this variant
 * wraps the response in DTOs the Thymeleaf templates consume.
 */
@Controller
public class GroupsViewController {

    private static final Logger log = LoggerFactory.getLogger(GroupsViewController.class);

    private final BrokerAdminClientPool pool;

    public GroupsViewController(BrokerAdminClientPool pool) {
        this.pool = pool;
    }

    @GetMapping("/groups")
    public String list(Model model) {
        var byId = new LinkedHashMap<String, ConsumerGroupSummary>();
        for (var c : pool.clients()) {
            try {
                var resp = c.listConsumerGroups();
                if (resp.getError() != ErrorCode.OK) continue;
                for (var g : resp.getGroupsList()) {
                    byId.putIfAbsent(
                            g.getGroupId(),
                            new ConsumerGroupSummary(
                                    g.getGroupId(),
                                    g.getState(),
                                    g.getMemberCount(),
                                    g.getGeneration(),
                                    g.getAssignor(),
                                    g.getCoordinatorPartition()));
                }
            } catch (StatusRuntimeException e) {
                log.debug("broker {} unreachable: {}", c.address(), e.getStatus());
            }
        }
        model.addAttribute("groups", new ArrayList<>(byId.values()));
        return "groups";
    }

    @GetMapping("/groups/{id}")
    public String detail(@PathVariable("id") String id, Model model, HttpServletResponse response) {
        DescribeConsumerGroupResponse best = null;
        for (var c : pool.clients()) {
            try {
                var r = c.describeConsumerGroup(id);
                if (r.getError() == ErrorCode.OK) {
                    best = r;
                    break;
                }
                best = r;
            } catch (StatusRuntimeException e) {
                log.debug("broker {} failed describeConsumerGroup({}): {}", c.address(), id, e.getStatus());
            }
        }
        if (best == null || best.getError() != ErrorCode.OK) {
            response.setStatus(404);
            model.addAttribute("groupId", id);
            return "group-not-found";
        }
        model.addAttribute("detail", toDetail(best));
        return "group-detail";
    }

    /**
     * Form-POST handler the group-detail delete button submits to. Mirrors the
     * topic-delete pattern ({@link TopicsViewController#delete}): fan across
     * brokers until one returns OK (or UNKNOWN_GROUP), then redirect to the
     * group list. Non-OK responses still redirect — the group list will reflect
     * the actual post-delete state, which is the source of truth.
     */
    @PostMapping("/ui/groups/{id}/delete")
    public String delete(@PathVariable("id") String id) {
        for (var c : pool.clients()) {
            try {
                var resp = c.deleteConsumerGroup(id);
                if (resp.getError() == ErrorCode.OK || resp.getError() == ErrorCode.UNKNOWN_GROUP) {
                    break;
                }
            } catch (StatusRuntimeException e) {
                log.debug("broker {} failed deleteConsumerGroup({}): {}", c.address(), id, e.getStatus());
            }
        }
        return "redirect:/groups";
    }

    /**
     * Form-POST handler the reset-offsets modal submits to. A single
     * {@code (topic, partition, offset, leaderEpoch)} row is translated into
     * a one-element {@link OffsetReset} list and dispatched to whichever
     * broker owns the group's coordinator partition. We redirect back to the
     * group page on every path (success or failure) so the re-rendered table
     * shows the authoritative post-reset state.
     */
    @PostMapping("/ui/groups/{id}/reset-offsets")
    public String resetOffsets(
            @PathVariable("id") String id,
            @RequestParam String topic,
            @RequestParam int partition,
            @RequestParam long offset,
            @RequestParam(defaultValue = "0") int leaderEpoch) {
        var reset = OffsetReset.newBuilder()
                .setTp(TopicPartition.newBuilder()
                        .setTopic(topic)
                        .setPartition(partition)
                        .build())
                .setOffset(offset)
                .setLeaderEpoch(leaderEpoch)
                .build();
        List<OffsetReset> resets = List.of(reset);
        for (BrokerAdminClient c : pool.clients()) {
            try {
                var resp = c.resetConsumerGroupOffsets(id, resets);
                if (resp.getError() == ErrorCode.OK) {
                    break;
                }
            } catch (StatusRuntimeException e) {
                log.debug("broker {} failed resetConsumerGroupOffsets({}): {}", c.address(), id, e.getStatus());
            }
        }
        return "redirect:/groups/" + id;
    }

    private ConsumerGroupDetail toDetail(DescribeConsumerGroupResponse r) {
        var members = new ArrayList<ConsumerGroupDetail.Member>();
        for (var m : r.getMembersList()) {
            var owned = new ArrayList<ConsumerGroupDetail.OwnedPartition>();
            for (var tps : m.getOwnedPartitionsList()) {
                owned.add(new ConsumerGroupDetail.OwnedPartition(tps.getTopic(), tps.getPartitionsList()));
            }
            members.add(new ConsumerGroupDetail.Member(
                    m.getMemberId(), m.getInstanceId(), m.getMemberEpoch(), m.getSubscribedTopicsList(), owned));
        }
        var parts = new ArrayList<ConsumerGroupDetail.PartitionLag>();
        for (var pl : r.getPartitionsList()) {
            parts.add(new ConsumerGroupDetail.PartitionLag(
                    pl.getTp().getTopic(),
                    pl.getTp().getPartition(),
                    pl.getCommittedOffset(),
                    pl.getHighWatermark(),
                    pl.getLag(),
                    pl.getOwnerMemberId()));
        }
        return new ConsumerGroupDetail(
                r.getGroupId(), r.getState(), r.getGeneration(), r.getAssignor(), members, parts);
    }
}
