package jbroker.admin.ui;

import io.grpc.StatusRuntimeException;
import jakarta.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import jbroker.admin.client.BrokerAdminClientPool;
import jbroker.admin.dto.ConsumerGroupDetail;
import jbroker.admin.dto.ConsumerGroupSummary;
import jbroker.proto.broker.DescribeConsumerGroupResponse;
import jbroker.proto.common.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

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
