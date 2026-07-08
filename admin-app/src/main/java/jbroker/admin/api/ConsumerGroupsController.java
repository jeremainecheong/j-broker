package jbroker.admin.api;

import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import jbroker.admin.client.BrokerAdminClientPool;
import jbroker.admin.dto.ConsumerGroupDetail;
import jbroker.admin.dto.ConsumerGroupSummary;
import jbroker.admin.dto.RestError;
import jbroker.proto.broker.DescribeConsumerGroupResponse;
import jbroker.proto.broker.OffsetReset;
import jbroker.proto.common.ErrorCode;
import jbroker.proto.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * {@code /api/v1/consumer-groups} — consumer-group observability endpoints.
 * Groups are partitioned across {@code __consumer_offsets} partition leaders,
 * so this controller fans out across every configured broker:
 *
 * <ul>
 *   <li>{@code GET /} — merge every broker's known-group set (dedup by id).</li>
 *   <li>{@code GET /{id}} — iterate brokers until one returns OK; brokers
 *       that don't lead the group's coordinator partition return
 *       {@code NOT_COORDINATOR}.</li>
 * </ul>
 *
 * <p>Reset-offsets + delete-group are admin mutations layered on top of
 * this read surface; lag accuracy is the acceptance gate for the reads.
 */
@RestController
@RequestMapping("/api/v1/consumer-groups")
public class ConsumerGroupsController {

    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupsController.class);

    private final BrokerAdminClientPool pool;

    public ConsumerGroupsController(BrokerAdminClientPool pool) {
        this.pool = pool;
    }

    @GetMapping
    public java.util.List<ConsumerGroupSummary> list() {
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
                log.debug("broker {} unreachable for listConsumerGroups: {}", c.address(), e.getStatus());
            }
        }
        return new ArrayList<>(byId.values());
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> describe(@PathVariable("id") String id) {
        DescribeConsumerGroupResponse best = null;
        for (var c : pool.clients()) {
            try {
                var resp = c.describeConsumerGroup(id);
                if (resp.getError() == ErrorCode.OK) {
                    best = resp;
                    break;
                }
                // Keep the last NOT_COORDINATOR response so we can return it
                // with the right routing hint if no broker owns the group.
                best = resp;
            } catch (StatusRuntimeException e) {
                log.debug("broker {} failed describe({}): {}", c.address(), id, e.getStatus());
            }
        }
        if (best == null) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(RestError.of("CLUSTER_UNREACHABLE", "no broker reachable"));
        }
        if (best.getError() != ErrorCode.OK) {
            // No broker owns the group: translate to 404. Empty groups (no
            // members yet) still land on the coordinator broker, so this
            // path means "genuinely unknown".
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(RestError.of("UNKNOWN_GROUP", "unknown or unowned consumer group: " + id));
        }
        return ResponseEntity.ok(toDetail(best));
    }

    /**
     * Admin-initiated group removal. Fans across brokers and picks
     * the one that returns OK (the coordinator); if every broker returns
     * NOT_COORDINATOR we propagate the last response as a 409.
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<?> delete(@PathVariable("id") String id) {
        jbroker.proto.broker.DeleteConsumerGroupResponse last = null;
        for (var c : pool.clients()) {
            try {
                var resp = c.deleteConsumerGroup(id);
                if (resp.getError() == ErrorCode.OK) {
                    return ResponseEntity.noContent().build();
                }
                if (resp.getError() == ErrorCode.UNKNOWN_GROUP) {
                    return ResponseEntity.status(HttpStatus.NOT_FOUND)
                            .body(RestError.of("UNKNOWN_GROUP", "no group: " + id));
                }
                last = resp;
            } catch (StatusRuntimeException e) {
                log.debug("broker {} failed delete({}): {}", c.address(), id, e.getStatus());
            }
        }
        if (last == null) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(RestError.of("CLUSTER_UNREACHABLE", "no broker reachable"));
        }
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(RestError.of("NOT_COORDINATOR", "no broker owns group " + id + "'s coordinator partition"));
    }

    /**
     * Admin-initiated offset reset. Body shape:
     * <pre>{@code
     * {
     *   "resets": [
     *     {"topic": "orders", "partition": 0, "offset": 0, "leader_epoch": 0},
     *     ...
     *   ]
     * }
     * }</pre>
     *
     * <p>Partial-success semantics: the response carries per-tp error
     * codes. HTTP status is 200 if every reset succeeded, 409 if the
     * top-level routing failed (NOT_COORDINATOR / no-coordinator), 503 if
     * no broker reachable.
     */
    @PostMapping("/{id}/reset-offsets")
    public ResponseEntity<?> resetOffsets(@PathVariable("id") String id, @RequestBody ResetOffsetsBody body) {
        if (body == null || body.resets() == null || body.resets().isEmpty()) {
            return ResponseEntity.badRequest().body(RestError.of("INVALID_REQUEST", "resets array must be non-empty"));
        }
        var protoResets = new ArrayList<OffsetReset>(body.resets().size());
        for (var r : body.resets()) {
            protoResets.add(OffsetReset.newBuilder()
                    .setTp(TopicPartition.newBuilder()
                            .setTopic(r.topic())
                            .setPartition(r.partition())
                            .build())
                    .setOffset(r.offset())
                    .setLeaderEpoch(r.leaderEpoch() == null ? 0 : r.leaderEpoch())
                    .build());
        }
        jbroker.proto.broker.ResetConsumerGroupOffsetsResponse last = null;
        for (var c : pool.clients()) {
            try {
                var resp = c.resetConsumerGroupOffsets(id, protoResets);
                if (resp.getError() == ErrorCode.OK) {
                    var results = new ArrayList<OffsetResetResult>();
                    for (var r : resp.getResultsList()) {
                        results.add(new OffsetResetResult(
                                r.getTp().getTopic(),
                                r.getTp().getPartition(),
                                r.getError().name()));
                    }
                    return ResponseEntity.ok(new ResetOffsetsResponse(results));
                }
                last = resp;
            } catch (StatusRuntimeException e) {
                log.debug("broker {} failed reset({}): {}", c.address(), id, e.getStatus());
            }
        }
        if (last == null) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(RestError.of("CLUSTER_UNREACHABLE", "no broker reachable"));
        }
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(RestError.of(last.getError().name(), "could not route reset to a coordinator"));
    }

    public record ResetOffsetsBody(List<OffsetResetEntry> resets) {}

    public record OffsetResetEntry(String topic, int partition, long offset, Integer leaderEpoch) {}

    public record OffsetResetResult(String topic, int partition, String error) {}

    public record ResetOffsetsResponse(List<OffsetResetResult> results) {}

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
