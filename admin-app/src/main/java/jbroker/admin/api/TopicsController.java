package jbroker.admin.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import java.util.ArrayList;
import java.util.Map;
import jbroker.admin.client.BrokerAdminClient;
import jbroker.admin.client.BrokerAdminClientPool;
import jbroker.admin.dto.RestError;
import jbroker.admin.dto.TopicDetail;
import jbroker.admin.dto.TopicSummary;
import jbroker.broker.ErrorCodeNames;
import jbroker.broker.ErrorCodes;
import jbroker.proto.broker.DescribeTopicPartitionsResponse;
import jbroker.proto.broker.PartitionStateInfo;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * {@code /api/v1/topics} — topic CRUD consumed by the topic list / detail UI
 * () and the {@code admin topics ...} CLI ().
 *
 * <p>Mutating operations (POST/DELETE/PATCH) route to the Raft leader via
 * {@link BrokerAdminClientPool#firstNonNotLeader} — a non-leader broker
 * responds with {@code NOT_LEADER} + suggested-leader hints the pool uses
 * to iterate. Reads are first-success.
 */
@RestController
@RequestMapping("/api/v1/topics")
public class TopicsController {

    private final BrokerAdminClientPool pool;

    public TopicsController(BrokerAdminClientPool pool) {
        this.pool = pool;
    }

    @GetMapping
    public java.util.List<TopicSummary> list() {
        var resp = pool.firstSuccessful(BrokerAdminClient::listTopics);
        var out = new ArrayList<TopicSummary>();
        for (var t : resp.getTopicsList()) {
            out.add(TopicSummary.of(t));
        }
        return out;
    }

    @GetMapping("/{name}")
    public ResponseEntity<?> describe(@PathVariable("name") String name) {
        var resp = fetchTopicPartitions(name);
        if (resp == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(RestError.of("UNKNOWN_TOPIC", "unknown topic: " + name));
        }
        return ResponseEntity.ok(toDetail(resp));
    }

    @GetMapping("/{name}/partitions/{p}")
    public ResponseEntity<?> partition(@PathVariable("name") String name, @PathVariable("p") int p) {
        var resp = fetchTopicPartitions(name);
        if (resp == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(RestError.of("UNKNOWN_TOPIC", "unknown topic: " + name));
        }
        for (var ps : resp.getPartitionStatesList()) {
            if (ps.getPartition() == p) {
                return ResponseEntity.ok(toPartitionState(ps));
            }
        }
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(RestError.of("UNKNOWN_PARTITION", "topic " + name + " has no partition " + p));
    }

    @PostMapping
    public ResponseEntity<?> create(@RequestBody @Valid CreateTopicBody body) {
        var resp = pool.firstNonNotLeader(
                c -> c.createTopic(
                        body.name(),
                        body.partitions(),
                        body.replicationFactor(),
                        body.config() == null ? Map.of() : body.config()),
                r -> r.hasError() ? r.getError().getCode() : 0,
                ErrorCodes.NOT_LEADER);
        if (resp.hasError() && resp.getError().getCode() != 0) {
            return errorEnvelope(resp.getError());
        }
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<?> delete(@PathVariable("name") String name) {
        var resp = pool.firstNonNotLeader(
                c -> c.deleteTopic(name), r -> r.hasError() ? r.getError().getCode() : 0, ErrorCodes.NOT_LEADER);
        if (resp.hasError() && resp.getError().getCode() != 0) {
            return errorEnvelope(resp.getError());
        }
        return ResponseEntity.noContent().build();
    }

    /**
     * force synchronous compaction on every replica of
     * {@code (topic, partition)}. Fans out to every broker; each returns its
     * local survivor count, or {@code -1} if it doesn't host the partition.
     * Aggregated response reports total survivors summed across hosting
     * replicas plus the count of brokers that actually compacted.
     *
     * <p>404 when no broker hosts the partition (or every broker returned
     * UNKNOWN_TOPIC / INVALID_PARTITION). The metadata plane is Raft-
     * replicated so an UNKNOWN_TOPIC reply from one broker means the topic
     * really doesn't exist.
     */
    @PostMapping("/{name}/partitions/{p}/compact")
    public ResponseEntity<?> forceCompact(@PathVariable("name") String name, @PathVariable("p") int p) {
        java.util.List<jbroker.proto.broker.ForceCompactPartitionResponse> responses;
        try {
            responses = pool.allSuccessful(c -> c.forceCompactPartition(name, p));
        } catch (IllegalStateException allUnreachable) {
            throw allUnreachable;
        }
        jbroker.proto.broker.Error firstError = null;
        int keptTotal = 0;
        int hostingReplicas = 0;
        for (var r : responses) {
            if (r.hasError() && r.getError().getCode() != 0) {
                if (firstError == null) firstError = r.getError();
                continue;
            }
            if (r.getRecordsKept() >= 0) {
                keptTotal += r.getRecordsKept();
                hostingReplicas++;
            }
        }
        if (hostingReplicas == 0) {
            if (firstError != null) {
                return errorEnvelope(firstError);
            }
            // All responses OK but no broker hosts the partition — partition
            // has no warm log on any replica. Shouldn't happen in practice
            // unless the partition has literally never been touched.
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(RestError.of(
                            "NO_REPLICA_HOSTS_PARTITION",
                            "no broker has an open log for " + name + "/" + p));
        }
        return ResponseEntity.ok(new CompactResult(keptTotal, hostingReplicas));
    }

    public record CompactResult(int recordsKept, int brokersCompacted) {}

    @PatchMapping("/{name}/config")
    public ResponseEntity<?> updateConfig(@PathVariable("name") String name, @RequestBody Map<String, String> overlay) {
        var resp = pool.firstNonNotLeader(
                c -> c.updateTopicConfig(name, overlay == null ? Map.of() : overlay),
                r -> r.hasError() ? r.getError().getCode() : 0,
                ErrorCodes.NOT_LEADER);
        if (resp.hasError() && resp.getError().getCode() != 0) {
            return errorEnvelope(resp.getError());
        }
        return ResponseEntity.ok(resp.getConfigMap());
    }

    /**
     * fans DescribeTopicPartitions out to every broker and merges the
     * per-partition state by overlaying whichever broker actually leads each
     * partition (and therefore reports real {@code highWatermark} /
     * {@code logEndOffset}; followers set sentinel -1).
     *
     * <p>Non-leader fields (leader id, ISR, replicas, epochs) are identical
     * across replicas since the metadata plane is a single Raft state
     * machine, so we seed from the first successful response and only
     * replace the two offset fields.
     */
    private DescribeTopicPartitionsResponse fetchTopicPartitions(String name) {
        java.util.List<DescribeTopicPartitionsResponse> responses;
        try {
            responses = pool.allSuccessful(c -> c.describeTopicPartitions(name));
        } catch (IllegalStateException allUnreachable) {
            throw allUnreachable;
        }
        DescribeTopicPartitionsResponse primary = null;
        for (var r : responses) {
            if (r.getError() == jbroker.proto.common.ErrorCode.OK) {
                primary = r;
                break;
            }
        }
        if (primary == null) {
            // Every broker said UNKNOWN — topic really doesn't exist.
            return null;
        }
        return mergeLeaderReportedOffsets(primary, responses);
    }

    static DescribeTopicPartitionsResponse mergeLeaderReportedOffsets(
            DescribeTopicPartitionsResponse primary, java.util.List<DescribeTopicPartitionsResponse> all) {
        // For each partition, if any response reports a non-sentinel
        // highWatermark (≥ 0), it came from the leader — use it.
        var partitionToOffsets = new java.util.HashMap<Integer, long[]>();
        for (var r : all) {
            if (r.getError() != jbroker.proto.common.ErrorCode.OK) continue;
            for (var ps : r.getPartitionStatesList()) {
                if (ps.getHighWatermark() >= 0 || ps.getLogEndOffset() >= 0) {
                    partitionToOffsets.put(ps.getPartition(), new long[] {ps.getHighWatermark(), ps.getLogEndOffset()});
                }
            }
        }
        if (partitionToOffsets.isEmpty()) {
            // No broker reported real offsets — either the partition has no
            // leader yet, or every leader is unreachable. Return primary
            // untouched so callers still see leader id + ISR.
            return primary;
        }
        var builder = primary.toBuilder();
        var rebuiltStates = new ArrayList<PartitionStateInfo>();
        for (var ps : primary.getPartitionStatesList()) {
            var offsets = partitionToOffsets.get(ps.getPartition());
            if (offsets == null) {
                rebuiltStates.add(ps);
            } else {
                rebuiltStates.add(
                        ps.toBuilder().setHighWatermark(offsets[0]).setLogEndOffset(offsets[1]).build());
            }
        }
        builder.clearPartitionStates();
        for (var ps : rebuiltStates) {
            builder.addPartitionStates(ps);
        }
        return builder.build();
    }

    private ResponseEntity<?> errorEnvelope(jbroker.proto.broker.Error err) {
        var status = err.getCode() == ErrorCodes.NOT_LEADER ? HttpStatus.CONFLICT : HttpStatus.BAD_REQUEST;
        var body = new RestError(
                ErrorCodeNames.name(err.getCode()), err.getMessage(), java.util.Map.copyOf(err.getHintMap()));
        return ResponseEntity.status(status).body(body);
    }

    private TopicDetail toDetail(DescribeTopicPartitionsResponse r) {
        var parts = new ArrayList<TopicDetail.PartitionState>();
        for (var ps : r.getPartitionStatesList()) {
            parts.add(toPartitionState(ps));
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

    private TopicDetail.PartitionState toPartitionState(PartitionStateInfo ps) {
        return new TopicDetail.PartitionState(
                ps.getPartition(),
                ps.getLeader(),
                ps.getIsrList(),
                ps.getReplicasList(),
                ps.getLeaderEpoch(),
                ps.getPartitionEpoch(),
                ps.getHighWatermark(),
                ps.getLogEndOffset());
    }

    public record CreateTopicBody(
            @NotBlank String name, @Min(1) int partitions, @Min(1) int replicationFactor, Map<String, String> config) {}
}
