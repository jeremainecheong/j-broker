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

    private DescribeTopicPartitionsResponse fetchTopicPartitions(String name) {
        var resp = pool.firstSuccessful(c -> c.describeTopicPartitions(name));
        if (resp.getError() == jbroker.proto.common.ErrorCode.UNKNOWN) return null;
        return resp;
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
