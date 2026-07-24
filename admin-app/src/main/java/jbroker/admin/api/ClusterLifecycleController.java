package jbroker.admin.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import jbroker.admin.client.BrokerAdminClient;
import jbroker.admin.client.BrokerAdminClientPool;
import jbroker.admin.dto.MembershipView;
import jbroker.admin.dto.ReassignmentView;
import jbroker.admin.dto.RestError;
import jbroker.broker.ErrorCodeNames;
import jbroker.broker.ErrorCodes;
import jbroker.proto.broker.DescribeMembershipResponse;
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
 * {@code /api/v1/cluster/*} — the cluster-lifecycle operator surface:
 * membership (join/decommission progress), partition reassignments, and
 * preferred-leader rebalance. Consumed by the overview page's cluster
 * controls and the {@code admin cluster ...} CLI verbs.
 *
 * <p>Everything except {@code GET /reassignments} routes to the controller
 * (the Raft leader) via {@link BrokerAdminClientPool#controllerRouted},
 * which follows a NOT_LEADER answer's {@code suggested_leader_*} hints with
 * one bounded retry before falling back to pool iteration. Mutations answer
 * {@code 202} — joins, drains, and reassignments advance asynchronously on
 * the controller tick; poll {@code GET /membership} / {@code /reassignments}
 * for progress. Broker refusals map onto the envelope statuses:
 * {@code NOT_LEADER} and {@code REASSIGNMENT_IN_PROGRESS} are 409 (retry
 * later / elsewhere), validation refusals are 400, both carrying the
 * broker's message verbatim.
 *
 * <p>Auditing: like every admin mutation, POST/DELETE requests here are
 * logged with the acting principal by {@code AdminAuthFilter} before they
 * reach this controller.
 */
@RestController
@RequestMapping("/api/v1/cluster")
public class ClusterLifecycleController {

    private final BrokerAdminClientPool pool;

    public ClusterLifecycleController(BrokerAdminClientPool pool) {
        this.pool = pool;
    }

    @GetMapping("/membership")
    public ResponseEntity<?> membership() {
        var resp = describeMembership();
        if (resp.getError().getCode() != 0) return errorEnvelope(resp.getError());
        return ResponseEntity.ok(MembershipView.of(resp));
    }

    /**
     * Typed accessor for the overview view controller (same pattern as
     * {@code TopicsController#describeTopic}): empty when no broker is
     * reachable or the controller refuses, so the page can render without
     * the chips instead of erroring out.
     */
    public Optional<MembershipView> membershipView() {
        try {
            var resp = describeMembership();
            return resp.getError().getCode() == 0 ? Optional.of(MembershipView.of(resp)) : Optional.empty();
        } catch (RuntimeException unreachable) {
            return Optional.empty();
        }
    }

    private DescribeMembershipResponse describeMembership() {
        // Join/decommission progress lives on the controller, so even this
        // read is controller-routed.
        return pool.controllerRouted(BrokerAdminClient::describeMembership, DescribeMembershipResponse::getError);
    }

    @PostMapping("/add-broker")
    public ResponseEntity<?> addBroker(@RequestBody @Valid AddBrokerBody body) {
        var resp = pool.controllerRouted(
                c -> c.addBroker(body.brokerId(), body.host(), body.raftPort(), body.brokerPort()),
                jbroker.proto.broker.AddBrokerResponse::getError);
        if (resp.getError().getCode() != 0) return errorEnvelope(resp.getError());
        return ResponseEntity.accepted().build();
    }

    @PostMapping("/decommission/{brokerId}")
    public ResponseEntity<?> decommission(@PathVariable("brokerId") int brokerId) {
        var resp = pool.controllerRouted(
                c -> c.decommissionBroker(brokerId), jbroker.proto.broker.DecommissionBrokerResponse::getError);
        if (resp.getError().getCode() != 0) return errorEnvelope(resp.getError());
        return ResponseEntity.accepted().build();
    }

    @GetMapping("/reassignments")
    public ResponseEntity<?> reassignments() {
        var resp = pool.firstSuccessful(BrokerAdminClient::listReassignments);
        if (resp.getError().getCode() != 0) return errorEnvelope(resp.getError());
        return ResponseEntity.ok(toViews(resp));
    }

    /** Typed accessor for the overview page; empty on any failure. */
    public List<ReassignmentView> reassignmentViews() {
        try {
            var resp = pool.firstSuccessful(BrokerAdminClient::listReassignments);
            return resp.getError().getCode() == 0 ? toViews(resp) : List.of();
        } catch (RuntimeException unreachable) {
            return List.of();
        }
    }

    private static List<ReassignmentView> toViews(jbroker.proto.broker.ListReassignmentsResponse resp) {
        var out = new ArrayList<ReassignmentView>();
        for (var r : resp.getReassignmentsList()) {
            out.add(ReassignmentView.of(r));
        }
        return out;
    }

    @PostMapping("/reassignments")
    public ResponseEntity<?> reassign(@RequestBody @Valid ReassignBody body) {
        var resp = pool.controllerRouted(
                c -> c.reassignPartition(body.topic(), body.partition(), body.replicas()),
                jbroker.proto.broker.ReassignPartitionResponse::getError);
        if (resp.getError().getCode() != 0) return errorEnvelope(resp.getError());
        return ResponseEntity.accepted().build();
    }

    @DeleteMapping("/reassignments/{topic}/{partition}")
    public ResponseEntity<?> cancelReassignment(
            @PathVariable("topic") String topic, @PathVariable("partition") int partition) {
        var resp = pool.controllerRouted(
                c -> c.cancelReassignment(topic, partition), jbroker.proto.broker.CancelReassignmentResponse::getError);
        if (resp.getError().getCode() != 0) return errorEnvelope(resp.getError());
        return ResponseEntity.accepted().build();
    }

    @PostMapping("/rebalance-leadership")
    public ResponseEntity<?> rebalanceLeadership() {
        var resp = pool.controllerRouted(
                BrokerAdminClient::rebalanceLeadership, jbroker.proto.broker.RebalanceLeadershipResponse::getError);
        if (resp.getError().getCode() != 0) return errorEnvelope(resp.getError());
        return ResponseEntity.ok(new RebalanceResult(resp.getMovesProposed()));
    }

    /** {@code moves_proposed = 0} means leadership was already balanced. */
    public record RebalanceResult(int movesProposed) {}

    private ResponseEntity<RestError> errorEnvelope(jbroker.proto.broker.Error err) {
        var status =
                switch (err.getCode()) {
                        // Retriable routing/serialization refusals: NOT_LEADER
                        // propagates the suggested-leader hint; IN_PROGRESS
                        // means try again once the in-flight operation settles.
                    case ErrorCodes.NOT_LEADER, ErrorCodes.REASSIGNMENT_IN_PROGRESS -> HttpStatus.CONFLICT;
                    default -> HttpStatus.BAD_REQUEST;
                };
        var body = new RestError(
                ErrorCodeNames.name(err.getCode()), err.getMessage(), java.util.Map.copyOf(err.getHintMap()));
        return ResponseEntity.status(status).body(body);
    }

    public record AddBrokerBody(
            @Min(1) int brokerId, @NotBlank String host, @Min(1) int raftPort, @Min(1) int brokerPort) {}

    public record ReassignBody(@NotBlank String topic, @Min(0) int partition, @NotEmpty List<Integer> replicas) {}
}
