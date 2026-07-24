package jbroker.admin.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.List;
import jbroker.admin.client.BrokerAdminClientPool;
import jbroker.admin.dto.MembershipView;
import jbroker.admin.dto.RestError;
import jbroker.broker.ErrorCodes;
import jbroker.proto.broker.AddBrokerResponse;
import jbroker.proto.broker.CancelReassignmentResponse;
import jbroker.proto.broker.DecommissionBrokerResponse;
import jbroker.proto.broker.DescribeMembershipResponse;
import jbroker.proto.broker.ListReassignmentsResponse;
import jbroker.proto.broker.ReassignPartitionResponse;
import jbroker.proto.broker.ReassignmentInfo;
import jbroker.proto.broker.RebalanceLeadershipResponse;
import org.junit.jupiter.api.Test;

/**
 * Status-mapping contract of {@code /api/v1/cluster/*} with the gRPC layer
 * stubbed out: broker refusals must keep their message and land on the
 * right HTTP status — NOT_LEADER and REASSIGNMENT_IN_PROGRESS as 409
 * (retriable elsewhere/later, hints intact), validation refusals as 400.
 * The hint-following routing itself is pinned by
 * {@code jbroker.admin.client.BrokerAdminClientPoolHintFollowIT}; the
 * real-broker round trip by {@code ClusterOpsEndpointIT}.
 */
final class ClusterLifecycleControllerTest {

    private final BrokerAdminClientPool pool = mock(BrokerAdminClientPool.class);
    private final ClusterLifecycleController controller = new ClusterLifecycleController(pool);

    private static jbroker.proto.broker.Error error(int code, String message) {
        return jbroker.proto.broker.Error.newBuilder()
                .setCode(code)
                .setMessage(message)
                .build();
    }

    @Test
    void membershipMapsProtoOntoView() {
        var proto = DescribeMembershipResponse.newBuilder()
                .addAllVoterIds(List.of(1, 2, 3))
                .setJoinPhase("CATCHING_UP")
                .setJoinBrokerId(4)
                .setJoinLag(12)
                .setDecommissionPhase("IDLE")
                .build();
        doReturn(proto).when(pool).controllerRouted(any(), any());

        var resp = controller.membership();

        assertThat(resp.getStatusCode().value()).isEqualTo(200);
        var view = (MembershipView) resp.getBody();
        assertThat(view).isNotNull();
        assertThat(view.voterIds()).containsExactly(1, 2, 3);
        assertThat(view.join().phase()).isEqualTo("CATCHING_UP");
        assertThat(view.join().brokerId()).isEqualTo(4);
        assertThat(view.join().lag()).isEqualTo(12);
        assertThat(view.join().active()).isTrue();
        assertThat(view.decommission().active()).isFalse();
    }

    @Test
    void addBrokerAcceptedWhenControllerCommits() {
        doReturn(AddBrokerResponse.getDefaultInstance()).when(pool).controllerRouted(any(), any());

        var resp = controller.addBroker(new ClusterLifecycleController.AddBrokerBody(4, "broker-4", 7001, 9092));

        assertThat(resp.getStatusCode().value()).isEqualTo(202);
    }

    @Test
    void decommissionAcceptedWhenControllerCommits() {
        doReturn(DecommissionBrokerResponse.getDefaultInstance()).when(pool).controllerRouted(any(), any());

        assertThat(controller.decommission(4).getStatusCode().value()).isEqualTo(202);
    }

    @Test
    void inProgressRefusalIsConflictWithBrokerMessage() {
        var refusal = ReassignPartitionResponse.newBuilder()
                .setError(error(
                        ErrorCodes.REASSIGNMENT_IN_PROGRESS,
                        "a reassignment for orders-0 is already in flight — cancel it or wait"))
                .build();
        doReturn(refusal).when(pool).controllerRouted(any(), any());

        var resp = controller.reassign(new ClusterLifecycleController.ReassignBody("orders", 0, List.of(1, 2)));

        assertThat(resp.getStatusCode().value()).isEqualTo(409);
        var body = (RestError) resp.getBody();
        assertThat(body).isNotNull();
        assertThat(body.errorCode()).isEqualTo("REASSIGNMENT_IN_PROGRESS");
        assertThat(body.message()).contains("already in flight");
    }

    @Test
    void validationRefusalIsBadRequestWithBrokerMessage() {
        var refusal = ReassignPartitionResponse.newBuilder()
                .setError(error(ErrorCodes.INVALID_CONFIG, "target broker 99 is not registered"))
                .build();
        doReturn(refusal).when(pool).controllerRouted(any(), any());

        var resp = controller.reassign(new ClusterLifecycleController.ReassignBody("orders", 0, List.of(99)));

        assertThat(resp.getStatusCode().value()).isEqualTo(400);
        var body = (RestError) resp.getBody();
        assertThat(body).isNotNull();
        assertThat(body.errorCode()).isEqualTo("INVALID_CONFIG");
        assertThat(body.message()).contains("99 is not registered");
    }

    @Test
    void notLeaderEverywhereIsConflictAndKeepsTheHint() {
        // What controllerRouted returns when every broker answered
        // NOT_LEADER: the last refusal, hints intact.
        var refusal = DecommissionBrokerResponse.newBuilder()
                .setError(jbroker.proto.broker.Error.newBuilder()
                        .setCode(ErrorCodes.NOT_LEADER)
                        .setMessage("not the controller")
                        .putHint("suggested_leader_id", "2")
                        .putHint("suggested_leader_host", "broker-2")
                        .putHint("suggested_leader_port", "9092"))
                .build();
        doReturn(refusal).when(pool).controllerRouted(any(), any());

        var resp = controller.decommission(3);

        assertThat(resp.getStatusCode().value()).isEqualTo(409);
        var body = (RestError) resp.getBody();
        assertThat(body).isNotNull();
        assertThat(body.errorCode()).isEqualTo("NOT_LEADER");
        assertThat(body.hint())
                .containsEntry("suggested_leader_id", "2")
                .containsEntry("suggested_leader_host", "broker-2")
                .containsEntry("suggested_leader_port", "9092");
    }

    @Test
    void cancelWithoutPendingEntryIsBadRequest() {
        var refusal = CancelReassignmentResponse.newBuilder()
                .setError(error(ErrorCodes.INVALID_CONFIG, "no reassignment pending for orders-0"))
                .build();
        doReturn(refusal).when(pool).controllerRouted(any(), any());

        var resp = controller.cancelReassignment("orders", 0);

        assertThat(resp.getStatusCode().value()).isEqualTo(400);
        assertThat(((RestError) resp.getBody()).message()).contains("no reassignment pending");
    }

    @Test
    void rebalanceReportsMovesProposed() {
        doReturn(RebalanceLeadershipResponse.newBuilder().setMovesProposed(3).build())
                .when(pool)
                .controllerRouted(any(), any());

        var resp = controller.rebalanceLeadership();

        assertThat(resp.getStatusCode().value()).isEqualTo(200);
        assertThat(resp.getBody()).isEqualTo(new ClusterLifecycleController.RebalanceResult(3));
    }

    @Test
    void reassignmentsListMapsReplicatedCache() {
        var listed = ListReassignmentsResponse.newBuilder()
                .addReassignments(ReassignmentInfo.newBuilder()
                        .setTopic("orders")
                        .setPartition(0)
                        .addAllTargetReplicas(List.of(2, 3))
                        .addAllOriginalReplicas(List.of(1, 2)))
                .build();
        doReturn(listed).when(pool).firstSuccessful(any());

        var resp = controller.reassignments();

        assertThat(resp.getStatusCode().value()).isEqualTo(200);
        assertThat(resp.getBody())
                .isEqualTo(List.of(new jbroker.admin.dto.ReassignmentView("orders", 0, List.of(2, 3), List.of(1, 2))));
    }
}
