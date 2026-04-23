package jbroker.admin.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import jbroker.proto.broker.BrokerInfo;
import jbroker.proto.broker.DescribeClusterResponse;
import jbroker.proto.common.ErrorCode;
import org.junit.jupiter.api.Test;

/**
 * P11.9 — covers {@link ClusterController#mergeSelfReportedRoles}. Each
 * broker only knows its own role authoritatively; everything else it sees
 * via heartbeat state is reported as {@code UNKNOWN}. Admin-app fans out
 * and overlays concrete self-reports from every responder.
 */
final class ClusterMergeRolesTest {

    private static BrokerInfo node(int id, String role) {
        return BrokerInfo.newBuilder()
                .setBrokerId(id)
                .setHost("broker" + id)
                .setPort(9092)
                .setRole(role)
                .setAlive(true)
                .build();
    }

    private static DescribeClusterResponse resp(int controllerId, BrokerInfo... nodes) {
        var b = DescribeClusterResponse.newBuilder().setError(ErrorCode.OK).setControllerId(controllerId);
        for (var n : nodes) b.addNodes(n);
        return b.build();
    }

    @Test
    void eachBrokerContributesItsOwnRole() {
        // broker1 sees self=FOLLOWER, broker2/3=UNKNOWN
        var r1 = resp(3, node(1, "FOLLOWER"), node(2, "UNKNOWN"), node(3, "UNKNOWN"));
        // broker2 sees self=FOLLOWER, rest=UNKNOWN
        var r2 = resp(3, node(1, "UNKNOWN"), node(2, "FOLLOWER"), node(3, "UNKNOWN"));
        // broker3 (the controller) sees self=LEADER, rest=UNKNOWN
        var r3 = resp(3, node(1, "UNKNOWN"), node(2, "UNKNOWN"), node(3, "LEADER"));

        var merged = ClusterController.mergeSelfReportedRoles(List.of(r1, r2, r3));

        assertThat(merged.getNodesList()).extracting(BrokerInfo::getRole)
                .containsExactly("FOLLOWER", "FOLLOWER", "LEADER");
    }

    @Test
    void primaryIsFirstOkResponseEvenIfLaterResponsesHaveMoreConcreteRoles() {
        // Errors ignored entirely: primary is the first OK, merge draws from
        // every OK.
        var err = DescribeClusterResponse.newBuilder().setError(ErrorCode.UNKNOWN).build();
        var r1 = resp(2, node(1, "FOLLOWER"), node(2, "UNKNOWN"));
        var r2 = resp(2, node(1, "UNKNOWN"), node(2, "LEADER"));

        var merged = ClusterController.mergeSelfReportedRoles(List.of(err, r1, r2));
        assertThat(merged.getNodesList()).extracting(BrokerInfo::getRole)
                .containsExactly("FOLLOWER", "LEADER");
    }

    @Test
    void unknownRolesDoNotOverwriteConcreteOnes() {
        // If the first OK response happens to have a concrete role for a
        // peer (shouldn't happen in practice, but defensive), a later
        // UNKNOWN mustn't revert it.
        var r1 = resp(1, node(1, "LEADER"), node(2, "FOLLOWER"));
        var r2 = resp(1, node(1, "UNKNOWN"), node(2, "UNKNOWN"));

        var merged = ClusterController.mergeSelfReportedRoles(List.of(r1, r2));
        assertThat(merged.getNodesList()).extracting(BrokerInfo::getRole)
                .containsExactly("LEADER", "FOLLOWER");
    }

    @Test
    void allResponsesUnknownLeavesPrimaryUntouched() {
        var r1 = resp(-1, node(1, "UNKNOWN"), node(2, "UNKNOWN"));
        var r2 = resp(-1, node(1, "UNKNOWN"), node(2, "UNKNOWN"));
        var merged = ClusterController.mergeSelfReportedRoles(List.of(r1, r2));
        assertThat(merged.getNodesList()).extracting(BrokerInfo::getRole)
                .containsExactly("UNKNOWN", "UNKNOWN");
    }
}
