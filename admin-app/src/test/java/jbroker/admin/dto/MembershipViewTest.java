package jbroker.admin.dto;

import static org.assertj.core.api.Assertions.assertThat;

import jbroker.proto.broker.DescribeMembershipResponse;
import org.junit.jupiter.api.Test;

/**
 * The chip-visibility contract the overview page renders from:
 * {@code IDLE} and {@code DONE} are quiet, everything else (in-flight
 * phases and failures) demands operator attention.
 */
final class MembershipViewTest {

    @Test
    void quietPhasesAreNotActive() {
        assertThat(new MembershipView.Join("IDLE", 0, -1).active()).isFalse();
        assertThat(new MembershipView.Join("DONE", 4, 0).active()).isFalse();
        assertThat(new MembershipView.Join("", 0, -1).active()).isFalse();
        assertThat(new MembershipView.Decommission("IDLE", 0, 0, "").active()).isFalse();
        assertThat(new MembershipView.Decommission("DONE", 4, 0, "").active()).isFalse();
    }

    @Test
    void inFlightAndFailedPhasesAreActive() {
        assertThat(new MembershipView.Join("CATCHING_UP", 4, 120).active()).isTrue();
        assertThat(new MembershipView.Join("FAILED", 4, -1).active()).isTrue();
        assertThat(new MembershipView.Decommission("DRAINING", 4, 7, "").active())
                .isTrue();
        assertThat(new MembershipView.Decommission("REFUSED", 4, 0, "orders-0 would lose replication factor").active())
                .isTrue();
    }

    @Test
    void ofMapsEveryProtoField() {
        var view = MembershipView.of(DescribeMembershipResponse.newBuilder()
                .addVoterIds(1)
                .addVoterIds(2)
                .setJoinPhase("PROMOTING")
                .setJoinBrokerId(4)
                .setJoinLag(3)
                .setDecommissionPhase("REFUSED")
                .setDecommissionBrokerId(2)
                .setDecommissionRemainingPartitions(5)
                .setDecommissionDetail("orders-0 would lose replication factor")
                .build());

        assertThat(view.voterIds()).containsExactly(1, 2);
        assertThat(view.join()).isEqualTo(new MembershipView.Join("PROMOTING", 4, 3));
        assertThat(view.decommission())
                .isEqualTo(new MembershipView.Decommission("REFUSED", 2, 5, "orders-0 would lose replication factor"));
    }
}
