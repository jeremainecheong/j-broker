package jbroker.admin.dto;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

final class HealthBadgeTest {

    private static NodeInfo node(int id, boolean alive, String role) {
        return new NodeInfo(id, "broker" + id, 9092, role, alive, 0L);
    }

    @Test
    void greenWhenAllAliveAndControllerKnown() {
        var cluster = new ClusterSummary(
                1, 5L, 0L, List.of(node(1, true, "LEADER"), node(2, true, "FOLLOWER"), node(3, true, "FOLLOWER")));
        var badge = HealthBadge.from(cluster);
        assertThat(badge.status()).isEqualTo("green");
        assertThat(badge.reason()).contains("3/3").contains("controller 1");
    }

    @Test
    void yellowWhenMajorityUpButAtLeastOneBrokerDown() {
        var cluster = new ClusterSummary(
                1, 5L, 0L, List.of(node(1, true, "LEADER"), node(2, true, "FOLLOWER"), node(3, false, "UNKNOWN")));
        var badge = HealthBadge.from(cluster);
        assertThat(badge.status()).isEqualTo("yellow");
        assertThat(badge.reason()).contains("2/3");
    }

    @Test
    void redWhenAliveCountBelowQuorum() {
        var cluster = new ClusterSummary(
                1, 5L, 0L, List.of(node(1, true, "LEADER"), node(2, false, "UNKNOWN"), node(3, false, "UNKNOWN")));
        var badge = HealthBadge.from(cluster);
        assertThat(badge.status()).isEqualTo("red");
        assertThat(badge.reason()).contains("majority");
    }

    @Test
    void redWhenControllerIdNotYetKnown() {
        var cluster = new ClusterSummary(
                -1, 0L, 0L, List.of(node(1, true, "UNKNOWN"), node(2, true, "UNKNOWN"), node(3, true, "UNKNOWN")));
        var badge = HealthBadge.from(cluster);
        assertThat(badge.status()).isEqualTo("red");
        assertThat(badge.reason()).contains("no Raft leader");
    }

    @Test
    void redWhenNoNodesAtAll() {
        var badge = HealthBadge.from(new ClusterSummary(-1, 0L, 0L, List.of()));
        assertThat(badge.status()).isEqualTo("red");
        assertThat(badge.reason()).isEqualTo("no brokers registered");
    }
}
