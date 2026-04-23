package jbroker.broker.controller;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import jbroker.broker.TopicManager;
import org.junit.jupiter.api.Test;

final class PreferredLeaderBalancerTest {

    @Test
    void proposesMoveToPreferredWhenInIsr() {
        var tm = new TopicManager();
        tm.onPartitionChange("orders", 0, /*leader*/ 2, List.of(1, 2, 3), List.of(1, 2, 3), 0, 0);
        var balancer = new PreferredLeaderBalancer(tm, () -> true, () -> 0L, 0L);
        var proposals = balancer.proposeRebalances(10_000L);
        assertThat(proposals).hasSize(1);
        assertThat(proposals.get(0).newLeader()).isEqualTo(1);
        assertThat(proposals.get(0).leaderEpoch()).isEqualTo(1);
    }

    @Test
    void skipsPartitionAlreadyOnPreferred() {
        var tm = new TopicManager();
        tm.onPartitionChange("orders", 0, 1, List.of(1, 2, 3), List.of(1, 2, 3), 0, 0);
        var balancer = new PreferredLeaderBalancer(tm, () -> true, () -> 0L, 0L);
        assertThat(balancer.proposeRebalances(10_000L)).isEmpty();
    }

    @Test
    void skipsWhenPreferredOutOfIsr() {
        var tm = new TopicManager();
        tm.onPartitionChange("orders", 0, 2, List.of(2, 3), List.of(1, 2, 3), 0, 0);
        var balancer = new PreferredLeaderBalancer(tm, () -> true, () -> 0L, 0L);
        assertThat(balancer.proposeRebalances(10_000L)).isEmpty();
    }

    @Test
    void skipsWhenNotActiveController() {
        var tm = new TopicManager();
        tm.onPartitionChange("orders", 0, 2, List.of(1, 2, 3), List.of(1, 2, 3), 0, 0);
        var balancer = new PreferredLeaderBalancer(tm, () -> false, () -> 0L, 0L);
        assertThat(balancer.proposeRebalances(10_000L)).isEmpty();
    }

    @Test
    void skipsWithinStabilityWindow() {
        var tm = new TopicManager();
        tm.onPartitionChange("orders", 0, 2, List.of(1, 2, 3), List.of(1, 2, 3), 0, 0);
        // Last leader change was 10s ago; stability window is 30s → skip.
        var balancer = new PreferredLeaderBalancer(tm, () -> true, () -> 90_000L, 30_000L);
        assertThat(balancer.proposeRebalances(100_000L)).isEmpty();
    }

    @Test
    void proposesAcrossMultipleImbalancedPartitions() {
        var tm = new TopicManager();
        tm.onPartitionChange("orders", 0, 2, List.of(1, 2, 3), List.of(1, 2, 3), 0, 0);
        tm.onPartitionChange("orders", 1, 3, List.of(1, 2, 3), List.of(2, 1, 3), 0, 0);
        tm.onPartitionChange("orders", 2, 1, List.of(1, 2, 3), List.of(1, 2, 3), 0, 0); // already preferred
        var balancer = new PreferredLeaderBalancer(tm, () -> true, () -> 0L, 0L);
        var proposals = balancer.proposeRebalances(10_000L);
        assertThat(proposals).hasSize(2);
    }
}
