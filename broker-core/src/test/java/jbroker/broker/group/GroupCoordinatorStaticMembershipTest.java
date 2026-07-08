package jbroker.broker.group;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Drills into static-membership semantics relied on so that a consumer
 * restarting with the same instance_id triggers no rebalance:
 * <ul>
 *   <li>Same {@code instance_id} rejoining keeps the same {@code member_id}
 *       and {@code member_epoch}; generation does NOT bump.</li>
 *   <li>Different members under the same {@code instance_id} trigger eviction
 *       of the prior holder (slot-takeover); generation still does NOT bump
 *       in the current implementation — eviction-with-bump is a future polish if
 *       coordinator failover surfaces a need.</li>
 *   <li>Empty {@code instance_id} keeps classic dynamic semantics — every
 *       join allocates a new member_id and bumps generation.</li>
 * </ul>
 */
class GroupCoordinatorStaticMembershipTest {

    private static final long SESSION_TIMEOUT_MS = 1_000L;
    private static final int REBALANCE_TIMEOUT_MS = 10_000;
    private static final long MS = 1_000_000L;

    @Test
    void staticRejoinKeepsMemberIdAndEpochAndDoesNotBumpGeneration() {
        var coord = newCoord(java.util.Map.of("orders", 4));
        var first = coord.join("g1", "instance-A", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        int genBefore = coord.generationOf("g1");

        // Process restart: same instance_id rejoins.
        var rejoin =
                coord.join("g1", "instance-A", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 100 * MS);

        assertThat(rejoin.memberId()).isEqualTo(first.memberId());
        assertThat(rejoin.memberEpoch()).isEqualTo(first.memberEpoch());
        assertThat(coord.generationOf("g1")).isEqualTo(genBefore);
        assertThat(coord.memberCountOf("g1")).isEqualTo(1);
    }

    @Test
    void staticRejoinPreservesAssignment() {
        var coord = newCoord(java.util.Map.of("orders", 4));
        var first = coord.join("g1", "instance-A", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        var rejoin =
                coord.join("g1", "instance-A", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 100 * MS);

        // Same instance_id → same assignment surface.
        assertThat(rejoin.assignment()).isEqualTo(first.assignment());
    }

    @Test
    void emptyInstanceIdKeepsDynamicSemantics() {
        var coord = newCoord(java.util.Map.of("orders", 4));
        var first = coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        int genBefore = coord.generationOf("g1");

        // Second join with empty instance_id — fresh member_id, generation bumps.
        var second = coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 100 * MS);

        assertThat(second.memberId()).isNotEqualTo(first.memberId());
        assertThat(coord.generationOf("g1")).isEqualTo(genBefore + 1);
        assertThat(coord.memberCountOf("g1")).isEqualTo(2);
    }

    @Test
    void multipleStaticMembersCoexist() {
        var coord = newCoord(java.util.Map.of("orders", 6));
        var a = coord.join("g1", "instance-A", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        var b = coord.join("g1", "instance-B", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 100 * MS);

        assertThat(a.memberId()).isNotEqualTo(b.memberId());
        // Each rejoin keeps its slot.
        var aRejoin =
                coord.join("g1", "instance-A", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 200 * MS);
        var bRejoin =
                coord.join("g1", "instance-B", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 300 * MS);
        assertThat(aRejoin.memberId()).isEqualTo(a.memberId());
        assertThat(bRejoin.memberId()).isEqualTo(b.memberId());
    }

    @Test
    void staticRejoinUpdatesSubscriptionWithoutBumpingGeneration() {
        var coord = newCoord(java.util.Map.of("orders", 4, "alerts", 2));
        coord.join("g1", "instance-A", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        int genBefore = coord.generationOf("g1");

        // Same instance_id but expanded subscription. Current behavior: the
        // subscription is captured but the assignor doesn't re-fire on
        // static rejoin (assignment only changes when generation bumps,
        // which happens for new dynamic joins / leaves / evictions).
        // The assertion locks in this behaviour so future changes are
        // intentional.
        coord.join("g1", "instance-A", Set.of("orders", "alerts"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 100 * MS);
        assertThat(coord.generationOf("g1")).isEqualTo(genBefore);
    }

    private static GroupCoordinator newCoord(java.util.Map<String, Integer> topicPartitions) {
        var counter = new AtomicInteger();
        return new GroupCoordinator(
                topic -> topicPartitions.getOrDefault(topic, 0),
                new RangeAssignor(),
                instanceId -> "member-" + counter.incrementAndGet());
    }
}
