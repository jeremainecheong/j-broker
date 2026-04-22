package jbroker.broker.group;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import jbroker.proto.common.TopicPartition;
import org.junit.jupiter.api.Test;

class GroupCoordinatorTest {

    private static final long SESSION_TIMEOUT_MS = 1_000L;
    private static final int REBALANCE_TIMEOUT_MS = 10_000;
    private static final long MS = 1_000_000L; // 1 ms in nanos

    @Test
    void join_assignsMemberId_andSetsGenerationToOne() {
        var coord = newCoord(Map.of("orders", 6));

        var join = coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, /*nowNs*/ 0L);

        assertThat(join.memberId()).isEqualTo("member-1");
        assertThat(join.generation()).isEqualTo(1);
        assertThat(coord.generationOf("g1")).isEqualTo(1);
        assertThat(join.assignment()).hasSize(6); // single member gets all partitions
        assertThat(join.memberEpoch()).isEqualTo(1); // first assignment bumps from 0 → 1
    }

    @Test
    void secondJoin_addsMember_bumpsGeneration_redistributes() {
        var coord = newCoord(Map.of("orders", 6));

        coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        var second = coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);

        assertThat(coord.generationOf("g1")).isEqualTo(2);
        assertThat(coord.memberCountOf("g1")).isEqualTo(2);
        // Range assignor: member-1 gets [0,1,2], member-2 gets [3,4,5].
        // Sorted member ids matters; ours are member-1, member-2.
        assertThat(second.assignment()).extracting(TopicPartition::getPartition).containsExactly(3, 4, 5);
    }

    @Test
    void heartbeat_steadyState_returnsEmptyAssignment_andEchoesEpoch() {
        var coord = newCoord(Map.of("orders", 4));

        var join = coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        var hb = coord.heartbeat("g1", join.memberId(), join.memberEpoch(), List.of(), 100 * MS);

        assertThat(hb.outcome()).isEqualTo(GroupCoordinator.HeartbeatOutcome.OK);
        assertThat(hb.memberEpoch()).isEqualTo(join.memberEpoch());
        assertThat(hb.newAssignment()).isEmpty();
    }

    @Test
    void heartbeat_afterMembershipChange_returnsNewAssignmentWithBumpedEpoch() {
        var coord = newCoord(Map.of("orders", 6));

        var first = coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        // First member's epoch=1, assignment=all 6.
        coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        // After second join, first member's epoch was bumped to 2 and
        // assignment shrunk to [0,1,2]. The first member's NEXT heartbeat
        // still carries epoch=1, so the coordinator surfaces the new
        // assignment + bumped epoch.
        var hb = coord.heartbeat("g1", first.memberId(), first.memberEpoch(), List.of(), 100 * MS);

        assertThat(hb.outcome()).isEqualTo(GroupCoordinator.HeartbeatOutcome.OK);
        assertThat(hb.memberEpoch()).isEqualTo(2);
        assertThat(hb.newAssignment()).isPresent();
        assertThat(hb.newAssignment().get())
                .extracting(TopicPartition::getPartition)
                .containsExactly(0, 1, 2);
    }

    @Test
    void heartbeat_unknownMember_returnsUnknownMemberId() {
        var coord = newCoord(Map.of("orders", 4));
        var hb = coord.heartbeat("g1", "ghost", 0, List.of(), 0L);
        assertThat(hb.outcome()).isEqualTo(GroupCoordinator.HeartbeatOutcome.UNKNOWN_MEMBER_ID);
    }

    @Test
    void heartbeat_futureEpoch_returnsFencedMemberEpoch() {
        var coord = newCoord(Map.of("orders", 4));
        var join = coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);

        var hb = coord.heartbeat("g1", join.memberId(), join.memberEpoch() + 5, List.of(), 0L);
        assertThat(hb.outcome()).isEqualTo(GroupCoordinator.HeartbeatOutcome.FENCED_MEMBER_EPOCH);
    }

    @Test
    void leave_dropsMember_andBumpsGeneration() {
        var coord = newCoord(Map.of("orders", 6));
        var first = coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        int genBeforeLeave = coord.generationOf("g1");

        coord.leave("g1", first.memberId());

        assertThat(coord.memberCountOf("g1")).isEqualTo(1);
        assertThat(coord.generationOf("g1")).isEqualTo(genBeforeLeave + 1);
        assertThat(coord.assignmentFor("g1", first.memberId())).isEmpty();
    }

    @Test
    void tickEvictions_dropsMembersPastSessionTimeout() {
        var coord = newCoord(Map.of("orders", 4));
        var join = coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);

        // Advance time past sessionTimeoutMs (1s = 1_000_000_000 ns) without
        // sending another heartbeat.
        var evicted = coord.tickEvictions(2_000_000_000L);

        assertThat(evicted).hasSize(1);
        assertThat(evicted.get(0).memberId()).isEqualTo(join.memberId());
        assertThat(coord.memberCountOf("g1")).isZero();
    }

    @Test
    void tickEvictions_doesNotDropFreshMembers() {
        var coord = newCoord(Map.of("orders", 4));
        coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);

        // Within session window — no eviction.
        var evicted = coord.tickEvictions(500 * MS);

        assertThat(evicted).isEmpty();
        assertThat(coord.memberCountOf("g1")).isEqualTo(1);
    }

    @Test
    void uniformAssignor_swappable_viaConstructor() {
        var counter = new AtomicInteger();
        var coord = new GroupCoordinator(
                topic -> Map.of("orders", 6).getOrDefault(topic, 0),
                new UniformAssignor(),
                instanceId -> "member-" + counter.incrementAndGet());

        coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        var second = coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);

        // Round-robin: member-2 gets [1, 3, 5].
        assertThat(second.assignment()).extracting(TopicPartition::getPartition).containsExactly(1, 3, 5);
    }

    @Test
    void staticMembership_secondJoinWithSameInstanceIdReusesSlot() {
        var coord = newCoord(Map.of("orders", 4));
        var first = coord.join("g1", "instance-A", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        int genBefore = coord.generationOf("g1");
        var rejoin =
                coord.join("g1", "instance-A", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 100 * MS);

        assertThat(rejoin.memberId()).isEqualTo(first.memberId());
        assertThat(coord.generationOf("g1")).isEqualTo(genBefore); // no bump
        assertThat(rejoin.memberEpoch()).isEqualTo(first.memberEpoch());
    }

    private static GroupCoordinator newCoord(java.util.Map<String, Integer> topicPartitions) {
        var counter = new AtomicInteger();
        return new GroupCoordinator(
                topic -> topicPartitions.getOrDefault(topic, 0),
                new RangeAssignor(),
                instanceId -> "member-" + counter.incrementAndGet());
    }

    // Imports for inner Map.of used above.
    private static final class Map {
        static java.util.Map<String, Integer> of(String k, int v) {
            return java.util.Map.of(k, v);
        }
    }
}
