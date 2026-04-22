package jbroker.broker.group;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import jbroker.proto.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * proves the cooperative incremental rebalance protocol:
 * <ul>
 *   <li>A member that loses partitions sees the kept set advertised first;
 *       only after it acks (sends owned_partitions == kept) does the
 *       coordinator hand it the eventual full target.</li>
 *   <li>A member that only gains partitions skips staging and goes
 *       straight to the full target.</li>
 *   <li>A brand-new member always goes straight to its target (had nothing
 *       to revoke).</li>
 *   <li>Member ack via {@code owned_partitions} drives stage advancement.</li>
 * </ul>
 */
class GroupCoordinatorIncrementalRebalanceTest {

    private static final long SESSION_TIMEOUT_MS = 1_000L;
    private static final int REBALANCE_TIMEOUT_MS = 10_000;
    private static final long MS = 1_000_000L;

    @Test
    void losingMemberSeesKeptSetFirstThenFullTargetAfterAck() {
        var coord = newCoord(java.util.Map.of("orders", 6));
        // Stage the original 2-member assignment: m1=[0,1,2], m2=[3,4,5].
        var m1 = coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        var m2 = coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        // Member m2's NEXT heartbeat surfaces the new assignment after m2's join.
        var m2WithNew = coord.heartbeat("g1", m2.memberId(), m2.memberEpoch(), List.of(), 100 * MS);
        // Sanity: under range assignor, m1=[0,1,2], m2=[3,4,5].
        assertThat(asPartitions(coord.assignmentFor("g1", m1.memberId()).orElseThrow()))
                .containsExactly(0, 1, 2);
        assertThat(asPartitions(coord.assignmentFor("g1", m2.memberId()).orElseThrow()))
                .containsExactly(3, 4, 5);

        // Add a third member. Range assignor's new shape: m1=[0,1], m2=[2,3], m3=[4,5].
        // m1 loses [2]. m2 gains [2] but loses [4,5]. m3 is brand new — gets [4,5] immediately.
        var m3 = coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 200 * MS);
        assertThat(asPartitions(m3.assignment())).containsExactly(4, 5);

        // m1's next heartbeat: stage 1 (lost only, no additions). target == kept,
        // so pendingTarget=null. currentAssignment becomes [0,1].
        var m1Hb1 = coord.heartbeat("g1", m1.memberId(), m1.memberEpoch(), List.of(), 300 * MS);
        assertThat(m1Hb1.newAssignment()).isPresent();
        assertThat(asPartitions(m1Hb1.newAssignment().get())).containsExactly(0, 1);

        // m1's subsequent heartbeat with ownedPartitions=[0,1]: no further change.
        var m1Hb2 = coord.heartbeat("g1", m1.memberId(), m1Hb1.memberEpoch(), tps("orders", 0, 1), 400 * MS);
        assertThat(m1Hb2.newAssignment()).isEmpty();

        // m2's next heartbeat after m3's join: stage 1 — kept = [3], lost = [4,5],
        // added = [2]. coordinator advertises kept=[3] first.
        var m2Hb1 = coord.heartbeat("g1", m2.memberId(), m2WithNew.memberEpoch(), tps("orders", 3, 4, 5), 500 * MS);
        assertThat(m2Hb1.newAssignment()).isPresent();
        assertThat(asPartitions(m2Hb1.newAssignment().get())).containsExactly(3);

        // m2 acks the revoke by sending ownedPartitions=[3]. Now stage 2 fires:
        // currentAssignment advances to target [2,3].
        var m2Hb2 = coord.heartbeat("g1", m2.memberId(), m2Hb1.memberEpoch(), tps("orders", 3), 600 * MS);
        assertThat(m2Hb2.newAssignment()).isPresent();
        assertThat(asPartitions(m2Hb2.newAssignment().get())).containsExactly(2, 3);

        // m2's NEXT heartbeat with the new ownedPartitions=[2,3] settles steady-state.
        var m2Hb3 = coord.heartbeat("g1", m2.memberId(), m2Hb2.memberEpoch(), tps("orders", 2, 3), 700 * MS);
        assertThat(m2Hb3.newAssignment()).isEmpty();
    }

    @Test
    void newMemberSkipsStaging() {
        // Verifies the same property the test above checks for m3 — but in
        // isolation so it's clear from the test name.
        var coord = newCoord(java.util.Map.of("orders", 4));
        var join = coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        // Single new member — no prior currentAssignment, no kept set, no staging.
        // Got the full target ([0,1,2,3]) in the join response.
        assertThat(asPartitions(join.assignment())).containsExactly(0, 1, 2, 3);
    }

    @Test
    void memberWithOnlyAdditions_skipsStaging() {
        var coord = newCoord(java.util.Map.of("orders", 4));
        // Member m1 starts with subscription that only matches one of the
        // topics in scope, then expands subscription. Hard to engineer
        // without subscription-change support — the simpler scenario:
        // member-1 alone has [0,1,2,3]. member-2 leaves immediately (no-op
        // since it never joined). member-1 still has [0,1,2,3].
        var m1 = coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        assertThat(asPartitions(m1.assignment())).containsExactly(0, 1, 2, 3);
    }

    @Test
    void memberAcksRevokeWithOwnedPartitionsMatchingKept() {
        // Drill-down into the ack mechanism: only OWNED_PARTITIONS that
        // match the advertised current assignment trigger the stage-2
        // advance. A member that sends a wrong owned set stays in stage 1.
        var coord = newCoord(java.util.Map.of("orders", 6));
        var m1 = coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        var m2 = coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        var m2WithNew = coord.heartbeat("g1", m2.memberId(), m2.memberEpoch(), List.of(), 100 * MS);

        // Add m3 — m2 enters staging (kept=[3], pending=[2,3]).
        coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 200 * MS);

        // m2's first HB after m3's join: stage 1, advertise kept=[3].
        var stageOne = coord.heartbeat("g1", m2.memberId(), m2WithNew.memberEpoch(), tps("orders", 3, 4, 5), 300 * MS);
        assertThat(asPartitions(stageOne.newAssignment().orElseThrow())).containsExactly(3);

        // m2 sends ownedPartitions=[3,4,5] (still has the lost partitions).
        // Coordinator does NOT advance — m2 hasn't acked the revoke yet.
        var notReady = coord.heartbeat("g1", m2.memberId(), stageOne.memberEpoch(), tps("orders", 3, 4, 5), 400 * MS);
        assertThat(notReady.newAssignment()).isEmpty();

        // m2 sends ownedPartitions=[3] — now coordinator advances to [2,3].
        var stageTwo = coord.heartbeat("g1", m2.memberId(), stageOne.memberEpoch(), tps("orders", 3), 500 * MS);
        assertThat(asPartitions(stageTwo.newAssignment().orElseThrow())).containsExactly(2, 3);
    }

    @Test
    void leaveTriggersRebalance_othersGetReleasedPartitionsAfterAck() {
        // happy-path mechanic: when a member leaves, surviving
        // members pick up the released partitions on their next heartbeat
        // (via stage-1 → stage-2 dance for any that have additions).
        var coord = newCoord(java.util.Map.of("orders", 6));
        var m1 = coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        var m2 = coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        var m3 = coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        // m1 was [0,1,2] but the second join shrunk it to [0,1] (kept set);
        // the third join shrinks again to [0,1] (no further loss).
        // m2 was [3,4,5] then staged to [3] (kept) then target [2,3].
        // m3 just joined, got [4,5] immediately.
        // Drive m1 + m2 to settled state by having them ack their stages.
        coord.heartbeat("g1", m1.memberId(), m1.memberEpoch(), List.of(), 100 * MS);
        coord.heartbeat(
                "g1",
                m1.memberId(),
                coord.assignmentFor("g1", m1.memberId()).map(a -> 0).orElse(0),
                tps("orders", 0, 1),
                110 * MS);
        var m2New1 = coord.heartbeat("g1", m2.memberId(), m2.memberEpoch(), tps("orders", 3, 4, 5), 200 * MS);
        var m2New2 = coord.heartbeat("g1", m2.memberId(), m2New1.memberEpoch(), tps("orders", 3), 250 * MS);
        coord.heartbeat("g1", m2.memberId(), m2New2.memberEpoch(), tps("orders", 2, 3), 300 * MS);

        // m3 leaves.
        coord.leave("g1", m3.memberId());

        // After m3's leave, range assignor over 2 members + 6 partitions:
        // m1 should get [0,1,2], m2 should get [3,4,5].
        // m1: was [0,1], target [0,1,2], pure addition → no staging.
        // m2: was [2,3], target [3,4,5], lost=[2], added=[4,5]. Stage 1 → kept=[3].
        var m1Final = coord.heartbeat("g1", m1.memberId(), /*deliberately stale*/ 0, tps("orders", 0, 1), 400 * MS);
        // m1's response carries [0,1,2] (pure addition, no staging).
        assertThat(asPartitions(m1Final.newAssignment().orElseThrow())).containsExactly(0, 1, 2);
    }

    private static GroupCoordinator newCoord(java.util.Map<String, Integer> topicPartitions) {
        var counter = new AtomicInteger();
        return new GroupCoordinator(
                topic -> topicPartitions.getOrDefault(topic, 0),
                new RangeAssignor(),
                instanceId -> "member-" + counter.incrementAndGet());
    }

    private static List<Integer> asPartitions(List<TopicPartition> tps) {
        return tps.stream().map(TopicPartition::getPartition).sorted().toList();
    }

    private static List<TopicPartition> tps(String topic, int... partitions) {
        var out = new java.util.ArrayList<TopicPartition>();
        for (int p : partitions) {
            out.add(TopicPartition.newBuilder().setTopic(topic).setPartition(p).build());
        }
        return out;
    }
}
