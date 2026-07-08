package jbroker.broker.group;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import jbroker.broker.ConsumerOffsetsTopic;
import jbroker.proto.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * Proves the GroupCoordinator snapshot/restore round-trip:
 * <ul>
 *   <li>{@link GroupCoordinator#snapshotGroup} captures the current state
 *       of a group in a deterministic, persist-able form.</li>
 *   <li>{@link GroupCoordinator#restoreGroup} rebuilds the in-memory state
 *       from a snapshot byte-for-byte.</li>
 *   <li>The {@link GroupCoordinator.SnapshotListener} fires on every
 *       structural membership change (join, leave, eviction) — but NOT on
 *       static rejoin, steady-state heartbeat, or stage-2 advancement
 *       (those don't change persisted state).</li>
 * </ul>
 */
class GroupCoordinatorSnapshotTest {

    private static final long SESSION_TIMEOUT_MS = 1_000L;
    private static final int REBALANCE_TIMEOUT_MS = 10_000;
    private static final long MS = 1_000_000L;

    @Test
    void snapshotCapturesGenerationAndMembers() {
        var coord = newCoord(java.util.Map.of("orders", 4));
        var join = coord.join("g1", "instance-A", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);

        var snap = coord.snapshotGroup("g1").orElseThrow();
        assertThat(snap.generation()).isEqualTo(1);
        assertThat(snap.members()).hasSize(1);
        var member = snap.members().get(0);
        assertThat(member.memberId()).isEqualTo(join.memberId());
        assertThat(member.instanceId()).isEqualTo("instance-A");
        assertThat(member.memberEpoch()).isEqualTo(join.memberEpoch());
        assertThat(member.subscribedTopics()).containsExactly("orders");
        assertThat(member.assignment()).extracting(TopicPartition::getPartition).containsExactly(0, 1, 2, 3);
    }

    @Test
    void snapshotCodecRoundTripsViaConsumerOffsetsTopic() {
        var coord = newCoord(java.util.Map.of("orders", 6));
        coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 100 * MS);

        var snap = coord.snapshotGroup("g1").orElseThrow();
        byte[] bytes = ConsumerOffsetsTopic.valueForGroupMetadata(snap);
        var decoded = ConsumerOffsetsTopic.decodeGroupMetadataValue(bytes);

        assertThat(decoded.generation()).isEqualTo(snap.generation());
        assertThat(decoded.members()).hasSize(snap.members().size());
        for (int i = 0; i < snap.members().size(); i++) {
            assertThat(decoded.members().get(i).memberId())
                    .isEqualTo(snap.members().get(i).memberId());
            assertThat(decoded.members().get(i).memberEpoch())
                    .isEqualTo(snap.members().get(i).memberEpoch());
        }
    }

    @Test
    void restoreRebuildsState_membersHeartbeatableWithSameId() {
        var sourceCoord = newCoord(java.util.Map.of("orders", 4));
        var join = sourceCoord.join("g1", "instance-A", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        var snap = sourceCoord.snapshotGroup("g1").orElseThrow();

        // Fresh coordinator (simulates failover to a new broker).
        var freshCoord = newCoord(java.util.Map.of("orders", 4));
        freshCoord.restoreGroup("g1", snap, /*nowNs*/ 100 * MS);

        // Fresh coordinator now knows the member — heartbeat with same
        // memberId + epoch returns OK (not UNKNOWN_MEMBER_ID).
        var hb = freshCoord.heartbeat("g1", join.memberId(), join.memberEpoch(), List.of(), 200 * MS);
        assertThat(hb.outcome()).isEqualTo(GroupCoordinator.HeartbeatOutcome.OK);
        // Generation and member count match.
        assertThat(freshCoord.generationOf("g1")).isEqualTo(sourceCoord.generationOf("g1"));
        assertThat(freshCoord.memberCountOf("g1")).isEqualTo(sourceCoord.memberCountOf("g1"));
        assertThat(freshCoord.assignmentFor("g1", join.memberId())).isPresent();
    }

    @Test
    void snapshotListenerFiresOnJoinLeaveEviction_notOnStaticRejoinOrSteadyHeartbeat() {
        var fires = new java.util.concurrent.CopyOnWriteArrayList<String>();
        var coord = newCoord(java.util.Map.of("orders", 4));
        coord.setSnapshotListener((g, s) -> fires.add(g + "/gen=" + s.generation()));

        // Join → fire #1
        var first = coord.join("g1", "instance-A", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        assertThat(fires).containsExactly("g1/gen=1");

        // Static rejoin under same instance_id → no fire (state on disk is still valid).
        coord.join("g1", "instance-A", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 50 * MS);
        assertThat(fires).containsExactly("g1/gen=1");

        // Steady heartbeat → no fire.
        coord.heartbeat("g1", first.memberId(), first.memberEpoch(), List.of(), 100 * MS);
        assertThat(fires).containsExactly("g1/gen=1");

        // Dynamic join → fire #2 (generation 2).
        coord.join("g1", "", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 200 * MS);
        assertThat(fires).hasSize(2);

        // Leave → fire #3.
        coord.leave("g1", first.memberId());
        assertThat(fires).hasSize(3);

        // Eviction (after sessionTimeout) → fire #4.
        coord.tickEvictions(/*nowNs*/ 1_000_000_000_000L);
        assertThat(fires).hasSize(4);
    }

    @Test
    void restoreOverwritesExistingState() {
        var coord = newCoord(java.util.Map.of("orders", 4));
        // Member that won't appear in the snapshot we restore.
        coord.join("g1", "ghost", Set.of("orders"), SESSION_TIMEOUT_MS, REBALANCE_TIMEOUT_MS, 0L);
        assertThat(coord.memberCountOf("g1")).isEqualTo(1);

        // Build a snapshot externally — single member with a fresh id.
        var snap = new ConsumerOffsetsTopic.GroupMetadataValue(
                42,
                List.of(new ConsumerOffsetsTopic.MemberSnapshot(
                        "restored-member",
                        "instance-restored",
                        7,
                        List.of("orders"),
                        List.of(TopicPartition.newBuilder()
                                .setTopic("orders")
                                .setPartition(0)
                                .build()))));
        coord.restoreGroup("g1", snap, 100 * MS);

        assertThat(coord.memberCountOf("g1")).isEqualTo(1);
        assertThat(coord.generationOf("g1")).isEqualTo(42);
        assertThat(coord.assignmentFor("g1", "restored-member")).isPresent();
        // Old member is gone.
        assertThat(coord.assignmentFor("g1", "ghost")).isEmpty();
    }

    private static GroupCoordinator newCoord(java.util.Map<String, Integer> topicPartitions) {
        var counter = new AtomicInteger();
        return new GroupCoordinator(
                topic -> topicPartitions.getOrDefault(topic, 0),
                new RangeAssignor(),
                instanceId -> "member-" + counter.incrementAndGet());
    }
}
