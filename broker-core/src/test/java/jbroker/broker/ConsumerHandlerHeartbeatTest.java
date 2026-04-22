package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import jbroker.broker.group.GroupCoordinator;
import jbroker.broker.group.RangeAssignor;
import jbroker.proto.broker.ConsumerGroupHeartbeatRequest;
import jbroker.proto.common.ErrorCode;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * proves the {@code ConsumerGroupHeartbeat} RPC translates the
 * single-RPC KIP-848 protocol (join / steady / leave) onto the
 * {@link GroupCoordinator} state machine, and that the
 * {@code NOT_COORDINATOR} routing guard fires when this broker doesn't
 * lead the group's coordinator partition.
 */
class ConsumerHandlerHeartbeatTest {

    private static final long NOW_NS = 1_000_000L;

    @Test
    void joinReturnsAssignmentForSingleConsumer(@TempDir Path dir) throws IOException {
        var fixture = newFixture(dir, /*selfBrokerId*/ 1, /*topicPartitionsLeadByThisBroker*/ true);
        // Subscribe a topic with 6 partitions so the assignor has work.
        fixture.tm.onTopicCommitted("orders", 6, 1, 0L);
        for (int p = 0; p < 6; p++) {
            fixture.tm.onPartitionChange("orders", p, /*leader*/ 1, List.of(1), List.of(1), 1, 0);
        }

        var resp = fixture.handler.consumerGroupHeartbeat(ConsumerGroupHeartbeatRequest.newBuilder()
                .setGroupId("g1")
                .setMemberEpoch(0)
                .addSubscribedTopics("orders")
                .build());

        assertThat(resp.getError()).isEqualTo(ErrorCode.OK);
        assertThat(resp.getMemberId()).isNotEmpty();
        assertThat(resp.getMemberEpoch()).isPositive();
        assertThat(resp.getAssignment().getAssignedPartitionsList())
                .singleElement()
                .satisfies(tp -> {
                    assertThat(tp.getTopic()).isEqualTo("orders");
                    assertThat(tp.getPartitionsList()).containsExactly(0, 1, 2, 3, 4, 5);
                });
    }

    @Test
    void steadyHeartbeatReturnsEmptyAssignment(@TempDir Path dir) throws IOException {
        var fixture = newFixture(dir, 1, true);
        fixture.tm.onTopicCommitted("orders", 4, 1, 0L);
        var join = fixture.handler.consumerGroupHeartbeat(ConsumerGroupHeartbeatRequest.newBuilder()
                .setGroupId("g1")
                .setMemberEpoch(0)
                .addSubscribedTopics("orders")
                .build());

        var hb = fixture.handler.consumerGroupHeartbeat(ConsumerGroupHeartbeatRequest.newBuilder()
                .setGroupId("g1")
                .setMemberId(join.getMemberId())
                .setMemberEpoch(join.getMemberEpoch())
                .build());

        assertThat(hb.getError()).isEqualTo(ErrorCode.OK);
        assertThat(hb.getAssignment().getAssignedPartitionsList()).isEmpty();
    }

    @Test
    void leaveReturnsOk(@TempDir Path dir) throws IOException {
        var fixture = newFixture(dir, 1, true);
        fixture.tm.onTopicCommitted("orders", 4, 1, 0L);
        var join = fixture.handler.consumerGroupHeartbeat(ConsumerGroupHeartbeatRequest.newBuilder()
                .setGroupId("g1")
                .setMemberEpoch(0)
                .addSubscribedTopics("orders")
                .build());
        int genBefore = fixture.coord.generationOf("g1");

        var leave = fixture.handler.consumerGroupHeartbeat(ConsumerGroupHeartbeatRequest.newBuilder()
                .setGroupId("g1")
                .setMemberId(join.getMemberId())
                .setMemberEpoch(-1)
                .build());

        assertThat(leave.getError()).isEqualTo(ErrorCode.OK);
        assertThat(fixture.coord.memberCountOf("g1")).isZero();
        assertThat(fixture.coord.generationOf("g1")).isEqualTo(genBefore + 1);
    }

    @Test
    void unknownMemberIdSurfaces(@TempDir Path dir) throws IOException {
        var fixture = newFixture(dir, 1, true);
        var resp = fixture.handler.consumerGroupHeartbeat(ConsumerGroupHeartbeatRequest.newBuilder()
                .setGroupId("g1")
                .setMemberId("ghost")
                .setMemberEpoch(7)
                .build());
        assertThat(resp.getError()).isEqualTo(ErrorCode.UNKNOWN_MEMBER_ID);
    }

    @Test
    void notCoordinatorReturnedWhenThisBrokerIsNotTheLeader(@TempDir Path dir) throws IOException {
        // Self is broker 1, but coordinator partition leader is broker 2.
        var fixture = newFixture(dir, 1, /*topicPartitionsLeadByThisBroker*/ false);
        var resp = fixture.handler.consumerGroupHeartbeat(ConsumerGroupHeartbeatRequest.newBuilder()
                .setGroupId("g1")
                .setMemberEpoch(0)
                .build());
        assertThat(resp.getError()).isEqualTo(ErrorCode.NOT_COORDINATOR);
    }

    @Test
    void coordinatorNotAvailableReturnedWhenConsumerOffsetsTopicMissing(@TempDir Path dir) throws IOException {
        var tm = new TopicManager();
        var coord = newCoord(tm);
        var handler = new ConsumerHandler(tm, newLogManager(dir), new BrokerRegistry(), coord, 1, () -> NOW_NS);

        var resp = handler.consumerGroupHeartbeat(ConsumerGroupHeartbeatRequest.newBuilder()
                .setGroupId("g1")
                .setMemberEpoch(0)
                .build());
        // No __consumer_offsets → no coordinator exists. Client retries
        // after backoff; FindCoordinator would be equally unavailable.
        assertThat(resp.getError()).isEqualTo(ErrorCode.COORDINATOR_NOT_AVAILABLE);
    }

    private record Fixture(ConsumerHandler handler, TopicManager tm, GroupCoordinator coord) {}

    private static Fixture newFixture(Path dir, int selfBrokerId, boolean leadsCoordPartition) throws IOException {
        var tm = new TopicManager();
        // Auto-create __consumer_offsets with 1 partition; partition 0
        // leader is configurable (this broker or another).
        tm.onTopicCommitted(ConsumerOffsetsTopic.NAME, 1, 1, 0L, true, true);
        int leader = leadsCoordPartition ? selfBrokerId : selfBrokerId + 1;
        tm.onPartitionChange(ConsumerOffsetsTopic.NAME, 0, leader, List.of(leader), List.of(leader), 1, 0);

        var coord = newCoord(tm);
        var handler =
                new ConsumerHandler(tm, newLogManager(dir), new BrokerRegistry(), coord, selfBrokerId, () -> NOW_NS);
        return new Fixture(handler, tm, coord);
    }

    private static GroupCoordinator newCoord(TopicManager tm) {
        var counter = new AtomicInteger();
        return new GroupCoordinator(
                topic -> tm.describe(topic).map(TopicDescription::partitions).orElse(0),
                new RangeAssignor(),
                instanceId -> "member-" + counter.incrementAndGet());
    }

    private static LogManager newLogManager(Path dir) throws IOException {
        return new LogManager(
                dir,
                new LogManager.Config(
                        128L * 1024 * 1024,
                        Long.MAX_VALUE,
                        LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        java.util.concurrent.TimeUnit.MINUTES.toMillis(5)));
    }
}
