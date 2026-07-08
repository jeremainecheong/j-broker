package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import jbroker.proto.broker.FindCoordinatorRequest;
import jbroker.proto.common.ErrorCode;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies {@link ConsumerHandler#findCoordinator} maps groups to
 * coordinator partitions deterministically (same group_id always maps to
 * the same coordinator broker), returns the leader's host:port via
 * {@link BrokerRegistry}, and surfaces {@code COORDINATOR_NOT_AVAILABLE}
 * when {@code __consumer_offsets} is missing or has no leader yet.
 */
class FindCoordinatorTest {

    @Test
    void returnsCoordinatorNotAvailableWhenTopicMissing(@TempDir Path dir) throws IOException {
        var handler = newHandler(dir, new TopicManager(), new BrokerRegistry());
        var resp = handler.findCoordinator(
                FindCoordinatorRequest.newBuilder().setKey("g1").build());
        assertThat(resp.getError()).isEqualTo(ErrorCode.COORDINATOR_NOT_AVAILABLE);
    }

    @Test
    void returnsCoordinatorNotAvailableWhenLeaderIsNoLeaderSentinel(@TempDir Path dir) throws IOException {
        var tm = new TopicManager();
        tm.onTopicCommitted(ConsumerOffsetsTopic.NAME, 50, 1, 0L, true, true);
        // partition with leader=-1 ("no surviving ISR") sentinel from BrokerFencer.
        for (int p = 0; p < 50; p++) {
            tm.onPartitionChange(ConsumerOffsetsTopic.NAME, p, /*leader*/ -1, List.of(), List.of(1), 1, 0);
        }
        var registry = new BrokerRegistry();
        registry.onBrokerRegistration(1, "host-1", 9001);

        var handler = newHandler(dir, tm, registry);
        var resp = handler.findCoordinator(
                FindCoordinatorRequest.newBuilder().setKey("g1").build());
        assertThat(resp.getError()).isEqualTo(ErrorCode.COORDINATOR_NOT_AVAILABLE);
    }

    @Test
    void returnsLeaderEndpointForGroup(@TempDir Path dir) throws IOException {
        var tm = newSeededTopicManager();
        var registry = new BrokerRegistry();
        registry.onBrokerRegistration(1, "host-1", 9001);
        registry.onBrokerRegistration(2, "host-2", 9002);
        registry.onBrokerRegistration(3, "host-3", 9003);

        var handler = newHandler(dir, tm, registry);
        var resp = handler.findCoordinator(
                FindCoordinatorRequest.newBuilder().setKey("g1").build());

        assertThat(resp.getError()).isEqualTo(ErrorCode.OK);
        // Coordinator partition is deterministic — Math.floorMod(hashCode, 50)
        // — so the test doesn't hard-code which broker (1/2/3) but does
        // assert that whichever was returned matches the leader for that
        // partition + the registry entry.
        int expectedPartition = Math.floorMod("g1".hashCode(), 50);
        int expectedLeader = tm.partitionState(ConsumerOffsetsTopic.NAME, expectedPartition)
                .orElseThrow()
                .leader();
        assertThat(resp.getCoordinator().getNodeId()).isEqualTo(expectedLeader);
        assertThat(resp.getCoordinator().getHost()).isEqualTo("host-" + expectedLeader);
        assertThat(resp.getCoordinator().getPort()).isEqualTo(9000 + expectedLeader);
    }

    @Test
    void sameGroupAlwaysMapsToSameCoordinator(@TempDir Path dir) throws IOException {
        var tm = newSeededTopicManager();
        var registry = new BrokerRegistry();
        registry.onBrokerRegistration(1, "host-1", 9001);
        registry.onBrokerRegistration(2, "host-2", 9002);
        registry.onBrokerRegistration(3, "host-3", 9003);
        var handler = newHandler(dir, tm, registry);

        var first = handler.findCoordinator(
                FindCoordinatorRequest.newBuilder().setKey("checkout").build());
        var second = handler.findCoordinator(
                FindCoordinatorRequest.newBuilder().setKey("checkout").build());

        assertThat(first.getCoordinator().getNodeId())
                .isEqualTo(second.getCoordinator().getNodeId());
    }

    @Test
    void differentGroupsCanMapToDifferentCoordinators(@TempDir Path dir) throws IOException {
        var tm = newSeededTopicManager();
        var registry = new BrokerRegistry();
        registry.onBrokerRegistration(1, "host-1", 9001);
        registry.onBrokerRegistration(2, "host-2", 9002);
        registry.onBrokerRegistration(3, "host-3", 9003);
        var handler = newHandler(dir, tm, registry);

        // With 50 partitions round-robined across 3 brokers, picking a
        // handful of group ids is overwhelmingly likely to surface at
        // least two distinct coordinators. (If an unlucky hash collision
        // ever fails this in CI, swap the literals — the underlying
        // routing is deterministic per JVM build.)
        var coordinators = new java.util.HashSet<Integer>();
        for (var g : List.of("a", "b", "c", "d", "e", "f", "g", "h")) {
            var resp = handler.findCoordinator(
                    FindCoordinatorRequest.newBuilder().setKey(g).build());
            coordinators.add(resp.getCoordinator().getNodeId());
        }
        assertThat(coordinators).hasSizeGreaterThanOrEqualTo(2);
    }

    @Test
    void returnsCoordinatorNotAvailableWhenLeaderHasNoRegistryEntry(@TempDir Path dir) throws IOException {
        var tm = newSeededTopicManager();
        // Registry only knows broker 1 — partitions led by 2 or 3 won't
        // resolve to a host:port and must surface COORDINATOR_NOT_AVAILABLE
        // rather than a half-built endpoint.
        var registry = new BrokerRegistry();
        registry.onBrokerRegistration(1, "host-1", 9001);
        var handler = newHandler(dir, tm, registry);

        // Find a group whose coordinator partition is led by broker 2 or 3.
        String groupHittingBroker2Or3 = null;
        for (int i = 0; i < 100; i++) {
            String g = "g-" + i;
            int partition = Math.floorMod(g.hashCode(), 50);
            int leader = tm.partitionState(ConsumerOffsetsTopic.NAME, partition)
                    .orElseThrow()
                    .leader();
            if (leader == 2 || leader == 3) {
                groupHittingBroker2Or3 = g;
                break;
            }
        }
        assertThat(groupHittingBroker2Or3).isNotNull();

        var resp = handler.findCoordinator(FindCoordinatorRequest.newBuilder()
                .setKey(groupHittingBroker2Or3)
                .build());
        assertThat(resp.getError()).isEqualTo(ErrorCode.COORDINATOR_NOT_AVAILABLE);
    }

    private static TopicManager newSeededTopicManager() {
        var tm = new TopicManager();
        tm.onTopicCommitted(ConsumerOffsetsTopic.NAME, 50, 3, 0L, true, true);
        // Round-robin leadership across brokers 1, 2, 3 (matches
        // ConsumerOffsetsCreator's deterministic seeding).
        for (int p = 0; p < 50; p++) {
            int leader = (p % 3) + 1;
            tm.onPartitionChange(ConsumerOffsetsTopic.NAME, p, leader, List.of(leader), List.of(1, 2, 3), 1, 0);
        }
        return tm;
    }

    private static ConsumerHandler newHandler(Path dir, TopicManager tm, BrokerRegistry registry) throws IOException {
        var lm = new LogManager(
                dir,
                new LogManager.Config(
                        128L * 1024 * 1024,
                        Long.MAX_VALUE,
                        LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        java.util.concurrent.TimeUnit.MINUTES.toMillis(5)));
        return new ConsumerHandler(tm, lm, registry);
    }
}
