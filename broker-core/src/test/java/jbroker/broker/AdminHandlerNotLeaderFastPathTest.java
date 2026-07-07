package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import jbroker.proto.broker.CreateTopicRequest;
import jbroker.proto.broker.DeleteTopicRequest;
import jbroker.proto.broker.UpdateTopicConfigRequest;
import org.junit.jupiter.api.Test;

/**
 * Root cause of the k6 admin-api-smoke 5s p95 cliff: a Raft follower's
 * {@code RaftDriver.propose} silently drops the payload
 * (fire-and-forget), so {@code proposeAndWait} burned its full 5s
 * timeout before AdminHandler returned NOT_LEADER. Every admin mutation
 * routed to a non-leader broker cost ~5s instead of ~1ms, and the
 * admin-app's broker pool then retried elsewhere. The handler must fail
 * fast — it already knows the current leader via {@code LeaderIdLookup}.
 */
class AdminHandlerNotLeaderFastPathTest {

    private static AdminHandler handler(
            TopicManager tm, Optional<Integer> leader, BrokerRegistry reg, AtomicBoolean proposed) {
        return new AdminHandler(
                tm,
                (payload, timeoutMillis) -> {
                    proposed.set(true);
                    // Simulate the follower behaviour this fix removes from
                    // the request path: proposal dropped, future times out.
                    Thread.sleep(timeoutMillis);
                    throw new IllegalStateException("timed out waiting for apply");
                },
                /*self*/ 1,
                () -> Set.of(1, 2, 3),
                () -> leader,
                reg);
    }

    @Test
    void createOnFollowerFailsFastWithLeaderHints() {
        var tm = new TopicManager();
        var reg = new BrokerRegistry();
        reg.onBrokerRegistration(2, "leader-host", 9999);
        var proposed = new AtomicBoolean(false);
        var h = handler(tm, Optional.of(2), reg, proposed);

        long t0 = System.nanoTime();
        var resp = h.createTopic(CreateTopicRequest.newBuilder()
                .setTopic("t")
                .setPartitions(1)
                .setReplicationFactor(1)
                .build());
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

        assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NOT_LEADER);
        assertThat(resp.getError().getHintMap())
                .containsEntry("suggested_leader_id", "2")
                .containsEntry("suggested_leader_host", "leader-host")
                .containsEntry("suggested_leader_port", "9999");
        assertThat(proposed).as("follower must not propose into the void").isFalse();
        assertThat(elapsedMs).as("must not burn the propose timeout").isLessThan(1_000);
    }

    @Test
    void createDuringElectionWindowFailsFastWithoutHints() {
        var tm = new TopicManager();
        var proposed = new AtomicBoolean(false);
        var h = handler(tm, Optional.empty(), new BrokerRegistry(), proposed);

        long t0 = System.nanoTime();
        var resp = h.createTopic(CreateTopicRequest.newBuilder()
                .setTopic("t")
                .setPartitions(1)
                .setReplicationFactor(1)
                .build());

        assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NOT_LEADER);
        assertThat(proposed).isFalse();
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0)).isLessThan(1_000);
    }

    @Test
    void deleteAndConfigUpdateOnFollowerFailFast() {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 1, 0);
        var proposed = new AtomicBoolean(false);
        var h = handler(tm, Optional.of(3), new BrokerRegistry(), proposed);

        long t0 = System.nanoTime();
        var del =
                h.deleteTopic(DeleteTopicRequest.newBuilder().setTopic("orders").build());
        var cfg = h.updateTopicConfig(UpdateTopicConfigRequest.newBuilder()
                .setTopic("orders")
                .putConfig("cleanup.policy", "compact")
                .build());

        assertThat(del.getError().getCode()).isEqualTo(ErrorCodes.NOT_LEADER);
        assertThat(cfg.getError().getCode()).isEqualTo(ErrorCodes.NOT_LEADER);
        assertThat(proposed).isFalse();
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0)).isLessThan(1_000);
    }

    @Test
    void leaderStillProposesNormally() {
        var tm = new TopicManager();
        var proposed = new AtomicBoolean(false);
        var h = new AdminHandler(
                tm,
                (payload, timeoutMillis) -> proposed.set(true),
                /*self*/ 1,
                () -> Set.of(1),
                () -> Optional.of(1),
                new BrokerRegistry());

        var resp = h.createTopic(CreateTopicRequest.newBuilder()
                .setTopic("t")
                .setPartitions(1)
                .setReplicationFactor(1)
                .build());

        assertThat(resp.hasError()).isFalse();
        assertThat(proposed).isTrue();
    }
}
