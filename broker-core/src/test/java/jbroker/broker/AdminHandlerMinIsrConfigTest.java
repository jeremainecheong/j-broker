package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import jbroker.proto.broker.CreateTopicRequest;
import jbroker.proto.broker.UpdateTopicConfigRequest;
import org.junit.jupiter.api.Test;

/**
 * min.insync.replicas is validated where it enters the metadata log —
 * create-topic and update-topic-config. A floor the replica count can
 * never satisfy would brick every acks=all produce on the topic, so it
 * must be rejected before it commits.
 */
class AdminHandlerMinIsrConfigTest {

    private static AdminHandler handler(TopicManager tm, AtomicBoolean proposed, Set<Integer> brokers) {
        return new AdminHandler(
                tm,
                (payload, timeoutMillis) -> proposed.set(true),
                1,
                () -> brokers,
                () -> Optional.of(1),
                new BrokerRegistry());
    }

    private static CreateTopicRequest create(String minIsr) {
        return CreateTopicRequest.newBuilder()
                .setTopic("orders")
                .setPartitions(1)
                .setReplicationFactor(3)
                .putConfig(TopicDescription.MIN_INSYNC_REPLICAS_CONFIG, minIsr)
                .build();
    }

    @Test
    void createRejectsFloorAboveReplicationFactor() {
        var proposed = new AtomicBoolean();
        var h = handler(new TopicManager(), proposed, Set.of(1, 2, 3));

        var resp = h.createTopic(create("4"));

        assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.INVALID_CONFIG);
        assertThat(resp.getError().getMessage()).contains("replication factor");
        assertThat(proposed).isFalse();
    }

    @Test
    void createValidatesAgainstClampedReplicationFactor() {
        // Single-broker cluster: rf=3 request clamps to rf=1, so a floor
        // of 2 is unsatisfiable and must be rejected against the clamped
        // value, not the requested one.
        var proposed = new AtomicBoolean();
        var h = handler(new TopicManager(), proposed, Set.of(1));

        var resp = h.createTopic(create("2"));

        assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.INVALID_CONFIG);
        assertThat(proposed).isFalse();
    }

    @Test
    void createRejectsNonIntegerAndSubOneValues() {
        var proposed = new AtomicBoolean();
        var h = handler(new TopicManager(), proposed, Set.of(1, 2, 3));

        assertThat(h.createTopic(create("two")).getError().getCode()).isEqualTo(ErrorCodes.INVALID_CONFIG);
        assertThat(h.createTopic(create("0")).getError().getCode()).isEqualTo(ErrorCodes.INVALID_CONFIG);
        assertThat(proposed).isFalse();
    }

    @Test
    void createAcceptsValidFloor() {
        var proposed = new AtomicBoolean();
        var h = handler(new TopicManager(), proposed, Set.of(1, 2, 3));

        var resp = h.createTopic(create("2"));

        assertThat(resp.hasError()).isFalse();
        assertThat(proposed).isTrue();
    }

    @Test
    void updateRejectsFloorAboveExistingReplicationFactor() {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 2, 0L);
        var proposed = new AtomicBoolean();
        var h = handler(tm, proposed, Set.of(1, 2));

        var resp = h.updateTopicConfig(UpdateTopicConfigRequest.newBuilder()
                .setTopic("orders")
                .putConfig(TopicDescription.MIN_INSYNC_REPLICAS_CONFIG, "3")
                .build());

        assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.INVALID_CONFIG);
        assertThat(proposed).isFalse();
    }

    @Test
    void updateAcceptsValidFloor() {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        var proposed = new AtomicBoolean();
        var h = handler(tm, proposed, Set.of(1, 2, 3));

        var resp = h.updateTopicConfig(UpdateTopicConfigRequest.newBuilder()
                .setTopic("orders")
                .putConfig(TopicDescription.MIN_INSYNC_REPLICAS_CONFIG, "3")
                .build());

        assertThat(resp.hasError()).isFalse();
        assertThat(proposed).isTrue();
    }
}
