package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class TopicManagerReplicaSetTest {

    @Test
    void onPartitionChangePopulatesReplicaSetDistinctFromIsr() {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange(
                "orders", 0, /* leader */ 1, /* isr */ List.of(1, 2), /* replicas */ List.of(1, 2, 3), /* epoch */ 0);

        var state = tm.partitionState("orders", 0).orElseThrow();
        assertThat(state.isr()).containsExactly(1, 2);
        assertThat(state.replicas()).containsExactly(1, 2, 3);
    }

    @Test
    void isrOnlyFlipRequiresPartitionEpochBumpWithoutLeaderEpochChange() {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange(
                "orders",
                0, /* leader */
                1,
                List.of(1, 2, 3),
                List.of(1, 2, 3), /* leaderEpoch */
                5, /* partitionEpoch */
                0);

        // ISR-only flip keeps leaderEpoch at 5 but bumps partitionEpoch.
        tm.onPartitionChange("orders", 0, 1, List.of(1, 2), List.of(1, 2, 3), 5, 1);

        var state = tm.partitionState("orders", 0).orElseThrow();
        assertThat(state.leaderEpoch()).isEqualTo(5);
        assertThat(state.partitionEpoch()).isEqualTo(1);
        assertThat(state.isr()).containsExactly(1, 2);
    }

    @Test
    void stalePartitionEpochAtSameLeaderEpochIsIgnored() {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, 1, List.of(1, 2), List.of(1, 2, 3), 5, 3);
        // Re-ordered delivery of an older ISR flip at the same leader epoch.
        tm.onPartitionChange("orders", 0, 1, List.of(1), List.of(1, 2, 3), 5, 2);

        var state = tm.partitionState("orders", 0).orElseThrow();
        assertThat(state.partitionEpoch()).isEqualTo(3);
        assertThat(state.isr()).containsExactly(1, 2);
    }

    @Test
    void higherLeaderEpochAlwaysWinsEvenWithLowerPartitionEpoch() {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, 1, List.of(1, 2), List.of(1, 2, 3), 5, 7);
        // New leader bumps leaderEpoch, resets partitionEpoch to 0.
        tm.onPartitionChange("orders", 0, 2, List.of(2), List.of(1, 2, 3), 6, 0);

        var state = tm.partitionState("orders", 0).orElseThrow();
        assertThat(state.leader()).isEqualTo(2);
        assertThat(state.leaderEpoch()).isEqualTo(6);
        assertThat(state.partitionEpoch()).isZero();
    }

    @Test
    void legacyOnPartitionChangeDefaultsReplicasToIsr() {
        // Backward-compatible overload used by earliercallers + snapshot v2
        // restore — assumes ISR == replicas until a new PartitionChangeRecord
        // carrying a real replica set arrives.
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, 1, List.of(1, 2), 0);

        var state = tm.partitionState("orders", 0).orElseThrow();
        assertThat(state.isr()).containsExactly(1, 2);
        assertThat(state.replicas()).containsExactly(1, 2);
    }
}
