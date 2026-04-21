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
    void legacyOnPartitionChangeDefaultsReplicasToIsr() {
        // Backward-compatible overload used by pre-P6.3 callers + snapshot v2
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
