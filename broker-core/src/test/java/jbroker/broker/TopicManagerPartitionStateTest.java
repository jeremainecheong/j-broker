package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class TopicManagerPartitionStateTest {

    @Test
    void partitionLeaderEmptyBeforeAnyPartitionChange() {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 3, 1, 0L);
        assertThat(tm.partitionLeader("orders", 0)).isEmpty();
        assertThat(tm.partitionIsr("orders", 0)).isEmpty();
        assertThat(tm.partitionLeaderEpoch("orders", 0)).isEmpty();
    }

    @Test
    void onPartitionChangePopulatesLeaderIsrEpoch() {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 3, 3, 0L);

        tm.onPartitionChange("orders", 1, /* leader */ 7, List.of(7, 8, 9), /* epoch */ 4);

        assertThat(tm.partitionLeader("orders", 1)).contains(7);
        assertThat(tm.partitionIsr("orders", 1)).containsExactly(7, 8, 9);
        assertThat(tm.partitionLeaderEpoch("orders", 1)).contains(4);
    }

    @Test
    void onPartitionChangeWithStaleEpochIsIgnored() {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, 7, List.of(7, 8, 9), 5);

        // Older epoch arriving out of order must not overwrite the newer state.
        tm.onPartitionChange("orders", 0, 8, List.of(8, 9), 4);

        assertThat(tm.partitionLeader("orders", 0)).contains(7);
        assertThat(tm.partitionLeaderEpoch("orders", 0)).contains(5);
    }

    @Test
    void onPartitionChangeWithHigherEpochOverwrites() {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, 7, List.of(7, 8, 9), 5);

        tm.onPartitionChange("orders", 0, 8, List.of(8, 9), 6);

        assertThat(tm.partitionLeader("orders", 0)).contains(8);
        assertThat(tm.partitionIsr("orders", 0)).containsExactly(8, 9);
        assertThat(tm.partitionLeaderEpoch("orders", 0)).contains(6);
    }

    @Test
    void partitionLeaderUnknownTopicIsEmpty() {
        var tm = new TopicManager();
        assertThat(tm.partitionLeader("ghost", 0)).isEmpty();
    }

    @Test
    void partitionStateReturnsLeaderIsrEpochAtomically() {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, 7, List.of(7, 8, 9), 4);

        assertThat(tm.partitionState("orders", 0)).hasValueSatisfying(s -> {
            assertThat(s.leader()).isEqualTo(7);
            assertThat(s.isr()).containsExactly(7, 8, 9);
            assertThat(s.leaderEpoch()).isEqualTo(4);
        });
    }

    @Test
    void partitionStateUnknownPartitionIsEmpty() {
        var tm = new TopicManager();
        assertThat(tm.partitionState("ghost", 0)).isEmpty();
    }
}
