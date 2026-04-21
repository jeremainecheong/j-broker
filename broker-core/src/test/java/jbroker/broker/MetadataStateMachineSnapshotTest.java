package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import org.junit.jupiter.api.Test;

class MetadataStateMachineSnapshotTest {

    @Test
    void snapshotAndRestoreRoundTripsTopicsAndPartitionState() throws Exception {
        var srcTopics = new TopicManager();
        srcTopics.onTopicCommitted("orders", 3, 3, 1_700_000_000L);
        srcTopics.onTopicCommitted("alerts", 1, 1, 1_700_000_001L);
        srcTopics.onPartitionChange("orders", 0, 1, List.of(1, 2, 3), 4);
        srcTopics.onPartitionChange("orders", 1, 2, List.of(2, 3, 1), 7);
        srcTopics.onPartitionChange("orders", 2, 3, List.of(3, 1, 2), 0);
        srcTopics.onPartitionChange("alerts", 0, 1, List.of(1), 0);

        var srcSm = new MetadataStateMachine(srcTopics);
        var baos = new ByteArrayOutputStream();
        srcSm.snapshot(baos);

        var dstTopics = new TopicManager();
        var dstSm = new MetadataStateMachine(dstTopics);
        dstSm.restore(new ByteArrayInputStream(baos.toByteArray()));

        assertThat(dstTopics.describe("orders"))
                .hasValueSatisfying(d -> assertThat(d.partitions()).isEqualTo(3));
        assertThat(dstTopics.describe("alerts")).isPresent();

        assertThat(dstTopics.partitionLeader("orders", 0)).contains(1);
        assertThat(dstTopics.partitionIsr("orders", 0)).containsExactly(1, 2, 3);
        assertThat(dstTopics.partitionLeaderEpoch("orders", 0)).contains(4);

        assertThat(dstTopics.partitionLeader("orders", 1)).contains(2);
        assertThat(dstTopics.partitionLeaderEpoch("orders", 1)).contains(7);

        assertThat(dstTopics.partitionLeader("orders", 2)).contains(3);
        assertThat(dstTopics.partitionLeaderEpoch("orders", 2)).contains(0);

        assertThat(dstTopics.partitionLeader("alerts", 0)).contains(1);
    }
}
