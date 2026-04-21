package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import jbroker.proto.raft.MetadataRecord;
import jbroker.proto.raft.ProducerIdAssignmentRecord;
import jbroker.raft.core.LogEntry;
import jbroker.raft.core.Term;
import org.junit.jupiter.api.Test;

class MetadataStateMachineSnapshotTest {

    @Test
    void snapshotAndRestoreRoundTripsProducerIdCounter() throws Exception {
        var srcTopics = new TopicManager();
        var srcReg = new ProducerIdRegistry();
        var srcSm = new MetadataStateMachine(srcTopics, srcReg);

        // Simulate three InitProducerId commits advancing next_producer_id
        // to 3. Snapshot then restore into a fresh registry and assert the
        // counter is preserved so the new active controller allocates from
        // 3 instead of colliding on 0.
        srcSm.apply(producerIdApply(0L, 1L));
        srcSm.apply(producerIdApply(1L, 2L));
        srcSm.apply(producerIdApply(2L, 3L));
        assertThat(srcReg.peekNextProducerId()).isEqualTo(3L);

        var baos = new ByteArrayOutputStream();
        srcSm.snapshot(baos);

        var dstTopics = new TopicManager();
        var dstReg = new ProducerIdRegistry();
        var dstSm = new MetadataStateMachine(dstTopics, dstReg);
        dstSm.restore(new ByteArrayInputStream(baos.toByteArray()));

        assertThat(dstReg.peekNextProducerId()).isEqualTo(3L);
    }

    @Test
    void restoreAcceptsV2SnapshotFromBeforeProducerIdRegistry() throws Exception {
        // v2 snapshots (pre-P6.7) don't carry a producer-id section. Assert
        // restore treats the counter as 0 rather than blowing up on EOF.
        var srcTopics = new TopicManager();
        srcTopics.onTopicCommitted("t", 1, 1, 1L);
        srcTopics.onPartitionChange("t", 0, 1, List.of(1), 0);

        var baos = new ByteArrayOutputStream();
        var dout = new java.io.DataOutputStream(baos);
        dout.writeByte(2); // version 2
        dout.writeInt(1);
        dout.writeUTF("t");
        dout.writeInt(1);
        dout.writeInt(1);
        dout.writeLong(1L);
        dout.writeInt(1);
        dout.writeUTF("t");
        dout.writeInt(0);
        dout.writeInt(1);
        dout.writeInt(1);
        dout.writeInt(1);
        dout.writeInt(0);
        dout.flush();

        var dstTopics = new TopicManager();
        var dstReg = new ProducerIdRegistry();
        var dstSm = new MetadataStateMachine(dstTopics, dstReg);
        dstSm.restore(new ByteArrayInputStream(baos.toByteArray()));

        assertThat(dstTopics.describe("t")).isPresent();
        assertThat(dstReg.peekNextProducerId()).isEqualTo(0L);
    }

    private static LogEntry producerIdApply(long producerId, long nextProducerId) {
        var pid = ProducerIdAssignmentRecord.newBuilder()
                .setProducerId(producerId)
                .setProducerEpoch(0)
                .setNextProducerId(nextProducerId)
                .build();
        var record = MetadataRecord.newBuilder().setProducerIdAssignment(pid).build();
        return new LogEntry(1L, new Term(1L), LogEntry.Type.NORMAL, record.toByteArray());
    }

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
