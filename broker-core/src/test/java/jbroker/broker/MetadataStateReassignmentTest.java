package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import jbroker.proto.raft.MetadataRecord;
import jbroker.proto.raft.PartitionReassignmentRecord;
import jbroker.raft.core.LogEntry;
import jbroker.raft.core.Term;
import org.junit.jupiter.api.Test;

class MetadataStateReassignmentTest {

    @Test
    void applyingARecordSetsThePendingReassignment() {
        var sm = new MetadataStateMachine(new TopicManager());
        sm.apply(wrap(set("t", 0, List.of(2, 3, 4), List.of(1, 2, 3)), 1));

        var pending = sm.reassignments().get("t", 0).orElseThrow();
        assertThat(pending.target()).containsExactly(2, 3, 4);
        assertThat(pending.original()).containsExactly(1, 2, 3);
    }

    @Test
    void applyingAnEmptyTargetClearsThePendingReassignment() {
        var sm = new MetadataStateMachine(new TopicManager());
        sm.apply(wrap(set("t", 0, List.of(2, 3, 4), List.of(1, 2, 3)), 1));
        // A clear record carries an empty target (the driver emits it on completion).
        sm.apply(wrap(
                PartitionReassignmentRecord.newBuilder()
                        .setTopic("t")
                        .setPartition(0)
                        .build(),
                2));

        assertThat(sm.reassignments().get("t", 0)).isEmpty();
    }

    @Test
    void snapshotAndRestoreRoundTripsPendingReassignments() throws Exception {
        var srcSm = new MetadataStateMachine(new TopicManager());
        srcSm.apply(wrap(set("t", 0, List.of(2, 3, 4), List.of(1, 2, 3)), 1));
        srcSm.apply(wrap(set("t", 1, List.of(3, 4), List.of(2, 3)), 2));

        var baos = new ByteArrayOutputStream();
        srcSm.snapshot(baos);

        var dstSm = new MetadataStateMachine(new TopicManager());
        dstSm.restore(new ByteArrayInputStream(baos.toByteArray()));

        assertThat(dstSm.reassignments().list()).hasSize(2);
        var p0 = dstSm.reassignments().get("t", 0).orElseThrow();
        assertThat(p0.target()).containsExactly(2, 3, 4);
        assertThat(p0.original()).containsExactly(1, 2, 3);
        assertThat(dstSm.reassignments().get("t", 1).orElseThrow().target()).containsExactly(3, 4);
    }

    private static PartitionReassignmentRecord set(
            String topic, int partition, List<Integer> target, List<Integer> original) {
        return PartitionReassignmentRecord.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .addAllTargetReplicas(target)
                .addAllOriginalReplicas(original)
                .build();
    }

    private static LogEntry wrap(PartitionReassignmentRecord r, long index) {
        byte[] bytes =
                MetadataRecord.newBuilder().setPartitionReassignment(r).build().toByteArray();
        return new LogEntry(index, new Term(1), LogEntry.Type.NORMAL, bytes);
    }
}
