package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import jbroker.proto.raft.MetadataRecord;
import jbroker.proto.raft.PartitionChangeRecord;
import jbroker.raft.core.LogEntry;
import jbroker.raft.core.Term;
import org.junit.jupiter.api.Test;

class MetadataStateMachinePartitionChangeTest {

    @Test
    void appliesPartitionChangeRecordToTopicManager() {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 3, 3, 0L);
        var sm = new MetadataStateMachine(tm);

        var record = MetadataRecord.newBuilder()
                .setPartitionChange(PartitionChangeRecord.newBuilder()
                        .setTopic("orders")
                        .setPartition(2)
                        .setLeader(7)
                        .addIsr(7)
                        .addIsr(8)
                        .addIsr(9)
                        .setLeaderEpoch(11)
                        .build())
                .build();

        sm.apply(new LogEntry(1L, new Term(1L), LogEntry.Type.NORMAL, record.toByteArray()));

        assertThat(tm.partitionLeader("orders", 2)).contains(7);
        assertThat(tm.partitionIsr("orders", 2)).containsExactly(7, 8, 9);
        assertThat(tm.partitionLeaderEpoch("orders", 2)).contains(11);
    }

    @Test
    void emptyMetadataRecordIsIgnored() {
        var tm = new TopicManager();
        var sm = new MetadataStateMachine(tm);

        var empty = MetadataRecord.newBuilder().build();
        sm.apply(new LogEntry(1L, new Term(1L), LogEntry.Type.NORMAL, empty.toByteArray()));

        assertThat(tm.list()).isEmpty();
        assertThat(tm.partitionLeader("anything", 0)).isEmpty();
    }

    @Test
    void partitionChangeForUnknownTopicStillAppliesToPartitionState() {
        // Documents current behaviour: topics map and partitions map are
        // independent. This is intentional so replaying a snapshot in any
        // order (topics before or after partition state) converges, but
        // warrants a test so a future change is deliberate.
        var tm = new TopicManager();
        var sm = new MetadataStateMachine(tm);

        var record = MetadataRecord.newBuilder()
                .setPartitionChange(PartitionChangeRecord.newBuilder()
                        .setTopic("ghost")
                        .setPartition(0)
                        .setLeader(1)
                        .addIsr(1)
                        .setLeaderEpoch(0)
                        .build())
                .build();
        sm.apply(new LogEntry(1L, new Term(1L), LogEntry.Type.NORMAL, record.toByteArray()));

        assertThat(tm.exists("ghost")).isFalse();
        assertThat(tm.partitionLeader("ghost", 0)).contains(1);
    }

    @Test
    void corruptPayloadIsSkippedNotThrown() {
        var tm = new TopicManager();
        var sm = new MetadataStateMachine(tm);
        sm.apply(new LogEntry(1L, new Term(1L), LogEntry.Type.NORMAL, new byte[] {0x7f, (byte) 0xff, 0x13}));
        assertThat(tm.list()).isEmpty();
    }
}
