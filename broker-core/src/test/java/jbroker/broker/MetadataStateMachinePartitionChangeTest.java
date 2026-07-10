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
    void guardedRecordDroppedWhenPriorEpochsMismatch() {
        // The #115 stale-controller scenario: a fencer derived its proposal
        // from (le=4, pe=0), but an ISR shrink (pe 0→1) committed first.
        // The guarded record must be dropped — applying it would promote a
        // leader chosen from an outdated ISR.
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        var sm = new MetadataStateMachine(tm);

        apply(
                sm,
                PartitionChangeRecord.newBuilder()
                        .setTopic("orders")
                        .setPartition(0)
                        .setLeader(3)
                        .addIsr(3)
                        .addIsr(1)
                        .setLeaderEpoch(4)
                        .setPartitionEpoch(1) // the shrink that already committed
                        .build());

        apply(
                sm,
                PartitionChangeRecord.newBuilder()
                        .setTopic("orders")
                        .setPartition(0)
                        .setLeader(1) // stale-view promotion
                        .addIsr(1)
                        .setLeaderEpoch(5)
                        .setPartitionEpoch(0)
                        .setPriorLeaderEpoch(4)
                        .setPriorPartitionEpoch(0) // derived before the shrink
                        .build());

        assertThat(tm.partitionLeader("orders", 0)).contains(3);
        assertThat(tm.partitionLeaderEpoch("orders", 0)).contains(4);
        assertThat(tm.partitionIsr("orders", 0)).containsExactly(3, 1);
    }

    @Test
    void guardedRecordAppliesWhenPriorEpochsMatch() {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        var sm = new MetadataStateMachine(tm);

        apply(
                sm,
                PartitionChangeRecord.newBuilder()
                        .setTopic("orders")
                        .setPartition(0)
                        .setLeader(3)
                        .addIsr(3)
                        .addIsr(1)
                        .setLeaderEpoch(4)
                        .setPartitionEpoch(1)
                        .build());

        apply(
                sm,
                PartitionChangeRecord.newBuilder()
                        .setTopic("orders")
                        .setPartition(0)
                        .setLeader(1)
                        .addIsr(1)
                        .setLeaderEpoch(5)
                        .setPartitionEpoch(0)
                        .setPriorLeaderEpoch(4)
                        .setPriorPartitionEpoch(1) // matches current
                        .build());

        assertThat(tm.partitionLeader("orders", 0)).contains(1);
        assertThat(tm.partitionLeaderEpoch("orders", 0)).contains(5);
    }

    @Test
    void guardedDropDoesNotFireLeaderEpochListener() {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        var bumps = new java.util.ArrayList<Integer>();
        var sm = new MetadataStateMachine(tm, new ProducerIdRegistry(), (t, p, epoch, leader) -> bumps.add(epoch));

        apply(
                sm,
                PartitionChangeRecord.newBuilder()
                        .setTopic("orders")
                        .setPartition(0)
                        .setLeader(3)
                        .addIsr(3)
                        .setLeaderEpoch(4)
                        .setPartitionEpoch(2)
                        .build());
        assertThat(bumps).containsExactly(4);

        apply(
                sm,
                PartitionChangeRecord.newBuilder()
                        .setTopic("orders")
                        .setPartition(0)
                        .setLeader(1)
                        .addIsr(1)
                        .setLeaderEpoch(5)
                        .setPriorLeaderEpoch(4)
                        .setPriorPartitionEpoch(0) // stale
                        .build());

        // Dropped record: no truncation-triggering epoch bump leaked out.
        assertThat(bumps).containsExactly(4);
    }

    private static void apply(MetadataStateMachine sm, PartitionChangeRecord record) {
        var payload =
                MetadataRecord.newBuilder().setPartitionChange(record).build().toByteArray();
        sm.apply(new LogEntry(1L, new Term(1L), LogEntry.Type.NORMAL, payload));
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
