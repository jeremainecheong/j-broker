package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import jbroker.proto.raft.CreateTopicRecord;
import jbroker.proto.raft.MetadataRecord;
import jbroker.proto.raft.PartitionChangeRecord;
import jbroker.proto.raft.TopicRecord;
import jbroker.raft.core.LogEntry;
import jbroker.raft.core.Term;
import org.junit.jupiter.api.Test;

class MetadataStateMachineLeaderListenerTest {

    private record Event(String topic, int partition, int leaderEpoch, int leaderId) {}

    @Test
    void fireOnLeaderEpochBumpForFreshPartition() {
        var events = new ArrayList<Event>();
        var tm = new TopicManager();
        var sm = new MetadataStateMachine(
                tm, new ProducerIdRegistry(), (t, p, e, l) -> events.add(new Event(t, p, e, l)));

        // Fresh topic — leader_epoch starts at 0.
        sm.apply(entry(MetadataRecord.newBuilder()
                .setCreateTopic(CreateTopicRecord.newBuilder()
                        .setTopic(TopicRecord.newBuilder()
                                .setTopic("orders")
                                .setPartitions(2)
                                .setReplicationFactor(1)
                                .build())
                        .addPartitionChanges(leaderIs("orders", 0, 1))
                        .addPartitionChanges(leaderIs("orders", 1, 1))
                        .build())
                .build()));

        assertThat(events).containsExactly(new Event("orders", 0, 0, 1), new Event("orders", 1, 0, 1));
    }

    @Test
    void fireOnLeaderEpochBumpForExistingPartition() {
        var events = new ArrayList<Event>();
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange(
                "orders", 0, 1, List.of(1, 2), List.of(1, 2, 3), /* leaderEpoch */ 3, /* partitionEpoch */ 0);
        var sm = new MetadataStateMachine(
                tm, new ProducerIdRegistry(), (t, p, e, l) -> events.add(new Event(t, p, e, l)));

        // Leader changes from 1 to 2; leader_epoch bumps.
        sm.apply(entry(MetadataRecord.newBuilder()
                .setPartitionChange(PartitionChangeRecord.newBuilder()
                        .setTopic("orders")
                        .setPartition(0)
                        .setLeader(2)
                        .addIsr(2)
                        .addIsr(3)
                        .addReplicas(1)
                        .addReplicas(2)
                        .addReplicas(3)
                        .setLeaderEpoch(4)
                        .setPartitionEpoch(0)
                        .build())
                .build()));

        assertThat(events).containsExactly(new Event("orders", 0, 4, 2));
    }

    @Test
    void doesNotFireOnPartitionEpochOnlyFlip() {
        var events = new ArrayList<Event>();
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, 1, List.of(1, 2, 3), List.of(1, 2, 3), 3, 0);
        var sm = new MetadataStateMachine(
                tm, new ProducerIdRegistry(), (t, p, e, l) -> events.add(new Event(t, p, e, l)));

        // ISR shrink under same leader_epoch — only partition_epoch bumps.
        sm.apply(entry(MetadataRecord.newBuilder()
                .setPartitionChange(PartitionChangeRecord.newBuilder()
                        .setTopic("orders")
                        .setPartition(0)
                        .setLeader(1)
                        .addIsr(1)
                        .addIsr(2)
                        .addReplicas(1)
                        .addReplicas(2)
                        .addReplicas(3)
                        .setLeaderEpoch(3)
                        .setPartitionEpoch(1)
                        .build())
                .build()));

        assertThat(events).isEmpty();
    }

    private static PartitionChangeRecord leaderIs(String topic, int partition, int broker) {
        return PartitionChangeRecord.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setLeader(broker)
                .addIsr(broker)
                .addReplicas(broker)
                .setLeaderEpoch(0)
                .build();
    }

    private static LogEntry entry(MetadataRecord record) {
        return new LogEntry(1L, new Term(1L), LogEntry.Type.NORMAL, record.toByteArray());
    }
}
