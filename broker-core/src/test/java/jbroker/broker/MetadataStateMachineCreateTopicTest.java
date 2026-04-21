package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import jbroker.proto.raft.CreateTopicRecord;
import jbroker.proto.raft.MetadataRecord;
import jbroker.proto.raft.PartitionChangeRecord;
import jbroker.proto.raft.TopicRecord;
import jbroker.raft.core.LogEntry;
import jbroker.raft.core.Term;
import org.junit.jupiter.api.Test;

class MetadataStateMachineCreateTopicTest {

    @Test
    void appliesTopicAndAllPartitionChangesAtomically() {
        var tm = new TopicManager();
        var sm = new MetadataStateMachine(tm);

        var record = MetadataRecord.newBuilder()
                .setCreateTopic(CreateTopicRecord.newBuilder()
                        .setTopic(TopicRecord.newBuilder()
                                .setTopic("orders")
                                .setPartitions(3)
                                .setReplicationFactor(1)
                                .setCreatedMillis(1_700_000_000L)
                                .build())
                        .addPartitionChanges(leaderIs("orders", 0, 1))
                        .addPartitionChanges(leaderIs("orders", 1, 1))
                        .addPartitionChanges(leaderIs("orders", 2, 1))
                        .build())
                .build();

        sm.apply(new LogEntry(1L, new Term(1L), LogEntry.Type.NORMAL, record.toByteArray()));

        assertThat(tm.exists("orders")).isTrue();
        assertThat(tm.partitionLeader("orders", 0)).contains(1);
        assertThat(tm.partitionLeader("orders", 1)).contains(1);
        assertThat(tm.partitionLeader("orders", 2)).contains(1);
        assertThat(tm.partitionLeaderEpoch("orders", 0)).contains(0);
    }

    private static PartitionChangeRecord leaderIs(String topic, int partition, int broker) {
        return PartitionChangeRecord.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setLeader(broker)
                .addIsr(broker)
                .setLeaderEpoch(0)
                .build();
    }
}
