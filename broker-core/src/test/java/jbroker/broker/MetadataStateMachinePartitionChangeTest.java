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
}
