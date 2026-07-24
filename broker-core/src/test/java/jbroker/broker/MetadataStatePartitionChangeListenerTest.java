package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import jbroker.proto.raft.MetadataRecord;
import jbroker.proto.raft.PartitionChangeRecord;
import jbroker.raft.core.LogEntry;
import jbroker.raft.core.Term;
import org.junit.jupiter.api.Test;

class MetadataStatePartitionChangeListenerTest {

    @Test
    void partitionChangeListenerFiresOnEveryAppliedRecord() {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        var firings = new AtomicInteger();
        MetadataStateMachine.PartitionChangeListener listener = (topic, partition, state) -> firings.incrementAndGet();
        var sm = new MetadataStateMachine(
                tm, new ProducerIdRegistry(), (t, p, e, l) -> {}, (bid, h, pt, ah, ap, rp) -> {}, listener);

        // First leader_epoch bump -> fire.
        sm.apply(wrap(record("orders", 0, /*leader*/ 1, List.of(1, 2, 3), /*epoch*/ 5, /*partEpoch*/ 0), 1));
        // Same leader_epoch, higher partitionEpoch (ISR shrink) -> fire.
        sm.apply(wrap(record("orders", 0, 1, List.of(1, 2), 5, 1), 2));
        // Same leader_epoch, same partitionEpoch (duplicate) -> still fire,
        // but TopicManager merge keeps the prior state.
        sm.apply(wrap(record("orders", 0, 1, List.of(1, 2), 5, 1), 3));

        assertThat(firings.get()).isEqualTo(3);
        assertThat(tm.partitionState("orders", 0).orElseThrow().isr()).containsExactly(1, 2);
    }

    private static PartitionChangeRecord record(
            String topic, int partition, int leader, List<Integer> isr, int leaderEpoch, int partitionEpoch) {
        return PartitionChangeRecord.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setLeader(leader)
                .addAllIsr(isr)
                .addAllReplicas(List.of(1, 2, 3))
                .setLeaderEpoch(leaderEpoch)
                .setPartitionEpoch(partitionEpoch)
                .build();
    }

    private static LogEntry wrap(PartitionChangeRecord p, long index) {
        byte[] bytes = MetadataRecord.newBuilder().setPartitionChange(p).build().toByteArray();
        return new LogEntry(index, new Term(1), LogEntry.Type.NORMAL, bytes);
    }
}
