package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import jbroker.proto.raft.MetadataRecord;
import org.junit.jupiter.api.Test;

/**
 * Verifies {@link ConsumerOffsetsCreator} only proposes when the
 * caller is the leader, only proposes once, clamps RF correctly, and emits
 * exactly {@link ConsumerOffsetsTopic#PARTITION_COUNT} partitions.
 */
class ConsumerOffsetsCreatorTest {

    @Test
    void doesNothingWhenNotLeader() throws Exception {
        var tm = new TopicManager();
        var proposed = new java.util.ArrayList<byte[]>();
        var creator = new ConsumerOffsetsCreator(tm, () -> Set.of(1), 1, proposed::add, () -> false);

        boolean result = creator.ensureCreated();

        assertThat(result).isFalse();
        assertThat(proposed).isEmpty();
    }

    @Test
    void doesNothingWhenNoBrokersKnownYet() {
        var tm = new TopicManager();
        var proposed = new java.util.ArrayList<byte[]>();
        var creator = new ConsumerOffsetsCreator(tm, Set::of, 1, proposed::add, () -> true);

        assertThat(creator.ensureCreated()).isFalse();
        assertThat(proposed).isEmpty();
    }

    @Test
    void proposesExactlyOnceWhenLeaderAndAbsent() throws Exception {
        var tm = new TopicManager();
        var proposed = new java.util.ArrayList<byte[]>();
        var creator = new ConsumerOffsetsCreator(tm, () -> Set.of(1), 1, proposed::add, () -> true);

        assertThat(creator.ensureCreated()).isTrue();
        assertThat(creator.ensureCreated()).isFalse();
        assertThat(proposed).hasSize(1);

        var record = MetadataRecord.parseFrom(proposed.get(0));
        assertThat(record.hasCreateTopic()).isTrue();
        var ct = record.getCreateTopic();
        assertThat(ct.getTopic().getTopic()).isEqualTo("__consumer_offsets");
        assertThat(ct.getTopic().getPartitions()).isEqualTo(50);
        assertThat(ct.getTopic().getReplicationFactor()).isEqualTo(1);
        assertThat(ct.getTopic().getInternal()).isTrue();
        assertThat(ct.getTopic().getCompact()).isTrue();
        assertThat(ct.getPartitionChangesList()).hasSize(50);
    }

    @Test
    void topicNameFormCreatesTransactionStateWithItsCanonicalShape() throws Exception {
        var tm = new TopicManager();
        var proposed = new java.util.ArrayList<byte[]>();
        var creator = new ConsumerOffsetsCreator(
                tm,
                () -> Set.of(1, 2, 3),
                1,
                proposed::add,
                () -> true,
                jbroker.broker.txn.TxnStateTopic.PARTITION_COUNT,
                jbroker.broker.txn.TxnStateTopic.NAME);

        assertThat(creator.ensureCreated()).isTrue();
        var ct = MetadataRecord.parseFrom(proposed.get(0)).getCreateTopic();
        assertThat(ct.getTopic().getTopic()).isEqualTo("__transaction_state");
        assertThat(ct.getTopic().getPartitions()).isEqualTo(50);
        assertThat(ct.getTopic().getInternal()).isTrue();
        assertThat(ct.getTopic().getCompact()).isTrue();
        assertThat(ct.getTopic().getReplicationFactor()).isEqualTo(3);
        assertThat(ct.getPartitionChangesList()).hasSize(50);
    }

    @Test
    void clampsReplicationFactorToMinOfThreeAndKnownBrokers() throws Exception {
        var tm = new TopicManager();
        var proposed = new java.util.ArrayList<byte[]>();
        var creator = new ConsumerOffsetsCreator(tm, () -> Set.of(1, 2, 3, 4, 5), 1, proposed::add, () -> true);

        creator.ensureCreated();

        var ct = MetadataRecord.parseFrom(proposed.get(0)).getCreateTopic();
        assertThat(ct.getTopic().getReplicationFactor()).isEqualTo(3);
        // Each partition record carries 3 replicas (the chosen RF).
        ct.getPartitionChangesList()
                .forEach(pc -> assertThat(pc.getReplicasList()).hasSize(3));
    }

    @Test
    void leadershipRoundRobinsAcrossReplicas() throws Exception {
        var tm = new TopicManager();
        var proposed = new java.util.ArrayList<byte[]>();
        var creator = new ConsumerOffsetsCreator(tm, () -> Set.of(1, 2, 3), 1, proposed::add, () -> true);

        creator.ensureCreated();
        var ct = MetadataRecord.parseFrom(proposed.get(0)).getCreateTopic();

        var leaderCounts = new java.util.HashMap<Integer, Integer>();
        ct.getPartitionChangesList().forEach(pc -> leaderCounts.merge(pc.getLeader(), 1, Integer::sum));

        // 50 partitions / 3 replicas = 17 + 17 + 16. Each replica leads at
        // least 16 partitions; max-min ≤ 1.
        assertThat(leaderCounts.values()).allMatch(c -> c >= 16 && c <= 17);
    }

    @Test
    void doesNothingWhenTopicAlreadyExists() {
        var tm = new TopicManager();
        tm.onTopicCommitted("__consumer_offsets", 50, 1, 0L, true, true);
        var proposed = new java.util.ArrayList<byte[]>();
        var creator = new ConsumerOffsetsCreator(tm, () -> Set.of(1), 1, proposed::add, () -> true);

        assertThat(creator.ensureCreated()).isTrue();
        assertThat(proposed).isEmpty();
    }

    @Test
    void retriesAfterProposeFailure() {
        var tm = new TopicManager();
        var firstAttempt = new AtomicBoolean(true);
        ConsumerOffsetsCreator.Proposer flaky = payload -> {
            if (firstAttempt.getAndSet(false)) {
                throw new IllegalStateException("not leader yet");
            }
        };
        var creator = new ConsumerOffsetsCreator(tm, () -> Set.of(1), 1, flaky, () -> true);

        assertThat(creator.ensureCreated()).isFalse();
        // The retry attempt succeeds — implementation lets the caller
        // re-tick after the first failure.
        assertThat(creator.ensureCreated()).isTrue();
    }
}
