package jbroker.broker.replication;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import jbroker.broker.TopicManager;
import jbroker.proto.raft.MetadataRecord;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import jbroker.storage.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IsrManagerTest {

    private static final int SELF = 1;
    private static final long LAG_TIMEOUT_MS = 10_000L;

    @Test
    void decideReturnsEmptyWhenIsrIsHealthy(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, SELF, List.of(SELF, 2), List.of(SELF, 2), 0);
        var tracker = new FollowerStateTracker();
        try (var lm = lm(dir)) {
            lm.logFor("orders", 0).append(List.of(new Record(0, 0L, null, new byte[] {1})), 1_000L);
            tracker.record("orders", 0, 2, 1L, /* now */ 100_000L);

            var isr = new IsrManager(SELF, tm, lm, tracker, LAG_TIMEOUT_MS);
            var proposals = isr.decideChanges(/* now */ 105_000L);

            assertThat(proposals).isEmpty();
        }
    }

    @Test
    void internalTopicsGetIsrHousekeepingToo(@TempDir Path dir) throws Exception {
        // Regression: decideChanges used to iterate list(), which filters
        // internal topics — __consumer_offsets ISR was frozen at creation, so
        // a replica added by reassignment could never join and laggards never
        // shrank out. A caught-up out-of-ISR replica of an internal topic
        // must be proposed for expansion like any other.
        var tm = new TopicManager();
        tm.onTopicCommitted("__consumer_offsets", 1, 3, 0L);
        tm.onPartitionChange("__consumer_offsets", 0, SELF, List.of(SELF, 2), List.of(SELF, 2, 3), /* epoch */ 1);
        var tracker = new FollowerStateTracker();
        try (var lm = lm(dir)) {
            tracker.record("__consumer_offsets", 0, 2, 0L, /* fresh */ 100_000L);
            tracker.record("__consumer_offsets", 0, 3, 0L, /* fresh, caught up */ 100_000L);

            var isr = new IsrManager(SELF, tm, lm, tracker, LAG_TIMEOUT_MS);
            var proposals = isr.decideChanges(/* now */ 105_000L);

            assertThat(proposals).hasSize(1);
            var change = MetadataRecord.parseFrom(proposals.get(0)).getPartitionChange();
            assertThat(change.getTopic()).isEqualTo("__consumer_offsets");
            assertThat(change.getIsrList()).containsExactlyInAnyOrder(SELF, 2, 3);
        }
    }

    @Test
    void decideProposesIsrShrinkWhenFollowerIsStale(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, SELF, List.of(SELF, 2, 3), List.of(SELF, 2, 3), /* epoch */ 5);
        var tracker = new FollowerStateTracker();
        try (var lm = lm(dir)) {
            tracker.record("orders", 0, 2, 1L, /* fresh */ 100_000L);
            tracker.record("orders", 0, 3, 1L, /* stale */ 50_000L);

            var isr = new IsrManager(SELF, tm, lm, tracker, LAG_TIMEOUT_MS);
            var proposals = isr.decideChanges(/* now */ 105_000L);

            assertThat(proposals).hasSize(1);
            var change = MetadataRecord.parseFrom(proposals.get(0)).getPartitionChange();
            assertThat(change.getTopic()).isEqualTo("orders");
            assertThat(change.getPartition()).isEqualTo(0);
            assertThat(change.getIsrList()).containsExactly(SELF, 2);
            assertThat(change.getReplicasList()).containsExactly(SELF, 2, 3); // unchanged
            // ISR flip is a partition-epoch event, not a leader-epoch event.
            assertThat(change.getLeaderEpoch()).isEqualTo(5);
            assertThat(change.getPartitionEpoch()).isEqualTo(1);
            // CAS guard: derived from (le=5, pe=0); a leader change or
            // competing flip landing first must invalidate this proposal.
            assertThat(change.hasPriorLeaderEpoch()).isTrue();
            assertThat(change.getPriorLeaderEpoch()).isEqualTo(5);
            assertThat(change.getPriorPartitionEpoch()).isZero();
        }
    }

    @Test
    void decideProposesIsrExpandWhenOutOfIsrReplicaCatchesUp(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        // Replica 3 is in the replica set but currently out of ISR.
        tm.onPartitionChange("orders", 0, SELF, List.of(SELF, 2), List.of(SELF, 2, 3), /* epoch */ 5);
        var tracker = new FollowerStateTracker();
        try (var lm = lm(dir)) {
            lm.logFor("orders", 0).append(List.of(new Record(0, 0L, null, new byte[] {1})), 1_000L);
            tracker.record("orders", 0, 2, /* leo */ 1L, 100_000L);
            tracker.record("orders", 0, 3, /* leo caught up */ 1L, 100_000L);

            var isr = new IsrManager(SELF, tm, lm, tracker, LAG_TIMEOUT_MS);
            var proposals = isr.decideChanges(105_000L);

            assertThat(proposals).hasSize(1);
            var change = MetadataRecord.parseFrom(proposals.get(0)).getPartitionChange();
            assertThat(change.getIsrList()).containsExactly(SELF, 2, 3);
            assertThat(change.getLeaderEpoch()).isEqualTo(5);
            assertThat(change.getPartitionEpoch()).isEqualTo(1);
        }
    }

    @Test
    void decideDoesNotExpandWhenCandidateLeoBelowHwm(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, SELF, List.of(SELF, 2), List.of(SELF, 2, 3), 5, 0);
        var tracker = new FollowerStateTracker();
        try (var lm = lm(dir)) {
            lm.logFor("orders", 0).append(List.of(new Record(0, 0L, null, new byte[] {1})), 1_000L);
            // HWM will be 1 (leader + broker 2 both at LEO=1).
            tracker.record("orders", 0, 2, 1L, 100_000L);
            // Broker 3 only caught up to LEO=0 — still behind.
            tracker.record("orders", 0, 3, 0L, 100_000L);

            var isr = new IsrManager(SELF, tm, lm, tracker, LAG_TIMEOUT_MS);
            assertThat(isr.decideChanges(105_000L)).isEmpty();
        }
    }

    @Test
    void decideSkipsPartitionsWhereSelfIsNotLeader(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, /* other leader */ 2, List.of(2, SELF), List.of(2, SELF), 0);
        var tracker = new FollowerStateTracker();

        try (var lm = lm(dir)) {
            var isr = new IsrManager(SELF, tm, lm, tracker, LAG_TIMEOUT_MS);
            var proposals = isr.decideChanges(100_000L);
            assertThat(proposals).isEmpty();
        }
    }

    @Test
    void decideDoesNotShrinkBelowOneMember(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 1, 0L);
        tm.onPartitionChange("orders", 0, SELF, List.of(SELF), List.of(SELF), 0);
        var tracker = new FollowerStateTracker();

        try (var lm = lm(dir)) {
            var isr = new IsrManager(SELF, tm, lm, tracker, LAG_TIMEOUT_MS);
            // Everyone "lagged" by never fetching, but we can't shrink out
            // the leader itself — ISR must always contain the leader.
            var proposals = isr.decideChanges(100_000L);
            assertThat(proposals).isEmpty();
        }
    }

    @Test
    void decideAccumulatesProposalsAcrossPartitions(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 2, 3, 0L);
        tm.onPartitionChange("orders", 0, SELF, List.of(SELF, 2), List.of(SELF, 2, 3), 0);
        tm.onPartitionChange("orders", 1, SELF, List.of(SELF, 2, 3), List.of(SELF, 2, 3), 0);
        var tracker = new FollowerStateTracker();
        try (var lm = lm(dir)) {
            lm.logFor("orders", 0).append(List.of(new Record(0, 0L, null, new byte[] {1})), 1_000L);
            lm.logFor("orders", 1).append(List.of(new Record(0, 0L, null, new byte[] {1})), 1_000L);
            // Partition 0: broker 3 caught up → expand.
            tracker.record("orders", 0, 2, 1L, 100_000L);
            tracker.record("orders", 0, 3, 1L, 100_000L);
            // Partition 1: broker 3 is stale → shrink.
            tracker.record("orders", 1, 2, 1L, 100_000L);
            tracker.record("orders", 1, 3, 1L, /* stale */ 50_000L);

            var isr = new IsrManager(SELF, tm, lm, tracker, LAG_TIMEOUT_MS);
            var proposals = isr.decideChanges(105_000L);

            assertThat(proposals).hasSize(2);
            var changes = new ArrayList<List<Integer>>();
            for (var p : proposals) {
                changes.add(MetadataRecord.parseFrom(p).getPartitionChange().getIsrList());
            }
            assertThat(changes).containsExactlyInAnyOrder(List.of(SELF, 2, 3), List.of(SELF, 2));
        }
    }

    private static LogManager lm(Path dir) throws Exception {
        return new LogManager(
                dir,
                new LogManager.Config(
                        128L * 1024 * 1024,
                        Long.MAX_VALUE,
                        LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        TimeUnit.MINUTES.toMillis(5)));
    }
}
