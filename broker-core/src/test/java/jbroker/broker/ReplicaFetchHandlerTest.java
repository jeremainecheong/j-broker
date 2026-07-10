package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import jbroker.broker.replication.FollowerStateTracker;
import jbroker.proto.broker.ReplicaFetchRequest;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import jbroker.storage.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReplicaFetchHandlerTest {

    private static final int LEADER = 1;
    private static final int FOLLOWER = 2;

    private final FollowerStateTracker tracker = new FollowerStateTracker();
    private final AtomicLong clock = new AtomicLong(1_700_000_000_000L);
    private final java.util.function.LongSupplier clockFn = clock::get;

    @Test
    void returnsBatchesAndHwmWhenEpochMatches(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, LEADER, List.of(LEADER, FOLLOWER), 0);

        try (var lm = lm(dir)) {
            // Leader already has one batch written locally.
            lm.logFor("orders", 0).append(List.of(new Record(0, 0L, null, new byte[] {1, 2, 3})), 1_000L);

            var handler = new ReplicaFetchHandler(lm, tm, LEADER, tracker, clockFn);
            var req = ReplicaFetchRequest.newBuilder()
                    .setTopic("orders")
                    .setPartition(0)
                    .setFollowerBrokerId(FOLLOWER)
                    .setLeaderEpoch(0)
                    .setFetchOffset(0L)
                    .build();

            var resp = handler.handle(req);
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(resp.getCurrentLeaderEpoch()).isEqualTo(0);
            assertThat(resp.getHighWatermark()).isGreaterThanOrEqualTo(0L);
            assertThat(resp.getRecords().size()).isGreaterThan(0);
        }
    }

    @Test
    void rejectsWhenSelfIsNotPartitionLeader(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, FOLLOWER, List.of(FOLLOWER, LEADER), 0);

        try (var lm = lm(dir)) {
            var handler = new ReplicaFetchHandler(lm, tm, LEADER, tracker, clockFn);
            var resp = handler.handle(request(FOLLOWER, 0, 0L));
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NOT_LEADER);
        }
    }

    @Test
    void rejectsWhenFollowerHasStaleEpoch(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, LEADER, List.of(LEADER, FOLLOWER), /* current epoch */ 5);

        try (var lm = lm(dir)) {
            var handler = new ReplicaFetchHandler(lm, tm, LEADER, tracker, clockFn);
            // Follower thinks the leader epoch is 3; really it's 5.
            var resp = handler.handle(request(FOLLOWER, /* stale epoch */ 3, 0L));
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.FENCED_EPOCH);
            assertThat(resp.getCurrentLeaderEpoch()).isEqualTo(5);
        }
    }

    @Test
    void rejectsFetchFromBrokerNotInIsr(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        // ISR is [LEADER, FOLLOWER]; an impostor broker id 99 is not a replica.
        tm.onPartitionChange("orders", 0, LEADER, List.of(LEADER, FOLLOWER), 0);

        try (var lm = lm(dir)) {
            var handler = new ReplicaFetchHandler(lm, tm, LEADER, tracker, clockFn);
            var resp = handler.handle(request(/* impostor */ 99, 0, 0L));
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NOT_LEADER);
            assertThat(resp.getError().getMessage()).contains("not a replica");
        }
    }

    @Test
    void handleRecordsFollowerLeoAndAdvancesHwm(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, LEADER, List.of(LEADER, FOLLOWER), 0);

        try (var lm = lm(dir)) {
            // Leader has 5 records.
            var log = lm.logFor("orders", 0);
            for (int i = 0; i < 5; i++) {
                log.append(List.of(new Record(0, 0L, null, new byte[] {(byte) i})), 1_000L);
            }

            var handler = new ReplicaFetchHandler(lm, tm, LEADER, tracker, clockFn);

            // Follower has replicated 3 records and asks for offset 3.
            var resp = handler.handle(request(FOLLOWER, 0, 3L));
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            // Tracker recorded follower's LEO = 3.
            assertThat(tracker.get("orders", 0, FOLLOWER).orElseThrow().leo()).isEqualTo(3L);
            // HWM = min(leader LEO=5, follower LEO=3) = 3.
            assertThat(resp.getHighWatermark()).isEqualTo(3L);
        }
    }

    @Test
    void handleDoesNotRecordLeoWhenRequestIsRejected(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, LEADER, List.of(LEADER, FOLLOWER), /* epoch */ 5);

        try (var lm = lm(dir)) {
            var handler = new ReplicaFetchHandler(lm, tm, LEADER, tracker, clockFn);
            // Stale epoch rejection — tracker must NOT update, or a fenced
            // follower would artificially hold back the HWM.
            handler.handle(request(FOLLOWER, /* stale */ 2, 0L));
            assertThat(tracker.get("orders", 0, FOLLOWER)).isEmpty();
        }
    }

    @Test
    void returnsEmptyRecordsWhenFollowerIsAtLogEnd(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, LEADER, List.of(LEADER, FOLLOWER), 0);

        try (var lm = lm(dir)) {
            var log = lm.logFor("orders", 0);
            log.append(List.of(new Record(0, 0L, null, new byte[] {7})), 1_000L);

            var handler = new ReplicaFetchHandler(lm, tm, LEADER, tracker, clockFn);
            var resp = handler.handle(request(FOLLOWER, 0, log.nextOffset()));
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(resp.getRecords().size()).isZero();
        }
    }

    @Test
    void fencesLineageDivergenceEvenWhenMetadataEpochMatches(@TempDir Path dir) throws Exception {
        // The soak-v5 glue bug: a follower whose metadata epoch is current
        // but whose LOG tail was written under a rejected lineage fetched
        // from its LEO and spliced the leader's records on top of junk.
        // The leader must fence when the follower's last-batch epoch does
        // not match its own lineage at fetch_offset - 1.
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, LEADER, List.of(LEADER, FOLLOWER), List.of(LEADER, FOLLOWER), 2, 0);

        try (var lm = lm(dir)) {
            var log = lm.logFor("orders", 0);
            log.append(List.of(new Record(0, 0L, null, new byte[] {1})), 1_000L, -1L, (short) -1, -1, 1);
            log.append(List.of(new Record(0, 0L, null, new byte[] {2})), 1_000L, -1L, (short) -1, -1, 2);
            lm.leaderEpochCheckpoint("orders", 0).assign(1, 0L);
            lm.leaderEpochCheckpoint("orders", 0).assign(2, 1L);

            var handler = new ReplicaFetchHandler(lm, tm, LEADER, tracker, clockFn);
            var resp = handler.handle(request(FOLLOWER, 2, 2L).toBuilder()
                    .setLastFetchedEpoch(1) // leader lineage at offset 1 is epoch 2
                    .build());

            assertThat(resp.hasError()).isTrue();
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.FENCED_EPOCH);
            assertThat(resp.getError().getMessage()).contains("lineage");
        }
    }

    @Test
    void servesWhenLineageMatches(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, LEADER, List.of(LEADER, FOLLOWER), List.of(LEADER, FOLLOWER), 2, 0);

        try (var lm = lm(dir)) {
            var log = lm.logFor("orders", 0);
            log.append(List.of(new Record(0, 0L, null, new byte[] {1})), 1_000L, -1L, (short) -1, -1, 1);
            log.append(List.of(new Record(0, 0L, null, new byte[] {2})), 1_000L, -1L, (short) -1, -1, 2);
            lm.leaderEpochCheckpoint("orders", 0).assign(1, 0L);
            lm.leaderEpochCheckpoint("orders", 0).assign(2, 1L);

            var handler = new ReplicaFetchHandler(lm, tm, LEADER, tracker, clockFn);
            var resp = handler.handle(request(FOLLOWER, 2, 1L).toBuilder()
                    .setLastFetchedEpoch(1) // offset 0 belongs to epoch 1 — matches
                    .build());

            assertThat(resp.hasError()).isFalse();
            assertThat(resp.getRecords().isEmpty()).isFalse();
        }
    }

    @Test
    void legacyFetchWithoutLineageFieldIsServed(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, LEADER, List.of(LEADER, FOLLOWER), List.of(LEADER, FOLLOWER), 2, 0);

        try (var lm = lm(dir)) {
            lm.logFor("orders", 0)
                    .append(List.of(new Record(0, 0L, null, new byte[] {1})), 1_000L, -1L, (short) -1, -1, 2);
            lm.leaderEpochCheckpoint("orders", 0).assign(2, 0L);

            var handler = new ReplicaFetchHandler(lm, tm, LEADER, tracker, clockFn);
            var resp = handler.handle(request(FOLLOWER, 2, 1L));

            assertThat(resp.hasError()).isFalse();
        }
    }

    private static ReplicaFetchRequest request(int follower, int epoch, long offset) {
        return ReplicaFetchRequest.newBuilder()
                .setTopic("orders")
                .setPartition(0)
                .setFollowerBrokerId(follower)
                .setLeaderEpoch(epoch)
                .setFetchOffset(offset)
                .build();
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
