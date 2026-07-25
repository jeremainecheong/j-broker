package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import jbroker.broker.replication.FollowerStateTracker;
import jbroker.proto.broker.ProduceRequest;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ProduceHandlerAcksAllTest {

    private static LogManager lm(Path dir) throws Exception {
        return new LogManager(
                dir,
                new LogManager.Config(
                        128L * 1024 * 1024,
                        Long.MAX_VALUE,
                        LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        TimeUnit.MINUTES.toMillis(5)));
    }

    private static byte[] singleRecordBatch(byte[] value) {
        var records = List.of(new Record(0, 0L, null, value));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        long now = 1_000L;
        RecordBatch.encode(buf, 0L, 0, now, now, -1L, (short) -1, -1, records);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return bytes;
    }

    @Test
    void acksAllSucceedsWhenIsrReplicatesFast(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 3, 0L);
        // Leader = 1, ISR = [1, 2, 3].
        tm.onPartitionChange("t", 0, 1, List.of(1, 2, 3), List.of(1, 2, 3), 0, 0);

        try (var lmgr = lm(dir)) {
            var tracker = new FollowerStateTracker();
            // Followers pre-seed as caught up to leader LEO=0 so HWM starts at 0.
            tracker.record("t", 0, 2, 0L, 1_000L);
            tracker.record("t", 0, 3, 0L, 1_000L);

            var handler = new ProduceHandler(lmgr, tm, 1, tracker);

            // Fire acks=all produce in a separate thread; while it blocks,
            // advance the followers' LEOs to simulate replication.
            var future =
                    java.util.concurrent.CompletableFuture.supplyAsync(() -> handler.handle(ProduceRequest.newBuilder()
                            .setTopic("t")
                            .setPartition(0)
                            .setBatch(ByteString.copyFrom(singleRecordBatch("v".getBytes())))
                            .setAcks(-1)
                            .setProducerId(-1L)
                            .setBaseSequence(-1)
                            .build()));
            // Give the handler time to append and start polling HWM.
            Thread.sleep(50);
            tracker.record("t", 0, 2, 1L, 2_000L);
            tracker.record("t", 0, 3, 1L, 2_000L);

            var resp = future.get(3, TimeUnit.SECONDS);
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(resp.getLastOffset()).isEqualTo(0L);
        }
    }

    @Test
    void acksAllTimesOutWhenFollowerNeverCatchesUp(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 3, 0L);
        tm.onPartitionChange("t", 0, 1, List.of(1, 2, 3), List.of(1, 2, 3), 0, 0);

        try (var lmgr = lm(dir)) {
            var tracker = new FollowerStateTracker();
            // Broker 3 never fetches — its LEO stays unknown. HWM can't
            // advance past 0; handler times out after 5s.
            tracker.record("t", 0, 2, 0L, 1_000L);

            var handler = new ProduceHandler(lmgr, tm, 1, tracker);

            long start = System.nanoTime();
            var resp = handler.handle(ProduceRequest.newBuilder()
                    .setTopic("t")
                    .setPartition(0)
                    .setBatch(ByteString.copyFrom(singleRecordBatch("v".getBytes())))
                    .setAcks(-1)
                    .setProducerId(-1L)
                    .setBaseSequence(-1)
                    .build());
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;

            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NOT_ENOUGH_REPLICAS);
            // Sanity: we actually waited rather than short-circuiting.
            assertThat(elapsedMs).isGreaterThanOrEqualTo(4_500L);
        }
    }

    @Test
    void acksDefaultOneDoesNotWaitOnIsr(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 3, 0L);
        tm.onPartitionChange("t", 0, 1, List.of(1, 2, 3), List.of(1, 2, 3), 0, 0);

        try (var lmgr = lm(dir)) {
            var tracker = new FollowerStateTracker();
            // No follower state at all — acks=1 should not care.
            var handler = new ProduceHandler(lmgr, tm, 1, tracker);

            long start = System.nanoTime();
            var resp = handler.handle(ProduceRequest.newBuilder()
                    .setTopic("t")
                    .setPartition(0)
                    .setBatch(ByteString.copyFrom(singleRecordBatch("v".getBytes())))
                    .setAcks(1)
                    .setProducerId(-1L)
                    .setBaseSequence(-1)
                    .build());
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;

            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(resp.getLastOffset()).isEqualTo(0L);
            // Must not have blocked on replication.
            assertThat(elapsedMs).isLessThan(500L);
        }
    }

    @Test
    void acksAllRejectedBeforeAppendWhenIsrBelowFloor(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 3, 0L);
        // ISR already shrunk to just the leader — the #115 shape. The
        // default floor (2) must reject BEFORE appending anything.
        tm.onPartitionChange("t", 0, 1, List.of(1), List.of(1, 2, 3), 0, 0);

        try (var lmgr = lm(dir)) {
            var metrics = new BrokerMetrics();
            var handler = new ProduceHandler(lmgr, tm, 1, new FollowerStateTracker(), metrics);

            long start = System.nanoTime();
            var resp = handler.handle(ProduceRequest.newBuilder()
                    .setTopic("t")
                    .setPartition(0)
                    .setBatch(ByteString.copyFrom(singleRecordBatch("v".getBytes())))
                    .setAcks(-1)
                    .setProducerId(-1L)
                    .setBaseSequence(-1)
                    .build());
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;

            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NOT_ENOUGH_REPLICAS);
            assertThat(resp.getError().getMessage()).contains("min.insync.replicas");
            // Rejected pre-append: fast, and the log is untouched.
            assertThat(elapsedMs).isLessThan(1_000L);
            assertThat(lmgr.logFor("t", 0).nextOffset()).isZero();
            // The rejection is observable: the pre-append floor check is
            // one of the NOT_ENOUGH_REPLICAS counter's increment sites.
            assertThat(metrics.notEnoughReplicasRejections()).isEqualTo(1L);
        }
    }

    @Test
    void acksAllFailsFastWhenIsrShrinksBelowFloorMidWait(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 3, 0L);
        tm.onPartitionChange("t", 0, 1, List.of(1, 2), List.of(1, 2, 3), 0, 0);

        try (var lmgr = lm(dir)) {
            var tracker = new FollowerStateTracker();
            tracker.record("t", 0, 2, 0L, 1_000L);

            var metrics = new BrokerMetrics();
            var handler = new ProduceHandler(lmgr, tm, 1, tracker, metrics);

            // Produce blocks: follower 2 never advances past LEO 0.
            var future =
                    java.util.concurrent.CompletableFuture.supplyAsync(() -> handler.handle(ProduceRequest.newBuilder()
                            .setTopic("t")
                            .setPartition(0)
                            .setBatch(ByteString.copyFrom(singleRecordBatch("v".getBytes())))
                            .setAcks(-1)
                            .setProducerId(-1L)
                            .setBaseSequence(-1)
                            .build()));
            Thread.sleep(50);
            // ISR shrinks to just the leader (partition-epoch bump, same
            // leader epoch). Without the floor re-check this is the #115
            // false-ack: computeHwm over {1} jumps to the leader LEO and
            // the produce would be acked with one copy.
            tm.onPartitionChange("t", 0, 1, List.of(1), List.of(1, 2, 3), 0, 1);

            // Well inside the 5s acks=all budget — the shrink fails it fast.
            var resp = future.get(3, TimeUnit.SECONDS);
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NOT_ENOUGH_REPLICAS);
            assertThat(resp.getError().getMessage()).contains("min.insync.replicas");
            // Post-append replication-wait failures count too.
            assertThat(metrics.notEnoughReplicasRejections()).isEqualTo(1L);
        }
    }

    @Test
    void explicitTopicOverrideOfOneAllowsSoloIsr(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        // Operator explicitly opted out of the floor for this topic.
        tm.onTopicCommitted(
                "t", 1, 3, 0L, false, false, java.util.Map.of(TopicDescription.MIN_INSYNC_REPLICAS_CONFIG, "1"));
        tm.onPartitionChange("t", 0, 1, List.of(1), List.of(1, 2, 3), 0, 0);

        try (var lmgr = lm(dir)) {
            var handler = new ProduceHandler(lmgr, tm, 1, new FollowerStateTracker());

            var resp = handler.handle(ProduceRequest.newBuilder()
                    .setTopic("t")
                    .setPartition(0)
                    .setBatch(ByteString.copyFrom(singleRecordBatch("v".getBytes())))
                    .setAcks(-1)
                    .setProducerId(-1L)
                    .setBaseSequence(-1)
                    .build());

            // Floor 1 + ISR {leader}: HWM is the leader LEO, instant ack.
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(resp.getLastOffset()).isEqualTo(0L);
        }
    }

    @Test
    void clusterDefaultClampsToReplicationFactorForSingleReplicaTopics(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        // RF=1 dev topic: the cluster default of 2 clamps down to 1, so
        // acks=all keeps working on single-broker setups.
        tm.onTopicCommitted("t", 1, 1, 0L);
        tm.onPartitionChange("t", 0, 1, List.of(1), List.of(1), 0, 0);

        try (var lmgr = lm(dir)) {
            var handler = new ProduceHandler(lmgr, tm, 1, new FollowerStateTracker());

            var resp = handler.handle(ProduceRequest.newBuilder()
                    .setTopic("t")
                    .setPartition(0)
                    .setBatch(ByteString.copyFrom(singleRecordBatch("v".getBytes())))
                    .setAcks(-1)
                    .setProducerId(-1L)
                    .setBaseSequence(-1)
                    .build());

            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(resp.getLastOffset()).isEqualTo(0L);
        }
    }

    private static ProduceRequest idempotentAcksAll(String value, long pid, int seq) {
        return ProduceRequest.newBuilder()
                .setTopic("t")
                .setPartition(0)
                .setBatch(ByteString.copyFrom(singleRecordBatch(value.getBytes())))
                .setAcks(-1)
                .setProducerId(pid)
                .setProducerEpoch(0)
                .setBaseSequence(seq)
                .build();
    }

    @Test
    void retryAfterFailedReplicationWaitDoesNotAppendASecondCopy(@TempDir Path dir) throws Exception {
        // The soak-v5 duplicate generator: an acks=all produce APPENDS,
        // then fails its replication wait (here: ISR below the floor after
        // append). The batch is on disk; dedup state must reflect that, or
        // every retry of the same (pid, seq) appends another copy — the
        // soak found eight copies of one record at consecutive offsets,
        // seeding permanent replica divergence.
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 3, 0L);
        tm.onPartitionChange("t", 0, 1, List.of(1, 2), List.of(1, 2, 3), 0, 0);

        try (var lmgr = lm(dir)) {
            var tracker = new FollowerStateTracker();
            tracker.record("t", 0, 2, 0L, 1_000L);
            var handler = new ProduceHandler(lmgr, tm, 1, tracker);

            // First attempt: appends, then the ISR shrinks below the floor
            // mid-wait → NOT_ENOUGH_REPLICAS, but offset 0 is on disk.
            var attempt = java.util.concurrent.CompletableFuture.supplyAsync(
                    () -> handler.handle(idempotentAcksAll("v", 7L, 0)));
            Thread.sleep(50);
            tm.onPartitionChange("t", 0, 1, List.of(1), List.of(1, 2, 3), 0, 1);
            assertThat(attempt.get(3, TimeUnit.SECONDS).getError().getCode()).isEqualTo(ErrorCodes.NOT_ENOUGH_REPLICAS);
            assertThat(lmgr.logFor("t", 0).nextOffset()).isEqualTo(1L);

            // Retry while still degraded: pre-append gate rejects, log untouched.
            assertThat(handler.handle(idempotentAcksAll("v", 7L, 0)).getError().getCode())
                    .isEqualTo(ErrorCodes.NOT_ENOUGH_REPLICAS);
            assertThat(lmgr.logFor("t", 0).nextOffset()).isEqualTo(1L);

            // ISR recovers and the follower catches up: the retry must ack
            // the ORIGINAL offsets — no second copy, ever.
            tm.onPartitionChange("t", 0, 1, List.of(1, 2), List.of(1, 2, 3), 0, 2);
            tracker.record("t", 0, 2, 1L, 2_000L);
            var resp = handler.handle(idempotentAcksAll("v", 7L, 0));
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(resp.getBaseOffset()).isZero();
            assertThat(resp.getLastOffset()).isZero();
            assertThat(lmgr.logFor("t", 0).nextOffset()).isEqualTo(1L);
        }
    }

    @Test
    void cachedRetryStillWaitsForReplication(@TempDir Path dir) throws Exception {
        // "Seen before" must not imply "committed": a cached dedup hit for
        // an acks=all retry has to run the replication wait on the cached
        // offsets, not return instant success.
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 3, 0L);
        tm.onPartitionChange("t", 0, 1, List.of(1, 2), List.of(1, 2, 3), 0, 0);

        try (var lmgr = lm(dir)) {
            var tracker = new FollowerStateTracker();
            tracker.record("t", 0, 2, 0L, 1_000L);
            var handler = new ProduceHandler(lmgr, tm, 1, tracker);

            // Seed dedup state via acks=1 (appends + records, no wait).
            var seeded = handler.handle(ProduceRequest.newBuilder()
                    .setTopic("t")
                    .setPartition(0)
                    .setBatch(ByteString.copyFrom(singleRecordBatch("v".getBytes())))
                    .setAcks(1)
                    .setProducerId(7L)
                    .setProducerEpoch(0)
                    .setBaseSequence(0)
                    .build());
            assertThat(seeded.getError().getCode()).isEqualTo(ErrorCodes.NONE);

            // acks=all retry of the same (pid, seq): follower 2 has not
            // replicated offset 0, so the cached hit must BLOCK until it
            // does — not ack instantly.
            var retry = java.util.concurrent.CompletableFuture.supplyAsync(
                    () -> handler.handle(idempotentAcksAll("v", 7L, 0)));
            Thread.sleep(300);
            assertThat(retry.isDone())
                    .as("cached acks=all retry must wait for HWM, not short-circuit")
                    .isFalse();

            tracker.record("t", 0, 2, 1L, 2_000L);
            var resp = retry.get(3, TimeUnit.SECONDS);
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(resp.getLastOffset()).isZero();
            assertThat(lmgr.logFor("t", 0).nextOffset()).isEqualTo(1L);
        }
    }

    @Test
    void acksAllRejectsOnLeadershipLoss(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 3, 0L);
        tm.onPartitionChange("t", 0, 1, List.of(1, 2, 3), List.of(1, 2, 3), 0, 0);

        try (var lmgr = lm(dir)) {
            var tracker = new FollowerStateTracker();
            tracker.record("t", 0, 2, 0L, 1_000L);

            var handler = new ProduceHandler(lmgr, tm, 1, tracker);

            var future =
                    java.util.concurrent.CompletableFuture.supplyAsync(() -> handler.handle(ProduceRequest.newBuilder()
                            .setTopic("t")
                            .setPartition(0)
                            .setBatch(ByteString.copyFrom(singleRecordBatch("v".getBytes())))
                            .setAcks(-1)
                            .setProducerId(-1L)
                            .setBaseSequence(-1)
                            .build()));
            Thread.sleep(50);
            // Simulate failover: a higher-epoch record demotes us.
            tm.onPartitionChange("t", 0, /*new leader*/ 2, List.of(2, 3), List.of(1, 2, 3), 1, 0);

            var resp = future.get(3, TimeUnit.SECONDS);
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NOT_LEADER);
        }
    }
}
