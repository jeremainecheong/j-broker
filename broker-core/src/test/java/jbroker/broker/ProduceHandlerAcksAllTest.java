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
