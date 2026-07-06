package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import jbroker.proto.broker.ProduceRequest;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Root-cause guard for the duplicate records found by
 * {@code scenario-chaos-with-load.sh} (payloads at 3 consecutive offsets):
 * {@link ProducerStateManager} is in-memory only, so a SIGKILLed broker
 * restarts with a full log but an empty dedup map — and {@code cached ==
 * null} meant "trust and append". A retry of an already-committed
 * (pid, epoch, sequence) was then double-appended, violating the spec
 * invariant #4. The handler must rebuild producer state from the log
 * before trusting an empty cache.
 */
class ProducerStateRecoveryTest {

    private static final int SELF = 1;
    private static final long PRODUCER_ID = 42L;

    @Test
    void retryAfterRestartReturnsCachedOffsetsInsteadOfDoubleAppending(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 1, 0L);
        tm.onPartitionChange("orders", 0, SELF, List.of(SELF), 0);

        try (var lm = lm(dir)) {
            // Pre-restart broker: two idempotent batches land (seqs 0..2, 3..5).
            var before = new ProduceHandler(lm, tm, SELF);
            var first = before.handle(produceRequest(PRODUCER_ID, 0, /*baseSeq*/ 0, 3));
            assertThat(first.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            var second = before.handle(produceRequest(PRODUCER_ID, 0, /*baseSeq*/ 3, 3));
            assertThat(second.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            long logEnd = lm.logFor("orders", 0).nextOffset();

            // "Restart": new handler, fresh (empty) ProducerStateManager,
            // same on-disk log — exactly the post-SIGKILL state.
            var after = new ProduceHandler(lm, tm, SELF);

            // Retry of the last committed batch must dedup, not append.
            var retry = after.handle(produceRequest(PRODUCER_ID, 0, /*baseSeq*/ 3, 3));
            assertThat(retry.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(retry.getBaseOffset()).isEqualTo(second.getBaseOffset());
            assertThat(retry.getLastOffset()).isEqualTo(second.getLastOffset());
            assertThat(lm.logFor("orders", 0).nextOffset())
                    .as("log must not grow on a retry of an already-committed batch")
                    .isEqualTo(logEnd);
        }
    }

    @Test
    void retryOfOlderBatchAfterRestartIsRejectedNotAppended(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 1, 0L);
        tm.onPartitionChange("orders", 0, SELF, List.of(SELF), 0);

        try (var lm = lm(dir)) {
            var before = new ProduceHandler(lm, tm, SELF);
            before.handle(produceRequest(PRODUCER_ID, 0, 0, 3));
            before.handle(produceRequest(PRODUCER_ID, 0, 3, 3));
            long logEnd = lm.logFor("orders", 0).nextOffset();

            var after = new ProduceHandler(lm, tm, SELF);
            var stale = after.handle(produceRequest(PRODUCER_ID, 0, /*baseSeq*/ 0, 3));
            assertThat(stale.getError().getCode()).isEqualTo(ErrorCodes.OUT_OF_ORDER_SEQUENCE);
            assertThat(lm.logFor("orders", 0).nextOffset()).isEqualTo(logEnd);
        }
    }

    @Test
    void continuationAfterRestartAppendsNormally(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 1, 0L);
        tm.onPartitionChange("orders", 0, SELF, List.of(SELF), 0);

        try (var lm = lm(dir)) {
            var before = new ProduceHandler(lm, tm, SELF);
            before.handle(produceRequest(PRODUCER_ID, 0, 0, 3));

            var after = new ProduceHandler(lm, tm, SELF);
            var next = after.handle(produceRequest(PRODUCER_ID, 0, /*baseSeq*/ 3, 2));
            assertThat(next.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(next.getBaseOffset()).isEqualTo(3L);
            assertThat(lm.logFor("orders", 0).nextOffset()).isEqualTo(5L);
        }
    }

    @Test
    void freshProducerOnRecoveredPartitionAppendsNormally(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 1, 0L);
        tm.onPartitionChange("orders", 0, SELF, List.of(SELF), 0);

        try (var lm = lm(dir)) {
            var before = new ProduceHandler(lm, tm, SELF);
            before.handle(produceRequest(PRODUCER_ID, 0, 0, 3));

            var after = new ProduceHandler(lm, tm, SELF);
            var other = after.handle(produceRequest(/*pid*/ 77L, 0, 0, 2));
            assertThat(other.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(other.getBaseOffset()).isEqualTo(3L);
        }
    }

    // --- helpers (mirrors ProduceHandlerDedupTest) ---

    private static LogManager lm(Path dir) throws Exception {
        return new LogManager(
                dir,
                new LogManager.Config(
                        128L * 1024 * 1024,
                        Long.MAX_VALUE,
                        LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        TimeUnit.MINUTES.toMillis(5)));
    }

    private static ProduceRequest produceRequest(long pid, int epoch, int baseSeq, int count) {
        var records = new java.util.ArrayList<Record>(count);
        for (int i = 0; i < count; i++) {
            records.add(new Record(i, 0L, null, ("v" + (baseSeq + i)).getBytes()));
        }
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        RecordBatch.encode(buf, 0L, 0, 1L, 1L, pid, (short) epoch, baseSeq, records);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return ProduceRequest.newBuilder()
                .setTopic("orders")
                .setPartition(0)
                .setProducerId(pid)
                .setProducerEpoch(epoch)
                .setBaseSequence(baseSeq)
                .setBatch(ByteString.copyFrom(bytes))
                .build();
    }
}
