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

class ProduceHandlerDedupTest {

    private static final int SELF = 1;
    private static final long PRODUCER_ID = 42L;

    @Test
    void duplicateProduceReturnsCachedOffsetsWithoutAppending(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 1, 0L);
        tm.onPartitionChange("orders", 0, SELF, List.of(SELF), 0);

        try (var lm = lm(dir)) {
            var handler = new ProduceHandler(lm, tm, SELF);
            var req = producerequest(PRODUCER_ID, /* epoch */ 0, /* baseSeq */ 5, 3);

            var first = handler.handle(req);
            assertThat(first.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(first.getBaseOffset()).isEqualTo(0L);
            assertThat(first.getLastOffset()).isEqualTo(2L);
            long logEndAfterFirst = lm.logFor("orders", 0).nextOffset();

            var dup = handler.handle(req);

            assertThat(dup.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(dup.getBaseOffset()).isEqualTo(0L);
            assertThat(dup.getLastOffset()).isEqualTo(2L);
            // Log must not have grown.
            assertThat(lm.logFor("orders", 0).nextOffset()).isEqualTo(logEndAfterFirst);
        }
    }

    @Test
    void outOfOrderSequenceIsRejected(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 1, 0L);
        tm.onPartitionChange("orders", 0, SELF, List.of(SELF), 0);

        try (var lm = lm(dir)) {
            var handler = new ProduceHandler(lm, tm, SELF);
            handler.handle(producerequest(PRODUCER_ID, 0, 0, 3)); // seqs 0..2

            var oor = handler.handle(producerequest(PRODUCER_ID, 0, 5, 1)); // skipped 3..4
            assertThat(oor.getError().getCode()).isEqualTo(ErrorCodes.OUT_OF_ORDER_SEQUENCE);
        }
    }

    @Test
    void duplicateRetryWithDifferentRecordCountIsRejected(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 1, 0L);
        tm.onPartitionChange("orders", 0, SELF, List.of(SELF), 0);

        try (var lm = lm(dir)) {
            var handler = new ProduceHandler(lm, tm, SELF);
            // First: 3 records at seq 5.
            handler.handle(producerequest(PRODUCER_ID, 0, 5, 3));

            // Retry at same baseSeq=5 but re-batched to 5 records — must NOT
            // silently return cached offsets covering a different count.
            var dup = handler.handle(producerequest(PRODUCER_ID, 0, 5, 5));
            assertThat(dup.getError().getCode()).isEqualTo(ErrorCodes.OUT_OF_ORDER_SEQUENCE);
        }
    }

    @Test
    void epochBumpResetsDedupState(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 1, 0L);
        tm.onPartitionChange("orders", 0, SELF, List.of(SELF), 0);

        try (var lm = lm(dir)) {
            var handler = new ProduceHandler(lm, tm, SELF);
            handler.handle(producerequest(PRODUCER_ID, /* epoch */ 0, 0, 3)); // seqs 0..2

            // Same producer-id, new epoch (client rebirth) — the new epoch is
            // its own dedup key, so seq=0 is NOT a duplicate.
            var rebirth = handler.handle(producerequest(PRODUCER_ID, /* epoch */ 1, 0, 3));
            assertThat(rebirth.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(lm.logFor("orders", 0).nextOffset()).isEqualTo(6L);
        }
    }

    @Test
    void producerIdZeroTreatedAsLegacy(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 1, 0L);
        tm.onPartitionChange("orders", 0, SELF, List.of(SELF), 0);

        try (var lm = lm(dir)) {
            var handler = new ProduceHandler(lm, tm, SELF);
            // proto3 default: int64 producer_id = 0. Treat as legacy (not
            // idempotent) so a non-Java client that forgets InitProducerId
            // can't collide with a real allocation.
            var r1 = handler.handle(producerequest(/* id */ 0L, 0, 0, 3));
            var r2 = handler.handle(producerequest(0L, 0, 0, 3));
            assertThat(r1.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(r2.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(lm.logFor("orders", 0).nextOffset()).isEqualTo(6L);
        }
    }

    @Test
    void legacyProduceWithoutProducerIdSkipsDedup(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 1, 0L);
        tm.onPartitionChange("orders", 0, SELF, List.of(SELF), 0);

        try (var lm = lm(dir)) {
            var handler = new ProduceHandler(lm, tm, SELF);
            // Two identical legacy requests (producer_id = -1): both append.
            var r1 = handler.handle(legacyRequest(3));
            var r2 = handler.handle(legacyRequest(3));

            assertThat(r1.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(r2.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(lm.logFor("orders", 0).nextOffset()).isEqualTo(6L);
        }
    }

    private static ProduceRequest producerequest(long producerId, int epoch, int baseSeq, int count) {
        return ProduceRequest.newBuilder()
                .setTopic("orders")
                .setPartition(0)
                .setBatch(encodedBatch(count, producerId, epoch, baseSeq))
                .setProducerId(producerId)
                .setProducerEpoch(epoch)
                .setBaseSequence(baseSeq)
                .build();
    }

    private static ProduceRequest legacyRequest(int count) {
        return ProduceRequest.newBuilder()
                .setTopic("orders")
                .setPartition(0)
                .setBatch(encodedBatch(count, -1L, 0, -1))
                .setProducerId(-1L)
                .setProducerEpoch(0)
                .setBaseSequence(-1)
                .build();
    }

    private static ByteString encodedBatch(int count, long producerId, int epoch, int baseSeq) {
        var records = new java.util.ArrayList<Record>();
        for (int i = 0; i < count; i++) {
            records.add(new Record(i, 0L, null, new byte[] {(byte) i}));
        }
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        RecordBatch.encode(buf, 0L, 0, 1L, 1L, producerId, (short) epoch, baseSeq, records);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return ByteString.copyFrom(bytes);
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
