package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import jbroker.proto.broker.ProduceRequest;
import jbroker.storage.Compression;
import jbroker.storage.ControlRecord;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Transactional produce gate: the partition leader validates producer
 * identity against the transaction state already in its log, keeps the
 * transactional attribute bit flowing through to disk (so the LSO holds
 * until a marker decides the transaction), and refuses client-forged
 * control batches. The non-transactional path is untouched — covered by
 * the sibling ProduceHandler tests.
 */
class ProduceHandlerTxnTest {

    private static final int SELF = 1;
    private static final long PID = 42L;

    @Test
    void transactionalBatchWithoutProducerIdIsInvalid(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var handler = handler(lm);
            // producerId 7 in the batch header but 0 on the request — the
            // request fields are what the leader trusts.
            var resp = handler.handle(txnRequest(/*reqPid*/ 0L, /*batchPid*/ 7L, /*epoch*/ 0, /*seq*/ 0));
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.INVALID_TXN_STATE);
        }
    }

    @Test
    void controlBatchesCannotBeProduced(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var handler = handler(lm);
            var buf = ByteBuffer.allocate(256);
            RecordBatch.encodeControl(buf, 0L, 0, 1L, PID, (short) 0, new ControlRecord(ControlRecord.Type.COMMIT, 0));
            buf.flip();
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            var resp = handler.handle(ProduceRequest.newBuilder()
                    .setTopic("orders")
                    .setPartition(0)
                    .setProducerId(PID)
                    .setBatch(ByteString.copyFrom(bytes))
                    .build());
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.CORRUPT_BATCH);
            assertThat(lm.logFor("orders", 0).nextOffset()).isZero();
        }
    }

    @Test
    void transactionalAppendOpensOngoingTransaction(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var handler = handler(lm);
            var resp = handler.handle(txnRequest(PID, PID, /*epoch*/ 0, /*seq*/ 0));
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            var log = lm.logFor("orders", 0);
            assertThat(log.firstOngoingTxnOffset())
                    .as("transactional bit must reach the log so the LSO holds")
                    .hasValue(0L);
            assertThat(log.lastStableOffset(log.nextOffset())).isZero();
        }
    }

    @Test
    void markerEpochBumpFencesStaleProducerAfterRestart(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var log = lm.logFor("orders", 0);
            // Epoch-0 transactional data, then the abort marker of an
            // InitTransactions epoch bump (epoch 1) — the on-disk shape a
            // fenced zombie's partition holds after coordinator recovery.
            log.append(
                    List.of(new Record(0, 0L, null, "v0".getBytes())),
                    1L,
                    PID,
                    (short) 0,
                    0,
                    0,
                    Compression.NONE,
                    /*transactional*/ true);
            log.appendControl(PID, (short) 1, new ControlRecord(ControlRecord.Type.ABORT, 0), 2L, 0);

            // Fresh handler (post-restart view): the recovery walk must
            // restore the epoch floor from the marker.
            var handler = handler(lm);
            var stale = handler.handle(txnRequest(PID, PID, /*epoch*/ 0, /*seq*/ 1));
            assertThat(stale.getError().getCode()).isEqualTo(ErrorCodes.PRODUCER_FENCED);

            var current = handler.handle(txnRequest(PID, PID, /*epoch*/ 1, /*seq*/ 0));
            assertThat(current.getError().getCode()).isEqualTo(ErrorCodes.NONE);
        }
    }

    @Test
    void sameEpochExtendsAndHigherEpochOpensNextTransaction(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var handler = handler(lm);
            assertThat(handler.handle(txnRequest(PID, PID, 0, 0)).getError().getCode())
                    .isEqualTo(ErrorCodes.NONE);
            assertThat(handler.handle(txnRequest(PID, PID, 0, 1)).getError().getCode())
                    .isEqualTo(ErrorCodes.NONE);
            // Marker decides the epoch-0 transaction; a bumped-epoch produce
            // then opens the next one.
            lm.logFor("orders", 0)
                    .appendControl(PID, (short) 0, new ControlRecord(ControlRecord.Type.COMMIT, 0), 3L, 0);
            assertThat(handler.handle(txnRequest(PID, PID, 1, 0)).getError().getCode())
                    .isEqualTo(ErrorCodes.NONE);
            assertThat(handler.handle(txnRequest(PID, PID, 0, 2)).getError().getCode())
                    .as("epoch 0 is fenced once epoch 1 was observed")
                    .isEqualTo(ErrorCodes.PRODUCER_FENCED);
        }
    }

    // --- helpers ---

    private static ProduceHandler handler(LogManager lm) {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 1, 0L);
        tm.onPartitionChange("orders", 0, SELF, List.of(SELF), 0);
        return new ProduceHandler(lm, tm, SELF);
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

    private static ProduceRequest txnRequest(long reqPid, long batchPid, int epoch, int baseSeq) {
        var records = List.of(new Record(0, 0L, null, ("v" + baseSeq).getBytes()));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        RecordBatch.encode(
                buf,
                0L,
                0,
                1L,
                1L,
                batchPid,
                (short) epoch,
                baseSeq,
                records,
                Compression.NONE, /*transactional*/
                true);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return ProduceRequest.newBuilder()
                .setTopic("orders")
                .setPartition(0)
                .setProducerId(reqPid)
                .setProducerEpoch(epoch)
                .setBaseSequence(baseSeq)
                .setBatch(ByteString.copyFrom(bytes))
                .build();
    }
}
