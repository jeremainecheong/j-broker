package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import jbroker.proto.broker.FetchRequest;
import jbroker.storage.Compression;
import jbroker.storage.ControlRecord;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link FetchHandler} isolation levels: read_committed caps the read at
 * the last stable offset and attaches the aborted ranges overlapping the
 * returned window; read_uncommitted serves the transferred bytes as-is —
 * byte-for-byte what a pre-isolation fetch returned, control batches
 * included (clients skip them by attribute flag).
 */
final class FetchHandlerReadCommittedTest {

    private static final long PID = 7L;
    private static final short EPOCH = 3;

    private LogManager logManager;
    private TopicManager topicManager;
    private FetchHandler handler;

    @BeforeEach
    void setUp(@TempDir Path dir) throws IOException {
        logManager = new LogManager(
                dir,
                new LogManager.Config(
                        128L * 1024 * 1024,
                        Long.MAX_VALUE,
                        LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        java.util.concurrent.TimeUnit.MINUTES.toMillis(5)));
        topicManager = new TopicManager();
        topicManager.onTopicCommitted("orders", 1, 1, System.currentTimeMillis());
        topicManager.onPartitionChange("orders", 0, 1, List.of(1), List.of(1), 0, 0);
        handler = new FetchHandler(logManager, topicManager);
    }

    @Test
    void readCommittedCapsAtLastStableOffset() throws IOException {
        appendPlain("a", "b", "c"); // offsets 0..2
        appendTransactional("t1", "t2"); // offsets 3..4, transaction still open

        var resp = handler.handle(fetch(0, 1));

        assertThat(resp.hasError()).isFalse();
        assertThat(resp.getLastStableOffset()).isEqualTo(3);
        assertThat(batchOffsets(resp.getRecords().toByteArray())).containsExactly(0L);
        assertThat(lastOffsetOfLastBatch(resp.getRecords().toByteArray())).isEqualTo(2L);
        assertThat(resp.getAbortedTxnsCount()).isZero();
    }

    @Test
    void readCommittedServesEverythingOnceCommitted() throws IOException {
        appendPlain("a"); // 0
        appendTransactional("t1", "t2"); // 1..2
        appendMarker(ControlRecord.Type.COMMIT); // 3

        var resp = handler.handle(fetch(0, 1));

        assertThat(resp.getLastStableOffset()).isEqualTo(4);
        // The commit marker's control batch ships with the data — skipping
        // it is the client's job.
        assertThat(batchOffsets(resp.getRecords().toByteArray())).containsExactly(0L, 1L, 3L);
        assertThat(resp.getAbortedTxnsCount()).isZero();
    }

    @Test
    void readCommittedAttachesAbortedRangesOverlappingTheWindow() throws IOException {
        appendTransactional("t1", "t2"); // 0..1
        appendMarker(ControlRecord.Type.ABORT); // 2
        appendPlain("a"); // 3

        var resp = handler.handle(fetch(0, 1));

        assertThat(resp.getLastStableOffset()).isEqualTo(4);
        assertThat(resp.getAbortedTxnsCount()).isEqualTo(1);
        assertThat(resp.getAbortedTxns(0).getProducerId()).isEqualTo(PID);
        assertThat(resp.getAbortedTxns(0).getFirstOffset()).isZero();
        // Aborted data and its marker still ship — the client filters using
        // the attached range and the ABORT marker in the stream.
        assertThat(batchOffsets(resp.getRecords().toByteArray())).containsExactly(0L, 2L, 3L);
    }

    @Test
    void abortedRangeOutsideTheWindowIsNotAttached() throws IOException {
        appendTransactional("t1", "t2"); // 0..1
        appendMarker(ControlRecord.Type.ABORT); // 2
        appendPlain("a", "b"); // 3..4

        var resp = handler.handle(fetch(3, 1));

        assertThat(resp.getAbortedTxnsCount()).isZero();
        assertThat(resp.getLastStableOffset()).isEqualTo(5);
    }

    @Test
    void readCommittedNeverShipsBatchesAtOrPastTheLso() throws IOException {
        appendPlain("a"); // 0
        appendTransactional("t1"); // 1, open

        var resp = handler.handle(fetch(1, 1));

        assertThat(resp.hasError()).isFalse();
        assertThat(resp.getLastStableOffset()).isEqualTo(1);
        // The sparse index may rewind the transfer to earlier batches (the
        // client drops pre-window records), but nothing at or past the LSO
        // may ship — the open transaction's batch is cut.
        for (long base : batchOffsets(resp.getRecords().toByteArray())) {
            assertThat(base).isLessThan(1);
        }
    }

    @Test
    void readUncommittedServesOpenTransactionBytesUnchanged() throws IOException {
        appendPlain("a"); // 0
        appendTransactional("t1", "t2"); // 1..2, open
        appendMarker(ControlRecord.Type.ABORT); // 3

        var resp = handler.handle(fetch(0, 0));

        // Everything ships, isolation fields stay at proto defaults.
        assertThat(batchOffsets(resp.getRecords().toByteArray())).containsExactly(0L, 1L, 3L);
        assertThat(resp.getLastStableOffset()).isZero();
        assertThat(resp.getAbortedTxnsCount()).isZero();
        assertThat(resp.getHighWatermark()).isEqualTo(4);
    }

    @Test
    void readUncommittedBytesAreByteForByteTheRawLog() throws IOException {
        appendPlain("a");
        appendTransactional("t1");
        appendMarker(ControlRecord.Type.COMMIT);

        var raw = new java.io.ByteArrayOutputStream();
        logManager.logFor("orders", 0).transferTo(0, 1 << 20, raw);

        var resp = handler.handle(fetch(0, 0));
        assertThat(resp.getRecords().toByteArray()).isEqualTo(raw.toByteArray());
    }

    // ---------- helpers ----------

    private FetchRequest fetch(long offset, int isolationLevel) {
        return FetchRequest.newBuilder()
                .setTopic("orders")
                .setPartition(0)
                .setOffset(offset)
                .setMaxBytes(1 << 20)
                .setIsolationLevel(isolationLevel)
                .build();
    }

    private void appendPlain(String... values) throws IOException {
        var log = logManager.logFor("orders", 0);
        log.append(records(values), System.currentTimeMillis(), -1L, (short) -1, -1, 0, Compression.NONE);
    }

    private void appendTransactional(String... values) throws IOException {
        var log = logManager.logFor("orders", 0);
        log.append(
                records(values),
                System.currentTimeMillis(),
                PID,
                EPOCH,
                /*baseSequence*/ -1,
                /*partitionLeaderEpoch*/ 0,
                Compression.NONE,
                /*transactional*/ true);
    }

    private void appendMarker(ControlRecord.Type type) throws IOException {
        var log = logManager.logFor("orders", 0);
        log.appendControl(PID, EPOCH, new ControlRecord(type, /*coordinatorEpoch*/ 1), System.currentTimeMillis(), 0);
    }

    private static List<Record> records(String... values) {
        var out = new ArrayList<Record>(values.length);
        for (int i = 0; i < values.length; i++) {
            out.add(new Record(i, 0L, null, values[i].getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        }
        return out;
    }

    /** Base offsets of every complete batch frame in {@code bytes}. */
    private static List<Long> batchOffsets(byte[] bytes) {
        var out = new ArrayList<Long>();
        var buf = ByteBuffer.wrap(bytes);
        int pos = 0;
        while (bytes.length - pos >= RecordBatch.BATCH_OVERHEAD) {
            out.add(buf.getLong(pos));
            pos += 12 + buf.getInt(pos + 8);
        }
        return out;
    }

    private static long lastOffsetOfLastBatch(byte[] bytes) {
        var buf = ByteBuffer.wrap(bytes);
        int pos = 0;
        long last = -1;
        while (bytes.length - pos >= RecordBatch.BATCH_OVERHEAD) {
            last = buf.getLong(pos) + buf.getInt(pos + 23);
            pos += 12 + buf.getInt(pos + 8);
        }
        return last;
    }
}
