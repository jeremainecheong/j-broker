package jbroker.broker.group;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import jbroker.broker.ConsumerOffsetsTopic;
import jbroker.storage.Compression;
import jbroker.storage.ControlRecord;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Transactional replay through {@link OffsetCacheRecovery}: staged
 * (transactional) offset records never reach the committed view on their
 * own, a COMMIT control batch folds them, an ABORT discards them, and a
 * walk that ends mid-transaction reconstructs the staged-but-undecided
 * state into the supplied {@link TxnOffsetStaging}.
 */
final class OffsetCacheRecoveryTxnTest {

    private static final long PID = 9L;
    private static final short EPOCH = 5;
    private static final int PARTITION = 0;

    @Test
    void undecidedStagedOffsetsAreReconstructedNotCommitted(@TempDir Path dir) throws IOException {
        var lm = newLogManager(dir);
        appendPlainOffset(lm, "g1", "orders", 0, 10L);
        appendStagedOffset(lm, "g1", "orders", 0, 42L); // txn open, no marker

        var cache = new OffsetCache();
        var staging = new TxnOffsetStaging();
        int applied = OffsetCacheRecovery.rebuild(lm, ConsumerOffsetsTopic.NAME, PARTITION, cache, staging);

        assertThat(applied).isEqualTo(1); // only the plain commit
        assertThat(cache.get("g1", "orders", 0).orElseThrow().offset()).isEqualTo(10L);
        assertThat(staging.stagedCount()).isEqualTo(1);
        assertThat(staging.stagedFor("g1", PID)).hasSize(1);

        // The marker arriving after activation decides the reconstructed stage.
        var folded = staging.onMarker(PARTITION, PID, EPOCH, /*commit*/ true, cache);
        assertThat(folded).hasSize(1);
        assertThat(cache.get("g1", "orders", 0).orElseThrow().offset()).isEqualTo(42L);
    }

    @Test
    void commitMarkerInTheLogFoldsStagedOffsets(@TempDir Path dir) throws IOException {
        var lm = newLogManager(dir);
        appendStagedOffset(lm, "g1", "orders", 0, 42L);
        appendMarker(lm, ControlRecord.Type.COMMIT);

        var cache = new OffsetCache();
        var staging = new TxnOffsetStaging();
        int applied = OffsetCacheRecovery.rebuild(lm, ConsumerOffsetsTopic.NAME, PARTITION, cache, staging);

        assertThat(applied).isEqualTo(1); // the folded record
        assertThat(cache.get("g1", "orders", 0).orElseThrow().offset()).isEqualTo(42L);
        assertThat(staging.stagedCount()).isZero();
    }

    @Test
    void abortMarkerInTheLogDiscardsStagedOffsets(@TempDir Path dir) throws IOException {
        var lm = newLogManager(dir);
        appendPlainOffset(lm, "g1", "orders", 0, 10L);
        appendStagedOffset(lm, "g1", "orders", 0, 42L);
        appendMarker(lm, ControlRecord.Type.ABORT);

        var cache = new OffsetCache();
        var staging = new TxnOffsetStaging();
        OffsetCacheRecovery.rebuild(lm, ConsumerOffsetsTopic.NAME, PARTITION, cache, staging);

        assertThat(cache.get("g1", "orders", 0).orElseThrow().offset()).isEqualTo(10L);
        assertThat(staging.stagedCount()).isZero();
    }

    @Test
    void foldedPlainRecordsAfterTheMarkerReplayIdempotently(@TempDir Path dir) throws IOException {
        // The live fold path appends the folded offsets as regular records
        // after the marker; a replay that sees stage + marker + folded copy
        // must land on the same committed view.
        var lm = newLogManager(dir);
        appendStagedOffset(lm, "g1", "orders", 0, 42L);
        appendMarker(lm, ControlRecord.Type.COMMIT);
        appendPlainOffset(lm, "g1", "orders", 0, 42L); // durable folded copy

        var cache = new OffsetCache();
        var staging = new TxnOffsetStaging();
        OffsetCacheRecovery.rebuild(lm, ConsumerOffsetsTopic.NAME, PARTITION, cache, staging);

        assertThat(cache.get("g1", "orders", 0).orElseThrow().offset()).isEqualTo(42L);
        assertThat(cache.size()).isEqualTo(1);
        assertThat(staging.stagedCount()).isZero();
    }

    @Test
    void stagingLessOverloadKeepsUndecidedOffsetsOutOfTheCommittedView(@TempDir Path dir) throws IOException {
        var lm = newLogManager(dir);
        appendStagedOffset(lm, "g1", "orders", 0, 42L); // undecided

        var cache = new OffsetCache();
        int applied = OffsetCacheRecovery.rebuild(lm, PARTITION, cache);

        assertThat(applied).isZero();
        assertThat(cache.get("g1", "orders", 0)).isEmpty();
    }

    @Test
    void replayFencesStagesBehindADecidedEpoch(@TempDir Path dir) throws IOException {
        var lm = newLogManager(dir);
        appendStagedOffset(lm, "g1", "orders", 0, 42L); // epoch 5
        appendMarker(lm, ControlRecord.Type.COMMIT); // decides epoch 5

        var cache = new OffsetCache();
        var staging = new TxnOffsetStaging();
        OffsetCacheRecovery.rebuild(lm, ConsumerOffsetsTopic.NAME, PARTITION, cache, staging);

        // A zombie stage below the decided epoch is refused post-replay.
        assertThat(staging.stage("g1", PARTITION, PID, EPOCH - 1, stagedMap("orders", 0, 99L)))
                .isEqualTo(TxnOffsetStaging.StageOutcome.PRODUCER_FENCED);
    }

    // ---------- helpers ----------

    private static java.util.Map<jbroker.proto.common.TopicPartition, OffsetCache.OffsetAndMetadata> stagedMap(
            String topic, int partition, long offset) {
        return java.util.Map.of(
                jbroker.proto.common.TopicPartition.newBuilder()
                        .setTopic(topic)
                        .setPartition(partition)
                        .build(),
                new OffsetCache.OffsetAndMetadata(offset, 0, "", 100L));
    }

    private static void appendPlainOffset(LogManager lm, String group, String topic, int tpPartition, long offset)
            throws IOException {
        appendOffsetBatch(lm, group, topic, tpPartition, offset, /*transactional*/ false);
    }

    private static void appendStagedOffset(LogManager lm, String group, String topic, int tpPartition, long offset)
            throws IOException {
        appendOffsetBatch(lm, group, topic, tpPartition, offset, /*transactional*/ true);
    }

    private static void appendOffsetBatch(
            LogManager lm, String group, String topic, int tpPartition, long offset, boolean transactional)
            throws IOException {
        byte[] key = ConsumerOffsetsTopic.keyForOffset(group, topic, tpPartition);
        byte[] value = ConsumerOffsetsTopic.valueForOffset(offset, 0, "", 100L);
        var records = List.of(new Record(0, 0L, key, value));
        var log = lm.logFor(ConsumerOffsetsTopic.NAME, PARTITION);
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        long base = log.nextOffset();
        long now = System.currentTimeMillis();
        RecordBatch.encode(
                buf,
                base,
                0,
                now,
                now,
                transactional ? PID : -1L,
                transactional ? EPOCH : (short) -1,
                -1,
                records,
                Compression.NONE,
                transactional);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        log.appendRaw(bytes, base);
    }

    private static void appendMarker(LogManager lm, ControlRecord.Type type) throws IOException {
        var log = lm.logFor(ConsumerOffsetsTopic.NAME, PARTITION);
        log.appendControl(PID, EPOCH, new ControlRecord(type, /*coordinatorEpoch*/ 1), System.currentTimeMillis(), 0);
    }

    private static LogManager newLogManager(Path dir) throws IOException {
        return new LogManager(
                dir,
                new LogManager.Config(
                        128L * 1024 * 1024,
                        Long.MAX_VALUE,
                        LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        java.util.concurrent.TimeUnit.MINUTES.toMillis(5)));
    }
}
