package jbroker.broker.txn;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import jbroker.broker.ErrorCodes;
import jbroker.proto.common.TopicPartition;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Proves the recovery walk reads a {@code __transaction_state} partition
 * back into a fresh {@link TxnCoordinator} with latest-write-wins
 * semantics, and that a coordinator rebuilt mid-two-phase re-derives its
 * marker-delivery obligations from the log alone.
 */
class TxnStateRecoveryTest {

    private static final TopicPartition ORDERS_0 = tp("orders", 0);
    private static final TopicPartition PAYMENTS_2 = tp("payments", 2);

    private static TopicPartition tp(String topic, int partition) {
        return TopicPartition.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .build();
    }

    @Test
    void rebuildRecoversLatestStatePerKey(@TempDir Path dir) throws IOException {
        var lm = newLogManager(dir);
        // Drive a live coordinator through a full commit and append every
        // record it hands back, exactly as the wiring will.
        var live = new TxnCoordinator(1);
        appendState(
                lm, live.initTransactions("t1", 30_000, 100L, 0L).stateRecord().orElseThrow());
        appendState(
                lm,
                live.addPartitions("t1", 100L, 0, List.of(ORDERS_0, PAYMENTS_2), 1L)
                        .stateRecord()
                        .orElseThrow());
        appendState(lm, live.endTxn("t1", 100L, 0, true, 2L).stateRecord().orElseThrow());
        appendState(lm, live.markersDelivered("t1", 100L, 0, 3L).stateRecord().orElseThrow());

        var rebuilt = new TxnCoordinator(2);
        int applied = TxnStateRecovery.rebuild(lm, 0, rebuilt);

        assertThat(applied).isEqualTo(4); // applied count includes overwrites
        assertThat(rebuilt.stateOf("t1")).isEqualTo(live.stateOf("t1"));
        assertThat(rebuilt.stateOf("t1").orElseThrow().state()).isEqualTo(TxnState.COMPLETE_COMMIT);
        assertThat(rebuilt.pendingMarkers()).isEmpty();
    }

    @Test
    void rebuildMidTwoPhaseResumesMarkerDelivery(@TempDir Path dir) throws IOException {
        var lm = newLogManager(dir);
        var live = new TxnCoordinator(1);
        appendState(
                lm, live.initTransactions("t1", 30_000, 100L, 0L).stateRecord().orElseThrow());
        appendState(
                lm,
                live.addPartitions("t1", 100L, 0, List.of(ORDERS_0, PAYMENTS_2), 1L)
                        .stateRecord()
                        .orElseThrow());
        appendState(lm, live.endTxn("t1", 100L, 0, true, 2L).stateRecord().orElseThrow());
        // Coordinator dies before markersDelivered — the new leadership
        // replays and must resume delivery with its own coordinator epoch.

        var rebuilt = new TxnCoordinator(2);
        TxnStateRecovery.rebuild(lm, 0, rebuilt);

        assertThat(rebuilt.stateOf("t1").orElseThrow().state()).isEqualTo(TxnState.PREPARE_COMMIT);
        var pending = rebuilt.pendingMarkers();
        assertThat(pending).hasSize(2);
        for (var m : pending) {
            assertThat(m.producerId()).isEqualTo(100L);
            assertThat(m.producerEpoch()).isZero();
            assertThat(m.commit()).isTrue();
            assertThat(m.coordinatorEpoch()).isEqualTo(2); // new tenure, new epoch
        }
        // Completion then proceeds normally on the rebuilt coordinator.
        assertThat(rebuilt.markersDelivered("t1", 100L, 0, 10L)
                        .stateRecord()
                        .orElseThrow()
                        .state())
                .isEqualTo(TxnState.COMPLETE_COMMIT);
    }

    @Test
    void multipleTransactionalIdsCoexist(@TempDir Path dir) throws IOException {
        var lm = newLogManager(dir);
        var live = new TxnCoordinator(1);
        appendState(
                lm, live.initTransactions("t1", 30_000, 100L, 0L).stateRecord().orElseThrow());
        appendState(
                lm, live.initTransactions("t2", 30_000, 200L, 0L).stateRecord().orElseThrow());
        appendState(
                lm,
                live.addPartitions("t2", 200L, 0, List.of(ORDERS_0), 1L)
                        .stateRecord()
                        .orElseThrow());

        var rebuilt = new TxnCoordinator(2);
        int applied = TxnStateRecovery.rebuild(lm, 0, rebuilt);

        assertThat(applied).isEqualTo(3);
        assertThat(rebuilt.stateOf("t1")).isEqualTo(live.stateOf("t1"));
        assertThat(rebuilt.stateOf("t2")).isEqualTo(live.stateOf("t2"));
        // Recovered epochs still fence: the pre-crash grant keeps working...
        assertThat(rebuilt.endTxn("t2", 200L, 0, true, 5L).errorCode()).isEqualTo(ErrorCodes.NONE);
        // ...and a stale one stays fenced.
        assertThat(rebuilt.addPartitions("t1", 100L, 5, List.of(ORDERS_0), 5L).errorCode())
                .isEqualTo(ErrorCodes.PRODUCER_FENCED);
    }

    @Test
    void emptyLogReturnsZero(@TempDir Path dir) throws IOException {
        var lm = newLogManager(dir);
        // touch the partition log to materialise it without writing anything
        lm.logFor(TxnStateTopic.NAME, 0);

        assertThat(TxnStateRecovery.rebuild(lm, 0, new TxnCoordinator(1))).isZero();
    }

    @Test
    void foreignRecordTypesAreSkipped(@TempDir Path dir) throws IOException {
        var lm = newLogManager(dir);
        var live = new TxnCoordinator(1);
        // One record with an unknown type tag, then a real one.
        appendBatch(lm, List.of(new Record(0, 0L, new byte[] {0x7F, 0x00, 0x01}, new byte[] {0x42})));
        appendState(
                lm, live.initTransactions("t1", 30_000, 100L, 0L).stateRecord().orElseThrow());

        var rebuilt = new TxnCoordinator(2);
        int applied = TxnStateRecovery.rebuild(lm, 0, rebuilt);

        assertThat(applied).isEqualTo(1);
        assertThat(rebuilt.stateOf("t1")).isEqualTo(live.stateOf("t1"));
    }

    private static void appendState(LogManager lm, TxnStateRecord rec) throws IOException {
        byte[] key = TxnStateTopic.keyForTxn(rec.transactionalId());
        byte[] value = TxnStateTopic.valueForTxnState(rec);
        appendBatch(lm, List.of(new Record(0, 0L, key, value)));
    }

    private static void appendBatch(LogManager lm, List<Record> records) throws IOException {
        var log = lm.logFor(TxnStateTopic.NAME, 0);
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        long base = log.nextOffset();
        long now = System.currentTimeMillis();
        RecordBatch.encode(buf, base, 0, now, now, -1L, (short) -1, -1, records);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        log.appendRaw(bytes, base);
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
