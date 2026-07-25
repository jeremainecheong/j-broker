package jbroker.storage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Transaction machinery at the {@link Log} level: LSO progression across
 * open → commit and open → abort (including interleaved producers), the
 * aborted-txn index, exact rebuild of both from disk on reopen, follower
 * {@code appendRaw} parity, truncation recompute, retention eviction, and
 * the control/compression and control/compaction exclusions.
 */
class LogTransactionTest {

    private static final Log.Config CONFIG = new Log.Config(1_000_000, 0, 4096);

    private static List<Record> records(String... values) {
        var out = new java.util.ArrayList<Record>(values.length);
        for (int i = 0; i < values.length; i++) {
            out.add(new Record(i, 0L, null, values[i].getBytes(UTF_8)));
        }
        return out;
    }

    private static long appendTxn(Log log, long producerId, String... values) throws IOException {
        return log.append(records(values), 1_000L, producerId, (short) 0, 0, 0, Compression.NONE, true);
    }

    @Test
    void lsoTracksOpenCommitLifecycle(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, CONFIG)) {
            log.append(records("plain"), 1_000L); // offset 0
            assertThat(log.lastStableOffset(log.nextOffset())).isEqualTo(1L);

            appendTxn(log, 7L, "a", "b"); // offsets 1..2
            long hwm = log.nextOffset();
            assertThat(log.lastStableOffset(hwm))
                    .as("held at the txn's first offset")
                    .isEqualTo(1L);
            assertThat(log.firstOngoingTxnOffset()).hasValue(1L);

            long marker = log.appendControl(7L, (short) 0, new ControlRecord(ControlRecord.Type.COMMIT, 1), 1_001L, 0);
            assertThat(marker).isEqualTo(3L);
            assertThat(log.lastStableOffset(log.nextOffset())).isEqualTo(4L);
            assertThat(log.abortedTxnsIn(0L, Long.MAX_VALUE)).isEmpty();
        }
    }

    @Test
    void lsoTracksOpenAbortLifecycleAndRecordsTheRange(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, CONFIG)) {
            appendTxn(log, 7L, "a"); // offset 0
            appendTxn(log, 7L, "b", "c"); // offsets 1..2
            log.appendControl(7L, (short) 0, new ControlRecord(ControlRecord.Type.ABORT, 1), 1_001L, 0); // offset 3

            assertThat(log.lastStableOffset(log.nextOffset())).isEqualTo(4L);
            assertThat(log.abortedTxnsIn(0L, Long.MAX_VALUE))
                    .containsExactly(new TransactionState.AbortedTxn(7L, 0L, 2L));
        }
    }

    @Test
    void interleavedProducersHoldLsoAtTheEarliestUndecidedTxn(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, CONFIG)) {
            appendTxn(log, 1L, "a1"); // offset 0
            appendTxn(log, 2L, "b1"); // offset 1
            appendTxn(log, 1L, "a2"); // offset 2
            assertThat(log.lastStableOffset(log.nextOffset())).isEqualTo(0L);

            // Later producer decides first: LSO must not move.
            log.appendControl(2L, (short) 0, new ControlRecord(ControlRecord.Type.COMMIT, 1), 1_001L, 0); // offset 3
            assertThat(log.lastStableOffset(log.nextOffset())).isEqualTo(0L);

            log.appendControl(1L, (short) 0, new ControlRecord(ControlRecord.Type.ABORT, 1), 1_002L, 0); // offset 4
            assertThat(log.lastStableOffset(log.nextOffset())).isEqualTo(5L);
            assertThat(log.abortedTxnsIn(0L, Long.MAX_VALUE))
                    .containsExactly(new TransactionState.AbortedTxn(1L, 0L, 2L));
        }
    }

    @Test
    void controlBatchesAreStoredUncompressedAndFlagged(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, CONFIG)) {
            appendTxn(log, 7L, "data");
            log.appendControl(7L, (short) 0, new ControlRecord(ControlRecord.Type.COMMIT, 5), 1_001L, 0);

            var batches = log.read(0L, 64 * 1024);
            assertThat(batches).hasSize(2);
            assertThat(batches.get(0).transactional()).isTrue();
            assertThat(batches.get(0).control()).isFalse();
            var control = batches.get(1);
            assertThat(control.control()).isTrue();
            assertThat(control.codec()).isEqualTo(Compression.NONE);
            assertThat(control.controlRecord()).isEqualTo(new ControlRecord(ControlRecord.Type.COMMIT, 5));
        }
    }

    @Test
    void reopenRebuildsOngoingTxnsAndAbortedIndexExactly(@TempDir Path dir) throws Exception {
        long hwm;
        try (var log = Log.open(dir, CONFIG)) {
            appendTxn(log, 1L, "a1"); // offset 0 — stays ongoing
            appendTxn(log, 2L, "b1", "b2"); // offsets 1..2 — aborted below
            appendTxn(log, 3L, "c1"); // offset 3 — committed below
            log.appendControl(2L, (short) 0, new ControlRecord(ControlRecord.Type.ABORT, 1), 1_001L, 0); // offset 4
            log.appendControl(3L, (short) 0, new ControlRecord(ControlRecord.Type.COMMIT, 1), 1_002L, 0); // offset 5
            hwm = log.nextOffset();
            assertThat(log.lastStableOffset(hwm)).isEqualTo(0L);
        }
        try (var reopened = Log.open(dir, CONFIG)) {
            assertThat(reopened.nextOffset()).isEqualTo(hwm);
            assertThat(reopened.firstOngoingTxnOffset()).hasValue(0L);
            assertThat(reopened.lastStableOffset(hwm)).isEqualTo(0L);
            assertThat(reopened.abortedTxnsIn(0L, Long.MAX_VALUE))
                    .containsExactly(new TransactionState.AbortedTxn(2L, 1L, 2L));

            // The rebuilt state stays live: deciding pid 1 releases the LSO.
            reopened.appendControl(1L, (short) 0, new ControlRecord(ControlRecord.Type.COMMIT, 2), 1_003L, 0);
            assertThat(reopened.lastStableOffset(reopened.nextOffset())).isEqualTo(reopened.nextOffset());
        }
    }

    @Test
    void reopenAcrossSegmentRollsRebuildsExactly(@TempDir Path dir) throws Exception {
        var smallSegments = new Log.Config(300, 0, 4096);
        try (var log = Log.open(dir, smallSegments)) {
            appendTxn(log, 1L, "a".repeat(120)); // offset 0
            appendTxn(log, 2L, "b".repeat(120)); // offset 1
            appendTxn(log, 1L, "c".repeat(120)); // offset 2
            log.appendControl(1L, (short) 0, new ControlRecord(ControlRecord.Type.ABORT, 1), 1_001L, 0); // offset 3
            assertThat(log.segments().size()).as("txn spans segments").isGreaterThan(1);
        }
        try (var reopened = Log.open(dir, smallSegments)) {
            assertThat(reopened.firstOngoingTxnOffset()).hasValue(1L);
            assertThat(reopened.abortedTxnsIn(0L, Long.MAX_VALUE))
                    .containsExactly(new TransactionState.AbortedTxn(1L, 0L, 2L));
        }
    }

    @Test
    void appendRawKeepsFollowerStateInStepWithTheLeader(@TempDir Path dir) throws Exception {
        // Leader writes; follower replays the leader's raw bytes.
        var leaderDir = dir.resolve("leader");
        var followerDir = dir.resolve("follower");
        try (var leader = Log.open(leaderDir, CONFIG);
                var follower = Log.open(followerDir, CONFIG)) {
            appendTxn(leader, 1L, "a1"); // offset 0
            appendTxn(leader, 2L, "b1"); // offset 1
            leader.appendControl(2L, (short) 0, new ControlRecord(ControlRecord.Type.ABORT, 1), 1_001L, 0); // offset 2

            var out = new java.io.ByteArrayOutputStream();
            leader.transferTo(0L, 1 << 20, out);
            byte[] raw = out.toByteArray();
            int pos = 0;
            for (var batch : leader.read(0L, 1 << 20)) {
                byte[] one = java.util.Arrays.copyOfRange(raw, pos, pos + batch.totalBytes());
                follower.appendRaw(one, batch.baseOffset());
                pos += batch.totalBytes();
            }

            long hwm = follower.nextOffset();
            assertThat(hwm).isEqualTo(leader.nextOffset());
            assertThat(follower.lastStableOffset(hwm)).isEqualTo(leader.lastStableOffset(hwm));
            assertThat(follower.firstOngoingTxnOffset()).hasValue(0L);
            assertThat(follower.abortedTxnsIn(0L, Long.MAX_VALUE))
                    .containsExactly(new TransactionState.AbortedTxn(2L, 1L, 1L));
        }
    }

    @Test
    void truncationResurrectsTransactionsWhoseMarkerWasDropped(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, CONFIG)) {
            appendTxn(log, 1L, "a1", "a2"); // offsets 0..1
            log.appendControl(1L, (short) 0, new ControlRecord(ControlRecord.Type.COMMIT, 1), 1_001L, 0); // offset 2
            assertThat(log.lastStableOffset(log.nextOffset())).isEqualTo(3L);

            // Follower reconciliation drops the marker: pid 1 is undecided again.
            log.truncateTo(2L);
            assertThat(log.nextOffset()).isEqualTo(2L);
            assertThat(log.firstOngoingTxnOffset()).hasValue(0L);
            assertThat(log.lastStableOffset(log.nextOffset())).isEqualTo(0L);
        }
    }

    @Test
    void truncationDropsAbortedRangesPastTheTarget(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, CONFIG)) {
            appendTxn(log, 1L, "a1"); // offset 0
            log.appendControl(1L, (short) 0, new ControlRecord(ControlRecord.Type.ABORT, 1), 1_001L, 0); // offset 1
            appendTxn(log, 2L, "b1"); // offset 2
            log.appendControl(2L, (short) 0, new ControlRecord(ControlRecord.Type.ABORT, 1), 1_002L, 0); // offset 3
            assertThat(log.abortedTxnsIn(0L, Long.MAX_VALUE)).hasSize(2);

            log.truncateTo(2L); // drops pid 2's data + marker entirely
            assertThat(log.abortedTxnsIn(0L, Long.MAX_VALUE))
                    .containsExactly(new TransactionState.AbortedTxn(1L, 0L, 0L));
            assertThat(log.firstOngoingTxnOffset()).isEmpty();
        }
    }

    @Test
    void retentionEvictsAbortedRangesBelowTheLogStart(@TempDir Path dir) throws Exception {
        var smallSegments = new Log.Config(200, 0, 4096);
        try (var log = Log.open(dir, smallSegments)) {
            appendTxn(log, 1L, "x".repeat(100)); // offset 0, old timestamp
            log.appendControl(1L, (short) 0, new ControlRecord(ControlRecord.Type.ABORT, 1), 1_000L, 0); // offset 1
            // Newer appends roll the old segment closed.
            for (int i = 0; i < 6; i++) {
                log.append(records("y".repeat(100)), 50_000L + i);
            }
            assertThat(log.abortedTxnsIn(0L, Long.MAX_VALUE)).hasSize(1);

            int removed = log.retain(/* cutoff */ 40_000L);
            assertThat(removed).isGreaterThan(0);
            long logStart = log.segments().get(0).baseOffset();
            assertThat(logStart).isGreaterThan(1L);
            assertThat(log.abortedTxnsIn(0L, Long.MAX_VALUE))
                    .as("aborted range below the new log start is unfetchable")
                    .isEmpty();
        }
    }

    @Test
    void compactionRefusesLogsHoldingControlBatches(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, CONFIG)) {
            log.append(List.of(new Record(0, 0L, "k".getBytes(UTF_8), "v".getBytes(UTF_8))), 1_000L);
            appendTxn(log, 1L, "a1");
            log.appendControl(1L, (short) 0, new ControlRecord(ControlRecord.Type.COMMIT, 1), 1_001L, 0);
            assertThatThrownBy(log::compactByKey)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("compaction is unsupported for transactional logs");
            // The refusal must not have destroyed anything.
            assertThat(log.read(0L, 1 << 20)).hasSize(3);
        }
    }

    @Test
    void controlAppendRequiresProducerIdentity(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, CONFIG)) {
            assertThatThrownBy(() -> log.appendControl(
                            -1L, (short) 0, new ControlRecord(ControlRecord.Type.COMMIT, 1), 1_000L, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("producerId");
        }
    }

    @Test
    void controlAppendGateRunsBeforeTheMarkerLands(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, CONFIG)) {
            appendTxn(log, 1L, "a1");
            var gateRan = new java.util.concurrent.atomic.AtomicInteger();
            log.setControlAppendGate(gateRan::incrementAndGet);
            log.appendControl(1L, (short) 0, new ControlRecord(ControlRecord.Type.COMMIT, 1), 1_000L, 0);
            assertThat(gateRan.get()).isEqualTo(1);

            // A refusing gate blocks the write entirely.
            long before = log.nextOffset();
            log.setControlAppendGate(() -> {
                throw new IOException("format marker refused");
            });
            assertThatThrownBy(() ->
                            log.appendControl(1L, (short) 0, new ControlRecord(ControlRecord.Type.ABORT, 1), 1_001L, 0))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("format marker refused");
            assertThat(log.nextOffset()).as("nothing was appended").isEqualTo(before);
        }
    }

    @Test
    void crashTornMarkerLeavesTheTransactionOngoing(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, CONFIG)) {
            appendTxn(log, 1L, "a1"); // offset 0
            log.appendControl(1L, (short) 0, new ControlRecord(ControlRecord.Type.COMMIT, 1), 1_001L, 0); // offset 1
            log.force();
        }
        // Simulate a torn write: chop bytes off the marker batch's tail.
        var segmentFile = dir.resolve(LogSegment.filenameBase(0L) + ".log");
        try (var ch = java.nio.channels.FileChannel.open(segmentFile, java.nio.file.StandardOpenOption.WRITE)) {
            ch.truncate(ch.size() - 3);
        }
        try (var reopened = Log.open(dir, CONFIG)) {
            assertThat(reopened.nextOffset()).as("torn marker truncated away").isEqualTo(1L);
            assertThat(reopened.firstOngoingTxnOffset()).hasValue(0L);
            assertThat(reopened.lastStableOffset(reopened.nextOffset())).isEqualTo(0L);
        }
    }

    @Test
    void readPathReturnsControlBatchesDistinguishably(@TempDir Path dir) throws Exception {
        // The fetch wiring slice will exclude control batches from
        // application decode; storage's contract is that the flag is
        // there to act on after a plain read.
        try (var log = Log.open(dir, CONFIG)) {
            appendTxn(log, 1L, "a1");
            log.appendControl(1L, (short) 0, new ControlRecord(ControlRecord.Type.ABORT, 3), 1_001L, 0);
            var batches = log.read(0L, 1 << 20);
            assertThat(batches).extracting(RecordBatch.Parsed::control).containsExactly(false, true);
            assertThat(batches.get(1).controlRecord().coordinatorEpoch()).isEqualTo(3);
        }
    }

    @Test
    void rawControlBytesReplicateByteIdentically(@TempDir Path dir) throws Exception {
        try (var leader = Log.open(dir.resolve("leader"), CONFIG);
                var follower = Log.open(dir.resolve("follower"), CONFIG)) {
            leader.appendControl(9L, (short) 2, new ControlRecord(ControlRecord.Type.COMMIT, 8), 1_000L, 4);
            var out = new java.io.ByteArrayOutputStream();
            leader.transferTo(0L, 1 << 20, out);
            byte[] raw = out.toByteArray();
            follower.appendRaw(raw, 0L);

            var replayed = new java.io.ByteArrayOutputStream();
            follower.transferTo(0L, 1 << 20, replayed);
            assertThat(replayed.toByteArray()).containsExactly(raw);

            var parsed = RecordBatch.decode(ByteBuffer.wrap(replayed.toByteArray()));
            assertThat(parsed.control()).isTrue();
            assertThat(parsed.partitionLeaderEpoch()).isEqualTo(4);
            assertThat(parsed.producerEpoch()).isEqualTo((short) 2);
            assertThat(parsed.controlRecord()).isEqualTo(new ControlRecord(ControlRecord.Type.COMMIT, 8));
        }
    }
}
