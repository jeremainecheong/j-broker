package jbroker.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LogAppendRawTest {

    @Test
    void appendRawPreservesLeaderBatchMetadata(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var log = lm.logFor("t", 0);
            // Build a leader-style batch with distinctive producer_id /
            // partition_leader_epoch / first_timestamp so we can prove the
            // follower-side write didn't clobber them.
            var records = List.of(new Record(0, 0L, null, new byte[] {1}), new Record(1, 5L, null, new byte[] {2}));
            var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
            long leaderTs = 1_700_000_000_000L;
            long producerId = 99L;
            short producerEpoch = 7;
            int baseSeq = 42;
            int partitionLeaderEpoch = 13;
            RecordBatch.encode(
                    buf, 0L, partitionLeaderEpoch, leaderTs, leaderTs + 5, producerId, producerEpoch, baseSeq, records);
            buf.flip();
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);

            log.appendRaw(bytes, /* expectedBaseOffset */ 0L);

            assertThat(log.nextOffset()).isEqualTo(2L);

            // Read it back and verify bytes are preserved.
            var batches = log.read(0L, 1024);
            assertThat(batches).hasSize(1);
            var parsed = batches.get(0);
            assertThat(parsed.baseOffset()).isZero();
            assertThat(parsed.firstTimestamp()).isEqualTo(leaderTs);
            assertThat(parsed.maxTimestamp()).isEqualTo(leaderTs + 5);
            assertThat(parsed.producerId()).isEqualTo(producerId);
            assertThat(parsed.producerEpoch()).isEqualTo(producerEpoch);
            assertThat(parsed.baseSequence()).isEqualTo(baseSeq);
            assertThat(parsed.partitionLeaderEpoch()).isEqualTo(partitionLeaderEpoch);
            assertThat(parsed.records()).hasSize(2);
        }
    }

    @Test
    void appendRawRejectsBatchWhoseHeaderDisagreesWithClaimedOffset(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var log = lm.logFor("t", 0);
            // Leader batch with baseOffset = 0.
            var records = List.of(new Record(0, 0L, null, new byte[] {1}));
            var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
            RecordBatch.encode(buf, 0L, 0, 1L, 1L, -1L, (short) -1, -1, records);
            buf.flip();
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);

            // The claimed append offset must match what the batch header
            // says — offsets are the leader's to assign, byte-identically.
            assertThatThrownBy(() -> log.appendRaw(bytes, 5L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContainingAll("batch baseOffset", "expectedBaseOffset");
        }
    }

    @Test
    void appendRawRejectsRewindBelowNextOffset(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var log = lm.logFor("t", 0);
            var records = List.of(new Record(0, 0L, null, new byte[] {1}));
            var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
            RecordBatch.encode(buf, 0L, 0, 1L, 1L, -1L, (short) -1, -1, records);
            buf.flip();
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            log.appendRaw(bytes, 0L);

            // Re-appending the same batch would rewrite offset 0.
            assertThatThrownBy(() -> log.appendRaw(bytes, 0L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContainingAll("expectedBaseOffset", "nextOffset");
        }
    }

    @Test
    void appendRawAdoptsForwardGapWhenLeaderRetainedPastFollower(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var log = lm.logFor("t", 0);
            // The leader's log starts at offset 5 — everything before was
            // deleted by retention. A follower at LEO 0 receives the
            // earliest available batch and must adopt it as-is.
            var records = List.of(new Record(0, 0L, null, new byte[] {7}), new Record(1, 0L, null, new byte[] {8}));
            var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
            RecordBatch.encode(buf, 5L, 0, 1_000L, 1_000L, -1L, (short) -1, -1, records);
            buf.flip();
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);

            log.appendRaw(bytes, 5L);

            assertThat(log.nextOffset()).isEqualTo(7L);
            // A fetch below the gap resolves to the earliest batch, same as
            // reading a compacted log below its first surviving offset.
            var batches = log.read(0L, 1024);
            assertThat(batches).hasSize(1);
            assertThat(batches.get(0).baseOffset()).isEqualTo(5L);
        }
        // The gap survives reopen: nextOffset recomputes from the batch header.
        try (var lm = lm(dir)) {
            assertThat(lm.logFor("t", 0).nextOffset()).isEqualTo(7L);
        }
    }

    @Test
    void appendRawPopulatesOffsetIndex(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var log = lm.logFor("t", 0);
            var records = List.of(new Record(0, 0L, null, new byte[] {1}));
            var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
            RecordBatch.encode(buf, 0L, 0, 1L, 1L, -1L, (short) -1, -1, records);
            buf.flip();
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);

            log.appendRaw(bytes, 0L);

            // Subsequent read at offset 0 must find the batch.
            assertThat(log.read(0L, 1024)).hasSize(1);
        }
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
