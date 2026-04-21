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
    void appendRawRejectsBatchWithWrongBaseOffset(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var log = lm.logFor("t", 0);
            // Leader batch with baseOffset = 0.
            var records = List.of(new Record(0, 0L, null, new byte[] {1}));
            var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
            RecordBatch.encode(buf, 0L, 0, 1L, 1L, -1L, (short) -1, -1, records);
            buf.flip();
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);

            // Follower's local log is empty (nextOffset = 0). It expects the
            // leader's batch to start there — claiming expectedBaseOffset=5
            // should fail because that's a gap the follower isn't allowed
            // to create.
            assertThatThrownBy(() -> log.appendRaw(bytes, 5L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContainingAll("expectedBaseOffset", "nextOffset");
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
