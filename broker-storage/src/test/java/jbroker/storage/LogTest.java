package jbroker.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LogTest {

    @Test
    void appendsAndReadsSingleSegment(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, new Log.Config(1_000_000, 0, 4096))) {
            long last = log.append(List.of(new Record(0, 0L, null, "hello".getBytes())), 1_000L);
            assertThat(last).isEqualTo(0L);

            var batches = log.read(0L, 64 * 1024);
            assertThat(batches).hasSize(1);
            assertThat(batches.get(0).records().get(0).value()).containsExactly("hello".getBytes());
        }
    }

    @Test
    void rollsOverToNewSegmentOnSizeThreshold(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, new Log.Config(500, 0, 4096))) {
            for (int i = 0; i < 10; i++) {
                log.append(List.of(new Record(0, 0L, null, new byte[80])), i);
            }
            assertThat(log.segments().size()).isGreaterThanOrEqualTo(2);
        }
    }

    @Test
    void retentionDeletesOldSegments(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, new Log.Config(200, 0, 4096))) {
            // Fill enough to cause several rollovers with ascending timestamps.
            for (int i = 0; i < 20; i++) {
                log.append(List.of(new Record(0, 0L, null, new byte[80])), 1_000L + i * 10);
            }
            int sizeBefore = log.segments().size();
            int removed = log.retain(/* cutoff */ 1_000L + 20 * 10);
            assertThat(removed).isGreaterThan(0);
            assertThat(log.segments().size()).isLessThan(sizeBefore);
        }
    }

    @Test
    void sizeRetentionDeletesOldestSegmentsDownToTheLimit(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, new Log.Config(200, 0, 4096))) {
            for (int i = 0; i < 20; i++) {
                log.append(List.of(new Record(0, 0L, null, new byte[80])), 1_000L + i);
            }
            long sizeBefore = totalBytes(log);
            assertThat(log.segments().size()).isGreaterThan(3);

            long limit = sizeBefore / 3;
            int removed = log.retain(Long.MIN_VALUE, limit);

            assertThat(removed).isGreaterThan(0);
            long sizeAfter = totalBytes(log);
            assertThat(sizeAfter).isGreaterThanOrEqualTo(limit);
            // Deleting one more segment would have dropped below the limit.
            assertThat(sizeAfter - log.segments().get(0).sizeBytes()).isLessThan(limit);
            // Reads below the new start resolve to the earliest survivor.
            var batches = log.read(0L, 64 * 1024);
            assertThat(batches).isNotEmpty();
            assertThat(batches.get(0).baseOffset())
                    .isEqualTo(log.segments().get(0).baseOffset());
        }
    }

    @Test
    void sizeRetentionNeverDeletesTheActiveSegment(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, new Log.Config(200, 0, 4096))) {
            for (int i = 0; i < 20; i++) {
                log.append(List.of(new Record(0, 0L, null, new byte[80])), 1_000L + i);
            }
            long nextBefore = log.nextOffset();

            // A zero-byte budget deletes every closed segment but must
            // leave the active one — the log tail stays appendable.
            log.retain(Long.MIN_VALUE, 0L);

            assertThat(log.segments()).hasSize(1);
            assertThat(log.nextOffset()).isEqualTo(nextBefore);
            log.append(List.of(new Record(0, 0L, null, new byte[8])), 2_000L);
            assertThat(log.nextOffset()).isEqualTo(nextBefore + 1);
        }
    }

    @Test
    void negativeRetentionBytesDisablesTheSizePass(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, new Log.Config(200, 0, 4096))) {
            for (int i = 0; i < 20; i++) {
                log.append(List.of(new Record(0, 0L, null, new byte[80])), 1_000L + i);
            }
            int segmentsBefore = log.segments().size();

            int removed = log.retain(Long.MIN_VALUE, -1L);

            assertThat(removed).isZero();
            assertThat(log.segments()).hasSize(segmentsBefore);
        }
    }

    private static long totalBytes(Log log) throws Exception {
        long total = 0;
        for (var seg : log.segments()) total += seg.sizeBytes();
        return total;
    }

    @Test
    void reopensAndContinuesFromLastOffset(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, new Log.Config(1_000_000, 0, 4096))) {
            log.append(List.of(new Record(0, 0L, null, new byte[16])), 1L);
            log.append(List.of(new Record(0, 0L, null, new byte[16])), 2L);
            log.force();
        }
        try (var reopened = Log.open(dir, new Log.Config(1_000_000, 0, 4096))) {
            assertThat(reopened.nextOffset()).isEqualTo(2L);
            reopened.append(List.of(new Record(0, 0L, null, new byte[16])), 3L);
            assertThat(reopened.nextOffset()).isEqualTo(3L);
        }
    }

    @Test
    void recoversFromTornTailByTruncating(@TempDir Path dir) throws Exception {
        // Write two batches, then deliberately corrupt the tail by appending
        // garbage bytes to the .log file. On reopen, the corrupt region
        // should be truncated and the log remain readable.
        try (var log = Log.open(dir, new Log.Config(1_000_000, 0, 4096))) {
            log.append(List.of(new Record(0, 0L, null, "a".getBytes())), 1L);
            log.append(List.of(new Record(0, 0L, null, "b".getBytes())), 2L);
            log.force();
        }
        // Pick the .log file and append a torn batch: some zero bytes.
        Path logFile;
        try (var stream = Files.list(dir)) {
            logFile = stream.filter(p -> p.getFileName().toString().endsWith(".log"))
                    .findFirst()
                    .orElseThrow();
        }
        try (var ch = FileChannel.open(logFile, StandardOpenOption.APPEND)) {
            ch.write(ByteBuffer.wrap(new byte[32])); // garbage tail
        }
        try (var reopened = Log.open(dir, new Log.Config(1_000_000, 0, 4096))) {
            var batches = reopened.read(0L, 64 * 1024);
            // The two original batches are intact; the corrupt tail was truncated.
            assertThat(batches).hasSize(2);
            assertThat(new String(batches.get(0).records().get(0).value())).isEqualTo("a");
            assertThat(new String(batches.get(1).records().get(0).value())).isEqualTo("b");
        }
    }

    @Test
    void transferToWritesRawBytesToOutputStream(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, new Log.Config(1_000_000, 0, 4096))) {
            log.append(List.of(new Record(0, 0L, null, "zero-copy".getBytes())), 1L);
            var baos = new ByteArrayOutputStream();
            long n = log.transferTo(0L, 64 * 1024, baos);
            assertThat(n).isGreaterThan(0);
            var buf = ByteBuffer.wrap(baos.toByteArray());
            var parsed = RecordBatch.decode(buf);
            assertThat(new String(parsed.records().get(0).value())).isEqualTo("zero-copy");
        }
    }
}
