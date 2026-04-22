package jbroker.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LogTruncateTest {

    @Test
    void truncateToDropsRecordsAtOrAboveGivenOffset(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var log = lm.logFor("t", 0);
            for (int i = 0; i < 5; i++) {
                log.append(List.of(new Record(0, 0L, null, new byte[] {(byte) i})), 1_000L);
            }
            assertThat(log.nextOffset()).isEqualTo(5L);

            log.truncateTo(3L);

            assertThat(log.nextOffset()).isEqualTo(3L);
            assertThat(log.read(0L, 1024))
                    .allSatisfy(p -> assertThat(p.lastOffset()).isLessThan(3L));
        }
    }

    @Test
    void truncateToLeoIsNoOp(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var log = lm.logFor("t", 0);
            for (int i = 0; i < 3; i++) {
                log.append(List.of(new Record(0, 0L, null, new byte[] {(byte) i})), 1_000L);
            }
            log.truncateTo(3L);
            assertThat(log.nextOffset()).isEqualTo(3L);
        }
    }

    @Test
    void truncateToZeroEmptiesLog(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var log = lm.logFor("t", 0);
            log.append(List.of(new Record(0, 0L, null, new byte[] {1})), 1_000L);
            log.append(List.of(new Record(0, 0L, null, new byte[] {2})), 1_000L);

            log.truncateTo(0L);

            assertThat(log.nextOffset()).isZero();
            assertThat(log.read(0L, 1024)).isEmpty();
        }
    }

    @Test
    void appendAfterTruncateUsesExpectedOffset(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var log = lm.logFor("t", 0);
            for (int i = 0; i < 5; i++) {
                log.append(List.of(new Record(0, 0L, null, new byte[] {(byte) i})), 1_000L);
            }
            log.truncateTo(2L);

            long assigned = log.append(List.of(new Record(0, 0L, null, new byte[] {7})), 2_000L);
            assertThat(assigned).isEqualTo(2L);
            assertThat(log.nextOffset()).isEqualTo(3L);
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
