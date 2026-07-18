package jbroker.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The offline verifier reports what a backup holds and where it stops
 * decoding — without modifying a byte (the live recovery scan truncates;
 * a verify pass over a backup must never).
 */
class LogVerifierTest {

    private static void fill(Path topicsDir, String partition, int records) throws Exception {
        try (var log = Log.open(topicsDir.resolve(partition), new Log.Config(1 << 20, 0, 4096))) {
            for (int i = 0; i < records; i++) {
                log.append(List.of(new Record(0, 0L, null, ("r" + i).getBytes())), 1_000L + i);
            }
            log.force();
        }
    }

    @Test
    void cleanPartitionsReportCountsAndOffsets(@TempDir Path dir) throws Exception {
        fill(dir, "orders-0", 5);
        fill(dir, "orders-1", 3);

        var reports = LogVerifier.verify(dir);

        assertThat(reports).hasSize(2);
        assertThat(LogVerifier.allClean(reports)).isTrue();
        var p0 = reports.get(0);
        assertThat(p0.partitionDir()).isEqualTo("orders-0");
        assertThat(p0.records()).isEqualTo(5);
        assertThat(p0.nextOffset()).isEqualTo(5);
    }

    @Test
    void corruptionIsReportedWithoutModifyingTheFile(@TempDir Path dir) throws Exception {
        fill(dir, "orders-0", 5);
        Path logFile;
        try (var stream = Files.list(dir.resolve("orders-0"))) {
            logFile = stream.filter(p -> p.getFileName().toString().endsWith(".log"))
                    .findFirst()
                    .orElseThrow();
        }
        long sizeBefore = Files.size(logFile);
        // Flip a CRC-covered byte in the middle of the file.
        try (var ch = FileChannel.open(logFile, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            long target = sizeBefore / 2;
            var one = ByteBuffer.allocate(1);
            ch.read(one, target);
            one.flip();
            ch.write(ByteBuffer.wrap(new byte[] {(byte) (one.get() ^ 0x40)}), target);
        }

        var reports = LogVerifier.verify(dir);

        assertThat(LogVerifier.allClean(reports)).isFalse();
        assertThat(reports.get(0).problem()).contains(".log position");
        assertThat(Files.size(logFile)).as("verify must never write").isEqualTo(sizeBefore);
    }

    @Test
    void tornTailIsReported(@TempDir Path dir) throws Exception {
        fill(dir, "orders-0", 2);
        Path logFile;
        try (var stream = Files.list(dir.resolve("orders-0"))) {
            logFile = stream.filter(p -> p.getFileName().toString().endsWith(".log"))
                    .findFirst()
                    .orElseThrow();
        }
        try (var ch = FileChannel.open(logFile, StandardOpenOption.APPEND)) {
            ch.write(ByteBuffer.wrap(new byte[17]));
        }

        var reports = LogVerifier.verify(dir);

        assertThat(reports.get(0).problem()).contains("partial trailing frame");
    }
}
