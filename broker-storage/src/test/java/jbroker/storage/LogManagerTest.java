package jbroker.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LogManagerTest {

    @Test
    void logsAreIsolatedPerTopicPartition(@TempDir Path dir) throws Exception {
        try (var mgr = new LogManager(dir, new LogManager.Config(10_000, 60_000, 4096, 1_000_000))) {
            var a = mgr.logFor("orders", 0);
            var b = mgr.logFor("orders", 1);
            assertThat(a).isNotSameAs(b);

            a.append(List.of(new Record(0, 0L, null, "a".getBytes())), 1L);
            b.append(List.of(new Record(0, 0L, null, "b".getBytes())), 2L);

            var aBatches = a.read(0L, 64 * 1024);
            var bBatches = b.read(0L, 64 * 1024);
            assertThat(aBatches.get(0).records().get(0).value()).containsExactly('a');
            assertThat(bBatches.get(0).records().get(0).value()).containsExactly('b');
        }
    }

    @Test
    void logsAreCachedByTopicPartitionKey(@TempDir Path dir) throws Exception {
        try (var mgr = new LogManager(dir, new LogManager.Config(10_000, 60_000, 4096, 1_000_000))) {
            var a1 = mgr.logFor("orders", 0);
            var a2 = mgr.logFor("orders", 0);
            assertThat(a1).isSameAs(a2);
        }
    }

    @Test
    void freshRootIsStampedWithCurrentFormat(@TempDir Path dir) throws Exception {
        try (var mgr = new LogManager(dir, new LogManager.Config(10_000, 60_000, 4096, 1_000_000))) {
            mgr.logFor("orders", 0);
        }
        var marker = dir.resolve(FormatVersion.FILE_NAME);
        assertThat(marker).exists();
        assertThat(Files.readString(marker).trim()).isEqualTo(Integer.toString(FormatVersion.CURRENT));
    }

    @Test
    void unstampedRootWithExistingSegmentsIsStampedAndStillReads(@TempDir Path dir) throws Exception {
        try (var mgr = new LogManager(dir, new LogManager.Config(10_000, 60_000, 4096, 1_000_000))) {
            mgr.logFor("orders", 0).append(List.of(new Record(0, 0L, null, "a".getBytes())), 1L);
        }
        // A dir written before markers existed: same layout, no marker.
        Files.delete(dir.resolve(FormatVersion.FILE_NAME));

        try (var mgr = new LogManager(dir, new LogManager.Config(10_000, 60_000, 4096, 1_000_000))) {
            var batches = mgr.logFor("orders", 0).read(0L, 64 * 1024);
            assertThat(batches.get(0).records().get(0).value()).containsExactly('a');
        }
        assertThat(Files.readString(dir.resolve(FormatVersion.FILE_NAME)).trim())
                .isEqualTo(Integer.toString(FormatVersion.CURRENT));
    }

    @Test
    void rootStampedByNewerBrokerRefusesToOpen(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve(FormatVersion.FILE_NAME), (FormatVersion.CURRENT + 1) + "\n");
        assertThatThrownBy(() -> new LogManager(dir, new LogManager.Config(10_000, 60_000, 4096, 1_000_000)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("newer broker")
                .hasMessageContaining("upgrade the binary or restore from backup");
    }
}
