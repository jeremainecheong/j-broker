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

    @Test
    void formatOneRootKeepsItsMarkerUntilTheFirstControlBatch(@TempDir Path dir) throws Exception {
        // A directory written by a pre-transactions broker: format 1.
        Files.writeString(dir.resolve(FormatVersion.FILE_NAME), "1\n");
        try (var mgr = new LogManager(dir, new LogManager.Config(10_000, 60_000, 4096, 1_000_000))) {
            var log = mgr.logFor("orders", 0);
            log.append(List.of(new Record(0, 0L, null, "plain".getBytes())), 1L);
            log.append(
                    List.of(new Record(0, 0L, null, "txn".getBytes())),
                    2L,
                    /*producerId*/ 5L,
                    (short) 0,
                    0,
                    0,
                    Compression.NONE,
                    /*transactional*/ true);
            // Data appends — even transactional ones — never move the
            // marker: only a control batch changes what a format-1 reader
            // would misread.
            assertThat(Files.readString(dir.resolve(FormatVersion.FILE_NAME)).trim())
                    .isEqualTo("1");

            log.appendControl(5L, (short) 0, new ControlRecord(ControlRecord.Type.COMMIT, 1), 3L, 0);
            assertThat(Files.readString(dir.resolve(FormatVersion.FILE_NAME)).trim())
                    .isEqualTo(Integer.toString(FormatVersion.TRANSACTIONS));
        }
        // The gate is the downgrade fence: a pre-transactions binary
        // (current = 1) now refuses this directory.
        assertThatThrownBy(() -> FormatVersion.check(dir, 1))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("newer broker");
    }

    @Test
    void controlWritesAfterTheFirstStampAreCheap(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve(FormatVersion.FILE_NAME), "1\n");
        try (var mgr = new LogManager(dir, new LogManager.Config(10_000, 60_000, 4096, 1_000_000))) {
            var log = mgr.logFor("orders", 0);
            for (long producerId = 1; producerId <= 3; producerId++) {
                log.appendControl(producerId, (short) 0, new ControlRecord(ControlRecord.Type.ABORT, 1), 1L, 0);
            }
            assertThat(Files.readString(dir.resolve(FormatVersion.FILE_NAME)).trim())
                    .isEqualTo(Integer.toString(FormatVersion.TRANSACTIONS));
        }
        // Reopening the stamped dir works and control writes still pass the gate.
        try (var mgr = new LogManager(dir, new LogManager.Config(10_000, 60_000, 4096, 1_000_000))) {
            mgr.logFor("orders", 0)
                    .appendControl(9L, (short) 0, new ControlRecord(ControlRecord.Type.COMMIT, 1), 2L, 0);
        }
    }

    @Test
    void replicatedControlBatchesAlsoStampTheMarker(@TempDir Path dir) throws Exception {
        // A follower receives control batches through appendRaw, never
        // appendControl — its data dir needs the same downgrade fence.
        byte[] encoded;
        {
            var buf = java.nio.ByteBuffer.allocate(256);
            int written = RecordBatch.encodeControl(
                    buf, 0L, 0, 1L, 5L, (short) 0, new ControlRecord(ControlRecord.Type.COMMIT, 1));
            encoded = java.util.Arrays.copyOf(buf.array(), written);
        }
        Files.writeString(dir.resolve(FormatVersion.FILE_NAME), "1\n");
        try (var mgr = new LogManager(dir, new LogManager.Config(10_000, 60_000, 4096, 1_000_000))) {
            mgr.logFor("orders", 0).appendRaw(encoded, 0L);
            assertThat(Files.readString(dir.resolve(FormatVersion.FILE_NAME)).trim())
                    .isEqualTo(Integer.toString(FormatVersion.TRANSACTIONS));
        }
    }
}
