package jbroker.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The append listener is the storage-side half of event-driven
 * replication: every successful append — data, raw replica apply, control
 * marker — must fire it exactly once, and a log without a listener must
 * behave as before.
 */
class LogAppendListenerTest {

    @Test
    void listenerFiresOnEveryAppendVariant(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, new Log.Config(1_000_000, 0, 4096))) {
            var fired = new AtomicInteger();
            log.setAppendListener(fired::incrementAndGet);

            log.append(List.of(new Record(0, 0L, null, "a".getBytes())), 1_000L);
            assertThat(fired.get()).isEqualTo(1);

            var records = List.of(new Record(0, 0L, null, "b".getBytes()));
            var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
            RecordBatch.encode(buf, 1L, 0, 1_000L, 1_000L, -1L, (short) -1, -1, records);
            buf.flip();
            byte[] raw = new byte[buf.remaining()];
            buf.get(raw);
            log.appendRaw(raw, 1L);
            assertThat(fired.get()).isEqualTo(2);

            log.appendControl(7L, (short) 0, new ControlRecord(ControlRecord.Type.COMMIT, 1), 1_000L, 0);
            assertThat(fired.get()).isEqualTo(3);
        }
    }

    @Test
    void appendsWorkWithoutAListener(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, new Log.Config(1_000_000, 0, 4096))) {
            long last = log.append(List.of(new Record(0, 0L, null, "solo".getBytes())), 1_000L);
            assertThat(last).isEqualTo(0L);
        }
    }

    @Test
    void logManagerRoutesListenerPerPartitionIncludingAlreadyOpenLogs(@TempDir Path dir) throws Exception {
        try (var lm = new LogManager(
                dir,
                new LogManager.Config(
                        128L * 1024 * 1024,
                        Long.MAX_VALUE,
                        LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        TimeUnit.MINUTES.toMillis(5)))) {
            // Opened BEFORE registration — must be wired retroactively.
            var early = lm.logFor("orders", 0);
            var seen = new java.util.concurrent.CopyOnWriteArrayList<String>();
            lm.setAppendListener((topic, partition) -> seen.add(topic + "-" + partition));

            early.append(List.of(new Record(0, 0L, null, "x".getBytes())), 1_000L);
            // Opened AFTER registration — wired on open. Dashes in the
            // topic name must not confuse the partition parse.
            lm.logFor("multi-dash-topic", 3).append(List.of(new Record(0, 0L, null, "y".getBytes())), 1_000L);

            assertThat(seen).containsExactly("orders-0", "multi-dash-topic-3");
        }
    }
}
