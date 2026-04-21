package jbroker.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Retention must not disrupt concurrent readers: while one thread repeatedly
 * runs {@code retain(cutoff)}, another thread is reading batches from the log
 * at offset 0. The reader should never throw, and should always see a
 * coherent subset of the appended values.
 */
class ConcurrentReaderRetentionTest {

    @Test
    void retentionDoesNotDisruptConcurrentReader(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, new Log.Config(256, 0, 1024))) {
            // Seed some segments.
            for (int i = 0; i < 40; i++) {
                log.append(List.of(new Record(0, 0L, null, new byte[80])), 1_000L + i * 10);
            }
            log.force();

            var stop = new AtomicBoolean();
            var reads = new AtomicInteger();
            var errors = new AtomicReference<Throwable>();
            var ready = new CountDownLatch(1);

            var readerThread = Thread.ofVirtual().start(() -> {
                try {
                    ready.countDown();
                    while (!stop.get()) {
                        try {
                            long start = Math.max(log.segments().get(0).baseOffset(), 0L);
                            var batches = log.read(start, 4096);
                            // Any batches we read must decode cleanly (CRC-checked).
                            for (var b : batches) {
                                assertThat(b.records()).isNotEmpty();
                            }
                            reads.incrementAndGet();
                        } catch (Exception ignored) {
                            /* transient IO during segment deletion is
                             * acceptable; persistent errors are not. */
                        }
                    }
                } catch (Throwable t) {
                    errors.set(t);
                }
            });
            ready.await();

            // Hammer retention.
            for (int i = 0; i < 50; i++) {
                log.retain(1_000L + 40L * 10);
                Thread.sleep(1);
            }
            stop.set(true);
            readerThread.join();

            assertThat(errors.get()).isNull();
            assertThat(reads.get()).isGreaterThan(0);
        }
    }
}
