package jbroker.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Root cause of the CompactionFollowerFetchIT CI flake (run 28790677875,
 * ClosedChannelException on 2 of 1270 concurrent fetches):
 * {@code Log.read}/{@code transferTo} resolve a segment lock-free, but
 * {@code compactByKeyLocked} closes the old segments before swapping the
 * list — a reader holding the stale reference then reads a closed
 * channel. The read paths must re-resolve and retry; after the swap the
 * fresh segment serves the same (sparse-preserved) offsets.
 */
class LogCompactionReadRaceTest {

    @Test
    void concurrentReadsNeverThrowAcrossRepeatedCompactions(@TempDir Path dir) throws Exception {
        var log = Log.open(dir, new Log.Config(128L * 1024 * 1024, Long.MAX_VALUE, 4096));
        int keys = 5;
        for (int i = 0; i < 200; i++) {
            var key = ("k" + (i % keys)).getBytes(StandardCharsets.UTF_8);
            log.append(List.of(new Record(0, 0L, key, ("v" + i).getBytes(StandardCharsets.UTF_8))), 1_000L);
        }

        var stop = new AtomicBoolean(false);
        var failures = new CopyOnWriteArrayList<Throwable>();
        try (var exec = Executors.newFixedThreadPool(4)) {
            for (int t = 0; t < 4; t++) {
                exec.submit(() -> {
                    long offset = 0;
                    while (!stop.get()) {
                        try {
                            var batches = log.read(offset, 64 * 1024);
                            if (batches.isEmpty()) {
                                offset = 0;
                                continue;
                            }
                            offset = batches.get(batches.size() - 1).lastOffset() + 1;
                            if (offset >= log.nextOffset()) offset = 0;
                        } catch (Throwable e) {
                            failures.add(e);
                            return;
                        }
                    }
                });
            }

            // Interleave compactions and fresh appends so every compaction
            // closes segments that readers are actively resolving into.
            for (int round = 0; round < 60 && failures.isEmpty(); round++) {
                log.compactByKey();
                for (int i = 0; i < 40; i++) {
                    var key = ("k" + (i % keys)).getBytes(StandardCharsets.UTF_8);
                    log.append(
                            List.of(new Record(0, 0L, key, ("r" + round + "-" + i).getBytes(StandardCharsets.UTF_8))),
                            1_000L);
                }
            }
            stop.set(true);
        }
        log.close();

        assertThat(failures)
                .as("no concurrent read may throw across compaction segment swaps")
                .isEmpty();
    }
}
