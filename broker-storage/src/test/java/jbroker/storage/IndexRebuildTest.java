package jbroker.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * spec §Milestone 4 acceptance gate: {@code .index} and {@code .timeindex} can be regenerated
 * from {@code .log} on startup. We delete them and reopen; reads at arbitrary
 * offsets should still work (falling back to a sequential scan from the
 * segment's start when the index lookup misses).
 */
class IndexRebuildTest {

    @Test
    void readStillWorksAfterIndexFilesDeleted(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, new Log.Config(10_000, 0, 512))) {
            for (int i = 0; i < 50; i++) {
                log.append(List.of(new Record(0, 0L, null, new byte[64])), 1_000L + i);
            }
            log.force();
        }

        // Delete every .index and .timeindex file.
        try (var stream = Files.list(dir)) {
            stream.filter(p -> {
                        var n = p.getFileName().toString();
                        return n.endsWith(".index") || n.endsWith(".timeindex");
                    })
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        }

        try (var reopened = Log.open(dir, new Log.Config(10_000, 0, 512))) {
            // Read from the start: a sequential scan fills in what the
            // sparse index would normally accelerate.
            var batches = reopened.read(0L, 64 * 1024);
            assertThat(batches).isNotEmpty();
            // And from the middle: without an index the segment falls back
            // to scanning from position 0 within its segment, so we still
            // see some batch whose lastOffset >= 25.
            var mid = reopened.read(25L, 64 * 1024);
            assertThat(mid).isNotEmpty();
            assertThat(mid.get(0).lastOffset()).isGreaterThanOrEqualTo(25L);
        }
    }
}
