package jbroker.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * P8.3 — verifies {@link LogManager#deleteTopicDir} evicts the in-memory
 * {@code (topic, partition) -> Log} cache, closes the underlying handles,
 * and cleans up segment files on disk. Critical because a topic recreated
 * with the same name after a DELETE must start from offset 0 rather than
 * inheriting the old partition's offsets.
 */
class LogManagerDeleteTopicDirTest {

    @Test
    void deleteTopicDirEvictsCacheAndRemovesFiles(@TempDir Path root) throws Exception {
        try (var mgr = new LogManager(root, new LogManager.Config(1 << 20, Long.MAX_VALUE, 4096, 60_000))) {
            var a0 = mgr.logFor("orders", 0);
            var a1 = mgr.logFor("orders", 1);
            a0.append(List.of(new Record(0, 0L, null, "hello".getBytes())), System.currentTimeMillis());
            a1.append(List.of(new Record(0, 0L, null, "world".getBytes())), System.currentTimeMillis());
            assertThat(a0.nextOffset()).isEqualTo(1L);
            assertThat(Files.exists(root.resolve("orders-0"))).isTrue();
            assertThat(Files.exists(root.resolve("orders-1"))).isTrue();

            // Unrelated topic must survive the delete.
            var other = mgr.logFor("audits", 0);
            other.append(List.of(new Record(0, 0L, null, "keep".getBytes())), System.currentTimeMillis());

            mgr.deleteTopicDir("orders");

            assertThat(Files.exists(root.resolve("orders-0"))).isFalse();
            assertThat(Files.exists(root.resolve("orders-1"))).isFalse();
            assertThat(Files.exists(root.resolve("audits-0"))).isTrue();

            // Recreating the same topic hands back a fresh Log starting at
            // offset 0 — the bug P8.3 closes.
            var fresh = mgr.logFor("orders", 0);
            assertThat(fresh.nextOffset()).isEqualTo(0L);
        }
    }

    @Test
    void deleteOnUnknownTopicIsNoOp(@TempDir Path root) throws Exception {
        try (var mgr = new LogManager(root, new LogManager.Config(1 << 20, Long.MAX_VALUE, 4096, 60_000))) {
            mgr.logFor("other", 0);
            mgr.deleteTopicDir("nonexistent");
            assertThat(Files.exists(root.resolve("other-0"))).isTrue();
        }
    }
}
