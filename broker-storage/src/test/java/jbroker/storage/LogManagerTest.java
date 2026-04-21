package jbroker.storage;

import static org.assertj.core.api.Assertions.assertThat;

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
}
