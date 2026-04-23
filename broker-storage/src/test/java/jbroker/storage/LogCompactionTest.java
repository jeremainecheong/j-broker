package jbroker.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * P9.5 — unit coverage for {@link Log#compactByKey()}. Verifies the three
 * semantic rules: latest-value-wins per key, tombstone drops the key, and
 * null-keyed records pass through untouched.
 */
final class LogCompactionTest {

    private Path dir;
    private Log log;

    @BeforeEach
    void setUp() throws Exception {
        dir = Files.createTempDirectory("compact-");
        log = Log.open(dir, Log.Config.defaults());
    }

    @AfterEach
    void tearDown() throws Exception {
        log.close();
        try (var walk = Files.walk(dir)) {
            walk.sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (Exception ignored) {
                }
            });
        }
    }

    @Test
    void latestValuePerKeyWins() throws Exception {
        // 100 records over 10 keys — each key repeated 10 times with
        // "v{i}" where i is the iteration index.
        for (int i = 0; i < 100; i++) {
            String key = "k" + (i % 10);
            String value = "v" + i;
            log.append(
                    List.of(new Record(
                            0, 0L, key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8))),
                    1_000L + i);
        }
        assertThat(log.nextOffset()).isEqualTo(100L);

        int kept = log.compactByKey();
        assertThat(kept).isEqualTo(10);

        var surviving = readAll(log);
        assertThat(surviving).hasSize(10);
        // Last value for k0 is "v90" (offset 90), for k9 is "v99".
        var valueByKey = new java.util.HashMap<String, String>();
        for (var r : surviving) {
            valueByKey.put(new String(r.key(), StandardCharsets.UTF_8), new String(r.value(), StandardCharsets.UTF_8));
        }
        assertThat(valueByKey).containsEntry("k0", "v90").containsEntry("k9", "v99");
    }

    @Test
    void tombstoneDropsKey() throws Exception {
        log.append(List.of(rec("k1", "v1")), 1_000L);
        log.append(List.of(rec("k2", "v2")), 1_000L);
        log.append(List.of(tombstone("k1")), 1_000L);

        int kept = log.compactByKey();
        assertThat(kept).isEqualTo(1);

        var surviving = readAll(log);
        assertThat(surviving).hasSize(1);
        assertThat(new String(surviving.get(0).key(), StandardCharsets.UTF_8)).isEqualTo("k2");
    }

    @Test
    void nullKeyPassthrough() throws Exception {
        log.append(List.of(new Record(0, 0L, null, "v1".getBytes())), 1_000L);
        log.append(List.of(rec("k1", "v1")), 1_000L);
        log.append(List.of(rec("k1", "v2")), 1_000L);
        log.append(List.of(new Record(0, 0L, null, "v3".getBytes())), 1_000L);

        int kept = log.compactByKey();
        // 2 null-keyed + 1 k1 = 3.
        assertThat(kept).isEqualTo(3);
        assertThat(log.nextOffset()).isEqualTo(3L);
    }

    private static Record rec(String key, String value) {
        return new Record(0, 0L, key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    private static Record tombstone(String key) {
        return new Record(0, 0L, key.getBytes(StandardCharsets.UTF_8), null);
    }

    private static List<Record> readAll(Log log) throws Exception {
        var out = new ArrayList<Record>();
        long pos = 0;
        long limit = log.nextOffset();
        while (pos < limit) {
            var batches = log.read(pos, 64 * 1024);
            if (batches.isEmpty()) break;
            for (var b : batches) {
                out.addAll(b.records());
                pos = b.lastOffset() + 1;
            }
        }
        return out;
    }
}
