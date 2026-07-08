package jbroker.broker.group;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import jbroker.broker.ConsumerOffsetsTopic;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Proves the recovery walk reads {@code __consumer_offsets}
 * partitions back into the {@link OffsetCache} with latest-write-wins
 * semantics (read-time compaction).
 */
class OffsetCacheRecoveryTest {

    @Test
    void rebuildRecoversSingleCommit(@TempDir Path dir) throws IOException {
        var lm = newLogManager(dir);
        appendOffset(lm, /*partition*/ 0, "g1", "orders", 0, /*offset*/ 42L, /*epoch*/ 3, "ckpt", 100L);

        var cache = new OffsetCache();
        int applied = OffsetCacheRecovery.rebuild(lm, /*partition*/ 0, cache);

        assertThat(applied).isEqualTo(1);
        var oam = cache.get("g1", "orders", 0).orElseThrow();
        assertThat(oam.offset()).isEqualTo(42L);
        assertThat(oam.leaderEpoch()).isEqualTo(3);
        assertThat(oam.metadata()).isEqualTo("ckpt");
        assertThat(oam.commitTimestamp()).isEqualTo(100L);
    }

    @Test
    void latestRecordWinsForSameKey(@TempDir Path dir) throws IOException {
        var lm = newLogManager(dir);
        // Three commits for the same (group, topic, partition) at increasing
        // offsets — the recovery walk should keep only the last.
        appendOffset(lm, 0, "g1", "orders", 0, 10L, 1, "v1", 100L);
        appendOffset(lm, 0, "g1", "orders", 0, 20L, 1, "v2", 200L);
        appendOffset(lm, 0, "g1", "orders", 0, 30L, 1, "v3", 300L);

        var cache = new OffsetCache();
        int applied = OffsetCacheRecovery.rebuild(lm, 0, cache);

        assertThat(applied).isEqualTo(3); // applied count includes overwrites
        assertThat(cache.size()).isEqualTo(1);
        assertThat(cache.get("g1", "orders", 0).orElseThrow().offset()).isEqualTo(30L);
        assertThat(cache.get("g1", "orders", 0).orElseThrow().metadata()).isEqualTo("v3");
    }

    @Test
    void multipleGroupsAndPartitionsCoexist(@TempDir Path dir) throws IOException {
        var lm = newLogManager(dir);
        appendOffset(lm, 0, "g1", "orders", 0, 1L, 0, "", 0L);
        appendOffset(lm, 0, "g1", "orders", 1, 2L, 0, "", 0L);
        appendOffset(lm, 0, "g2", "orders", 0, 3L, 0, "", 0L);

        var cache = new OffsetCache();
        OffsetCacheRecovery.rebuild(lm, 0, cache);

        assertThat(cache.size()).isEqualTo(3);
        assertThat(cache.get("g1", "orders", 0).orElseThrow().offset()).isEqualTo(1L);
        assertThat(cache.get("g1", "orders", 1).orElseThrow().offset()).isEqualTo(2L);
        assertThat(cache.get("g2", "orders", 0).orElseThrow().offset()).isEqualTo(3L);
    }

    @Test
    void emptyLogReturnsZero(@TempDir Path dir) throws IOException {
        var lm = newLogManager(dir);
        // touch the partition log to materialise it without writing anything
        lm.logFor(ConsumerOffsetsTopic.NAME, 0);

        var cache = new OffsetCache();
        int applied = OffsetCacheRecovery.rebuild(lm, 0, cache);

        assertThat(applied).isZero();
        assertThat(cache.size()).isZero();
    }

    @Test
    void groupMetadataRecordsAreSkipped(@TempDir Path dir) throws IOException {
        var lm = newLogManager(dir);
        // Mix one offset record + one type-2 group-metadata record.
        appendOffset(lm, 0, "g1", "orders", 0, 7L, 0, "", 0L);
        appendBatch(
                lm,
                0,
                List.of(new Record(
                        /*offsetDelta*/ 0,
                        /*timestampDelta*/ 0L,
                        ConsumerOffsetsTopic.keyForGroupMetadata("g1"),
                        new byte[] {0x42, 0x00, 0x01})));

        var cache = new OffsetCache();
        int applied = OffsetCacheRecovery.rebuild(lm, 0, cache);

        // Only the offset record applied; the group-metadata one is
        // silently skipped (a separate recovery walk reads those into GroupCoordinator).
        assertThat(applied).isEqualTo(1);
        assertThat(cache.get("g1", "orders", 0).orElseThrow().offset()).isEqualTo(7L);
    }

    private static void appendOffset(
            LogManager lm,
            int partition,
            String group,
            String topic,
            int tpPartition,
            long offset,
            int leaderEpoch,
            String metadata,
            long commitTs)
            throws IOException {
        byte[] key = ConsumerOffsetsTopic.keyForOffset(group, topic, tpPartition);
        byte[] value = ConsumerOffsetsTopic.valueForOffset(offset, leaderEpoch, metadata, commitTs);
        appendBatch(lm, partition, List.of(new Record(0, 0L, key, value)));
    }

    private static void appendBatch(LogManager lm, int partition, List<Record> records) throws IOException {
        var log = lm.logFor(ConsumerOffsetsTopic.NAME, partition);
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        long base = log.nextOffset();
        long now = System.currentTimeMillis();
        RecordBatch.encode(buf, base, 0, now, now, -1L, (short) -1, -1, records);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        log.appendRaw(bytes, base);
    }

    private static LogManager newLogManager(Path dir) throws IOException {
        return new LogManager(
                dir,
                new LogManager.Config(
                        128L * 1024 * 1024,
                        Long.MAX_VALUE,
                        LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        java.util.concurrent.TimeUnit.MINUTES.toMillis(5)));
    }
}
