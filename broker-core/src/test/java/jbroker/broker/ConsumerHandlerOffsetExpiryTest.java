package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import jbroker.broker.group.GroupCoordinator;
import jbroker.broker.group.OffsetCache;
import jbroker.broker.group.OffsetCacheRecovery;
import jbroker.broker.group.RangeAssignor;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import jbroker.storage.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Offset expiry (R2.7): commits of groups with no live members whose
 * newest commit outlived the retention window are deleted — durably.
 * The tombstone must land in {@code __consumer_offsets} before the cache
 * drops, so a restart's recovery walk cannot resurrect the offsets.
 */
class ConsumerHandlerOffsetExpiryTest {

    private static final long RETENTION_MS = TimeUnit.DAYS.toMillis(7);
    private static final long T0 = 1_000_000_000L;

    private static LogManager lm(Path dir) throws Exception {
        return new LogManager(
                dir,
                new LogManager.Config(
                        128L * 1024 * 1024,
                        Long.MAX_VALUE,
                        LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        TimeUnit.MINUTES.toMillis(5)));
    }

    private static TopicManager offsetsTopicLedBy(int leader) {
        var tm = new TopicManager();
        tm.onTopicCommitted(ConsumerOffsetsTopic.NAME, 1, 1, 0L, true, true);
        tm.onPartitionChange(ConsumerOffsetsTopic.NAME, 0, leader, List.of(leader), List.of(leader), 0, 0);
        return tm;
    }

    private static GroupCoordinator coord(TopicManager tm) {
        var counter = new AtomicInteger();
        return new GroupCoordinator(
                topic -> tm.describe(topic).map(TopicDescription::partitions).orElse(0),
                new RangeAssignor(),
                instanceId -> "member-" + counter.incrementAndGet());
    }

    /** Commit one offset both durably (log record) and in the cache, as the commit path does. */
    private static void commit(LogManager lmgr, OffsetCache cache, String group, long ts) throws Exception {
        byte[] key = ConsumerOffsetsTopic.keyForOffset(group, "orders", 0);
        byte[] value = ConsumerOffsetsTopic.valueForOffset(42L, 0, "", ts);
        lmgr.logFor(ConsumerOffsetsTopic.NAME, 0).append(List.of(new Record(0, 0L, key, value)), ts);
        cache.put(group, "orders", 0, new OffsetCache.OffsetAndMetadata(42L, 0, "", ts));
    }

    @Test
    void idleGroupExpiresDurably(@TempDir Path dir) throws Exception {
        try (var lmgr = lm(dir)) {
            var tm = offsetsTopicLedBy(1);
            var cache = new OffsetCache();
            var clock = new AtomicLong(T0);
            var handler =
                    new ConsumerHandler(tm, lmgr, new BrokerRegistry(), coord(tm), cache, 1, clock::get, clock::get);
            commit(lmgr, cache, "idle", T0);

            // Inside the window: nothing expires.
            clock.set(T0 + RETENTION_MS - 1);
            assertThat(handler.expireStaleOffsets(RETENTION_MS)).isZero();
            assertThat(cache.get("idle", "orders", 0)).isPresent();

            // Past the window: the commit expires and the cache clears.
            clock.set(T0 + RETENTION_MS + 1);
            assertThat(handler.expireStaleOffsets(RETENTION_MS)).isEqualTo(1);
            assertThat(cache.get("idle", "orders", 0)).isEmpty();

            // Durable: a fresh recovery walk (what a restart runs) applies
            // the tombstone — the offsets stay gone.
            var recovered = new OffsetCache();
            OffsetCacheRecovery.rebuild(lmgr, 0, recovered);
            assertThat(recovered.get("idle", "orders", 0)).isEmpty();
        }
    }

    @Test
    void freshCommitsAndForeignGroupsSurvive(@TempDir Path dir) throws Exception {
        try (var lmgr = lm(dir)) {
            var tm = offsetsTopicLedBy(1);
            var cache = new OffsetCache();
            var clock = new AtomicLong(T0 + RETENTION_MS * 2);
            var handler =
                    new ConsumerHandler(tm, lmgr, new BrokerRegistry(), coord(tm), cache, 1, clock::get, clock::get);
            // Freshly committed — inside the window even though the group is empty.
            commit(lmgr, cache, "fresh", clock.get() - 1_000);

            assertThat(handler.expireStaleOffsets(RETENTION_MS)).isZero();
            assertThat(cache.get("fresh", "orders", 0)).isPresent();
        }
    }

    @Test
    void nonCoordinatorNeverExpires(@TempDir Path dir) throws Exception {
        try (var lmgr = lm(dir)) {
            // Broker 2 leads the coordinator partition; self (1) must not touch the group.
            var tm = offsetsTopicLedBy(2);
            var cache = new OffsetCache();
            var clock = new AtomicLong(T0 + RETENTION_MS * 2);
            var handler =
                    new ConsumerHandler(tm, lmgr, new BrokerRegistry(), coord(tm), cache, 1, clock::get, clock::get);
            commit(lmgr, cache, "idle", T0);

            assertThat(handler.expireStaleOffsets(RETENTION_MS)).isZero();
            assertThat(cache.get("idle", "orders", 0)).isPresent();
        }
    }

    @Test
    void disabledRetentionIsInert(@TempDir Path dir) throws Exception {
        try (var lmgr = lm(dir)) {
            var tm = offsetsTopicLedBy(1);
            var cache = new OffsetCache();
            var clock = new AtomicLong(T0 + RETENTION_MS * 10);
            var handler =
                    new ConsumerHandler(tm, lmgr, new BrokerRegistry(), coord(tm), cache, 1, clock::get, clock::get);
            commit(lmgr, cache, "idle", T0);

            assertThat(handler.expireStaleOffsets(-1L)).isZero();
            assertThat(cache.get("idle", "orders", 0)).isPresent();
        }
    }
}
