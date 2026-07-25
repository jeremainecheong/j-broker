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
import jbroker.proto.common.ErrorCode;
import jbroker.proto.common.TopicPartition;
import jbroker.proto.txn.TxnOffsetCommitEntry;
import jbroker.proto.txn.TxnOffsetCommitRequest;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The marker hook ({@link ConsumerHandler#onTxnMarker}): a COMMIT marker
 * folds the staged offsets into the committed view AND re-appends them as
 * regular offset records; an ABORT discards them; a redelivered marker is
 * a no-op (no duplicate plain copies).
 */
class ConsumerHandlerTxnMarkerFoldTest {

    private static final long T0 = 1_000_000_000L;
    private static final String GROUP = "ctp-app";
    private static final long PID = 7L;

    @Test
    void commitMarkerFoldsIntoTheCacheAndReappendsPlainRecords(@TempDir Path dir) throws Exception {
        try (var lmgr = lm(dir)) {
            var tm = offsetsTopicLedBy(1);
            var cache = new OffsetCache();
            var coordinator = coord(tm);
            var handler = handler(lmgr, tm, coordinator, cache);
            stage(handler, 0, 42L);
            long endAfterStage = lmgr.logFor(ConsumerOffsetsTopic.NAME, 0).nextOffset();

            handler.onTxnMarker(0, PID, 0, /*commit*/ true);

            // Folded: FetchOffsets sees the offset now, the stage is gone.
            assertThat(cache.get(GROUP, "orders", 0))
                    .hasValueSatisfying(oam -> assertThat(oam.offset()).isEqualTo(42L));
            assertThat(coordinator.txnOffsetStaging().stagedFor(GROUP, PID)).isEmpty();

            // Durable plain copy: one fresh NON-transactional batch after
            // the staged one, carrying the same offset record.
            var log = lmgr.logFor(ConsumerOffsetsTopic.NAME, 0);
            var appended = log.read(endAfterStage, 1 << 20);
            assertThat(appended).hasSize(1);
            assertThat(appended.get(0).transactional()).isFalse();
            assertThat(appended.get(0).control()).isFalse();

            // The plain copy alone reconstructs the committed view — the
            // guarantee a staged-batch-dropping cleaner would rely on.
            var recovered = new OffsetCache();
            OffsetCacheRecovery.rebuild(lmgr, ConsumerOffsetsTopic.NAME, 0, recovered);
            assertThat(recovered.get(GROUP, "orders", 0))
                    .hasValueSatisfying(oam -> assertThat(oam.offset()).isEqualTo(42L));
        }
    }

    @Test
    void abortMarkerDiscardsWithoutTouchingTheCacheOrTheLog(@TempDir Path dir) throws Exception {
        try (var lmgr = lm(dir)) {
            var tm = offsetsTopicLedBy(1);
            var cache = new OffsetCache();
            var coordinator = coord(tm);
            var handler = handler(lmgr, tm, coordinator, cache);
            stage(handler, 0, 42L);
            long endAfterStage = lmgr.logFor(ConsumerOffsetsTopic.NAME, 0).nextOffset();

            handler.onTxnMarker(0, PID, 0, /*commit*/ false);

            assertThat(cache.get(GROUP, "orders", 0)).isEmpty();
            assertThat(coordinator.txnOffsetStaging().stagedFor(GROUP, PID)).isEmpty();
            assertThat(lmgr.logFor(ConsumerOffsetsTopic.NAME, 0).nextOffset())
                    .as("abort re-appends nothing")
                    .isEqualTo(endAfterStage);
        }
    }

    @Test
    void redeliveredMarkerIsANoOp(@TempDir Path dir) throws Exception {
        try (var lmgr = lm(dir)) {
            var tm = offsetsTopicLedBy(1);
            var cache = new OffsetCache();
            var handler = handler(lmgr, tm, coord(tm), cache);
            stage(handler, 0, 42L);
            handler.onTxnMarker(0, PID, 0, true);
            long endAfterFold = lmgr.logFor(ConsumerOffsetsTopic.NAME, 0).nextOffset();

            handler.onTxnMarker(0, PID, 0, true);
            assertThat(lmgr.logFor(ConsumerOffsetsTopic.NAME, 0).nextOffset())
                    .as("no duplicate plain copies on redelivery")
                    .isEqualTo(endAfterFold);
        }
    }

    @Test
    void markerOnAForeignPartitionLeavesTheStageAlone(@TempDir Path dir) throws Exception {
        try (var lmgr = lm(dir)) {
            // Two coordinator partitions, both led by self; the stage lives
            // under partition 0 and a marker on partition 1 must not
            // decide it.
            var tm = new TopicManager();
            tm.onTopicCommitted(ConsumerOffsetsTopic.NAME, 2, 1, 0L, true, true);
            tm.onPartitionChange(ConsumerOffsetsTopic.NAME, 0, 1, List.of(1), List.of(1), 0, 0);
            tm.onPartitionChange(ConsumerOffsetsTopic.NAME, 1, 1, List.of(1), List.of(1), 0, 0);
            var cache = new OffsetCache();
            var coordinator = coord(tm);
            var handler = handler(lmgr, tm, coordinator, cache);
            // GROUP routes to floorMod(hash, 2); stage through the real path
            // so the staged partition matches the routing.
            stage(handler, Math.floorMod(GROUP.hashCode(), 2), 42L);
            int otherPartition = 1 - Math.floorMod(GROUP.hashCode(), 2);

            handler.onTxnMarker(otherPartition, PID, 0, true);
            assertThat(cache.get(GROUP, "orders", 0)).isEmpty();
            assertThat(coordinator.txnOffsetStaging().stagedFor(GROUP, PID)).hasSize(1);
        }
    }

    // --- helpers ---

    private static void stage(ConsumerHandler handler, int expectedPartition, long offset) {
        var resp = handler.txnOffsetCommit(TxnOffsetCommitRequest.newBuilder()
                .setTransactionalId("txn-1")
                .setGroupId(GROUP)
                .setProducerId(PID)
                .setProducerEpoch(0)
                .addOffsets(TxnOffsetCommitEntry.newBuilder()
                        .setTp(TopicPartition.newBuilder()
                                .setTopic("orders")
                                .setPartition(0)
                                .build())
                        .setOffset(offset)
                        .build())
                .build());
        assertThat(resp.getResults(0).getError()).isEqualTo(ErrorCode.OK);
    }

    private static ConsumerHandler handler(
            LogManager lmgr, TopicManager tm, GroupCoordinator coordinator, OffsetCache cache) {
        var clock = new AtomicLong(T0);
        return new ConsumerHandler(tm, lmgr, new BrokerRegistry(), coordinator, cache, 1, clock::get, clock::get);
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

    private static LogManager lm(Path dir) throws Exception {
        return new LogManager(
                dir,
                new LogManager.Config(
                        128L * 1024 * 1024,
                        Long.MAX_VALUE,
                        LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        TimeUnit.MINUTES.toMillis(5)));
    }
}
