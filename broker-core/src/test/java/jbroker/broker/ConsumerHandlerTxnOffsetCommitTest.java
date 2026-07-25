package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import jbroker.broker.group.GroupCoordinator;
import jbroker.broker.group.OffsetCache;
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
 * The {@code TxnOffsets.TxnOffsetCommit} binding: routing guard, the
 * transactional batch appended to the group's coordinator partition, the
 * stage against the transaction (committed view untouched), and the
 * PRODUCER_FENCED mapping.
 */
class ConsumerHandlerTxnOffsetCommitTest {

    private static final long T0 = 1_000_000_000L;
    private static final String GROUP = "ctp-app";

    @Test
    void stagesDurablyWithoutTouchingTheCommittedView(@TempDir Path dir) throws Exception {
        try (var lmgr = lm(dir)) {
            var tm = offsetsTopicLedBy(1);
            var cache = new OffsetCache();
            var coordinator = coord(tm);
            var handler = handler(lmgr, tm, coordinator, cache);

            var resp = handler.txnOffsetCommit(request(GROUP, 7L, 0, 42L));
            assertThat(resp.getResultsList()).hasSize(1);
            assertThat(resp.getResults(0).getError()).isEqualTo(ErrorCode.OK);

            // Staged, not committed: FetchOffsets must not see it yet.
            assertThat(cache.get(GROUP, "orders", 0)).isEmpty();
            assertThat(coordinator.txnOffsetStaging().stagedFor(GROUP, 7L)).hasSize(1);

            // Durable: the batch is in the log, TRANSACTIONAL under the
            // producer identity, and holds the partition's LSO at its base.
            var log = lmgr.logFor(ConsumerOffsetsTopic.NAME, 0);
            var batches = log.read(0, 1 << 20);
            assertThat(batches).hasSize(1);
            assertThat(batches.get(0).transactional()).isTrue();
            assertThat(batches.get(0).producerId()).isEqualTo(7L);
            assertThat(log.lastStableOffset(log.nextOffset())).isZero();
        }
    }

    @Test
    void staleEpochAnswersProducerFencedOnEveryResult(@TempDir Path dir) throws Exception {
        try (var lmgr = lm(dir)) {
            var tm = offsetsTopicLedBy(1);
            var coordinator = coord(tm);
            var handler = handler(lmgr, tm, coordinator, new OffsetCache());

            assertThat(handler.txnOffsetCommit(request(GROUP, 7L, 3, 42L))
                            .getResults(0)
                            .getError())
                    .isEqualTo(ErrorCode.OK);
            var stale = handler.txnOffsetCommit(request(GROUP, 7L, 2, 43L));
            assertThat(stale.getResultsList())
                    .allSatisfy(r -> assertThat(r.getError()).isEqualTo(ErrorCode.PRODUCER_FENCED));
            // The staged epoch-3 offsets are untouched by the zombie.
            assertThat(coordinator.txnOffsetStaging().stagedFor(GROUP, 7L)).hasSize(1);
        }
    }

    @Test
    void nonCoordinatorAnswersWithSuggestedCoordinatorHints(@TempDir Path dir) throws Exception {
        try (var lmgr = lm(dir)) {
            // Broker 2 leads the coordinator partition; self is 1.
            var tm = offsetsTopicLedBy(2);
            var registry = new BrokerRegistry();
            registry.onBrokerRegistration(2, "broker2.internal", 9092, "broker2.example.com", 31092, 7000);
            var clock = new AtomicLong(T0);
            var handler =
                    new ConsumerHandler(tm, lmgr, registry, coord(tm), new OffsetCache(), 1, clock::get, clock::get);

            var resp = handler.txnOffsetCommit(request(GROUP, 7L, 0, 42L));
            assertThat(resp.getResultsList())
                    .allSatisfy(r -> assertThat(r.getError()).isEqualTo(ErrorCode.NOT_COORDINATOR));
            assertThat(resp.getSuggestedCoordinatorId()).isEqualTo(2);
            assertThat(resp.getSuggestedCoordinatorHost()).isEqualTo("broker2.example.com");
            assertThat(resp.getSuggestedCoordinatorPort()).isEqualTo(31092);
            // Nothing was appended on the wrong broker.
            assertThat(lmgr.logFor(ConsumerOffsetsTopic.NAME, 0).nextOffset()).isZero();
        }
    }

    @Test
    void missingProducerIdentityAnswersInvalidTxnState(@TempDir Path dir) throws Exception {
        try (var lmgr = lm(dir)) {
            var tm = offsetsTopicLedBy(1);
            var handler = handler(lmgr, tm, coord(tm), new OffsetCache());
            var resp = handler.txnOffsetCommit(TxnOffsetCommitRequest.newBuilder()
                    .setGroupId(GROUP)
                    .setProducerId(-1L)
                    .setProducerEpoch(0)
                    .addOffsets(entry(42L))
                    .build());
            assertThat(resp.getResults(0).getError()).isEqualTo(ErrorCode.INVALID_TXN_STATE);
        }
    }

    @Test
    void sameEpochRetryMergesInsteadOfDuplicating(@TempDir Path dir) throws Exception {
        try (var lmgr = lm(dir)) {
            var tm = offsetsTopicLedBy(1);
            var coordinator = coord(tm);
            var handler = handler(lmgr, tm, coordinator, new OffsetCache());

            assertThat(handler.txnOffsetCommit(request(GROUP, 7L, 0, 42L))
                            .getResults(0)
                            .getError())
                    .isEqualTo(ErrorCode.OK);
            assertThat(handler.txnOffsetCommit(request(GROUP, 7L, 0, 45L))
                            .getResults(0)
                            .getError())
                    .isEqualTo(ErrorCode.OK);
            var staged = coordinator.txnOffsetStaging().stagedFor(GROUP, 7L);
            assertThat(staged).hasSize(1);
            assertThat(staged.values().iterator().next().offset()).isEqualTo(45L);
        }
    }

    @Test
    void recoveryWalkReconstructsTheUndecidedStage(@TempDir Path dir) throws Exception {
        try (var lmgr = lm(dir)) {
            var tm = offsetsTopicLedBy(1);
            var handler = handler(lmgr, tm, coord(tm), new OffsetCache());
            handler.txnOffsetCommit(request(GROUP, 7L, 0, 42L));

            // A fresh coordinator replaying the partition (what activation
            // runs) finds the staged-but-undecided transaction again and
            // still keeps the committed view empty.
            var recoveredCache = new OffsetCache();
            var recoveredStaging = new jbroker.broker.group.TxnOffsetStaging();
            jbroker.broker.group.OffsetCacheRecovery.rebuild(
                    lmgr, ConsumerOffsetsTopic.NAME, 0, recoveredCache, recoveredStaging);
            assertThat(recoveredCache.get(GROUP, "orders", 0)).isEmpty();
            assertThat(recoveredStaging.stagedFor(GROUP, 7L)).hasSize(1);
        }
    }

    // --- helpers ---

    private static TxnOffsetCommitRequest request(String group, long pid, int epoch, long offset) {
        return TxnOffsetCommitRequest.newBuilder()
                .setTransactionalId("txn-1")
                .setGroupId(group)
                .setProducerId(pid)
                .setProducerEpoch(epoch)
                .addOffsets(entry(offset))
                .build();
    }

    private static TxnOffsetCommitEntry entry(long offset) {
        return TxnOffsetCommitEntry.newBuilder()
                .setTp(TopicPartition.newBuilder()
                        .setTopic("orders")
                        .setPartition(0)
                        .build())
                .setOffset(offset)
                .build();
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
