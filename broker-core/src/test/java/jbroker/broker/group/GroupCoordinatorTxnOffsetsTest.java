package jbroker.broker.group;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import jbroker.proto.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Staged transactional offset commits on {@link GroupCoordinator}: a
 * stage never touches the committed view, a COMMIT marker folds it into
 * the cache (returning the folded offsets for the durable re-append), an
 * ABORT marker discards it, and stale producer epochs are fenced at both
 * the stage and the marker.
 */
final class GroupCoordinatorTxnOffsetsTest {

    private static final long PID = 9L;
    private static final int P = 3; // the group's coordinator partition

    private GroupCoordinator coordinator;
    private OffsetCache cache;

    @BeforeEach
    void setUp() {
        coordinator = new GroupCoordinator(topic -> 4, new RangeAssignor());
        cache = new OffsetCache();
    }

    @Test
    void stageLeavesCommittedViewUntouched() {
        var out = coordinator.stageTxnOffsets("g1", P, PID, 5, offsets("orders", 0, 42L));

        assertThat(out).isEqualTo(TxnOffsetStaging.StageOutcome.OK);
        assertThat(cache.get("g1", "orders", 0)).isEmpty();
        assertThat(coordinator.txnOffsetStaging().stagedCount()).isEqualTo(1);
        assertThat(coordinator.txnOffsetStaging().stagedFor("g1", PID)).containsKey(tp("orders", 0));
    }

    @Test
    void commitMarkerFoldsStagedOffsetsIntoCache() {
        coordinator.stageTxnOffsets("g1", P, PID, 5, offsets("orders", 0, 42L));
        coordinator.stageTxnOffsets("g1", P, PID, 5, offsets("orders", 1, 7L));

        var folded = coordinator.onTxnMarker(P, PID, 5, /*commit*/ true, cache);

        assertThat(folded).hasSize(1);
        assertThat(folded.get(0).groupId()).isEqualTo("g1");
        assertThat(folded.get(0).offsets()).containsKeys(tp("orders", 0), tp("orders", 1));
        assertThat(cache.get("g1", "orders", 0).orElseThrow().offset()).isEqualTo(42L);
        assertThat(cache.get("g1", "orders", 1).orElseThrow().offset()).isEqualTo(7L);
        assertThat(coordinator.txnOffsetStaging().stagedCount()).isZero();
    }

    @Test
    void abortMarkerDiscardsStagedOffsets() {
        coordinator.stageTxnOffsets("g1", P, PID, 5, offsets("orders", 0, 42L));

        var folded = coordinator.onTxnMarker(P, PID, 5, /*commit*/ false, cache);

        assertThat(folded).isEmpty();
        assertThat(cache.get("g1", "orders", 0)).isEmpty();
        assertThat(coordinator.txnOffsetStaging().stagedCount()).isZero();
    }

    @Test
    void sameEpochStagesMergePerPartitionLaterWins() {
        coordinator.stageTxnOffsets("g1", P, PID, 5, offsets("orders", 0, 10L));
        coordinator.stageTxnOffsets("g1", P, PID, 5, offsets("orders", 0, 20L));

        coordinator.onTxnMarker(P, PID, 5, true, cache);

        assertThat(cache.get("g1", "orders", 0).orElseThrow().offset()).isEqualTo(20L);
    }

    @Test
    void staleEpochStageIsFenced() {
        coordinator.stageTxnOffsets("g1", P, PID, 5, offsets("orders", 0, 42L));

        var out = coordinator.stageTxnOffsets("g1", P, PID, 4, offsets("orders", 0, 99L));

        assertThat(out).isEqualTo(TxnOffsetStaging.StageOutcome.PRODUCER_FENCED);
        // The staged content is untouched by the fenced attempt.
        coordinator.onTxnMarker(P, PID, 5, true, cache);
        assertThat(cache.get("g1", "orders", 0).orElseThrow().offset()).isEqualTo(42L);
    }

    @Test
    void higherEpochStageReplacesTheDoomedLowerEpochStage() {
        coordinator.stageTxnOffsets("g1", P, PID, 5, offsets("orders", 0, 42L));
        // Producer re-registered (epoch bump) and staged its next txn.
        coordinator.stageTxnOffsets("g1", P, PID, 6, offsets("orders", 1, 7L));

        // The abandoned epoch-5 transaction's late ABORT marker must not
        // touch the epoch-6 stage.
        coordinator.onTxnMarker(P, PID, 5, false, cache);
        assertThat(coordinator.txnOffsetStaging().stagedFor("g1", PID)).containsOnlyKeys(tp("orders", 1));

        coordinator.onTxnMarker(P, PID, 6, true, cache);
        assertThat(cache.get("g1", "orders", 0)).isEmpty(); // epoch-5 offsets replaced, never committed
        assertThat(cache.get("g1", "orders", 1).orElseThrow().offset()).isEqualTo(7L);
    }

    @Test
    void zombieStageAfterDecisionIsFenced() {
        coordinator.stageTxnOffsets("g1", P, PID, 5, offsets("orders", 0, 42L));
        coordinator.onTxnMarker(P, PID, 5, true, cache);

        var out = coordinator.stageTxnOffsets("g1", P, PID, 4, offsets("orders", 0, 99L));

        assertThat(out).isEqualTo(TxnOffsetStaging.StageOutcome.PRODUCER_FENCED);
        assertThat(coordinator.txnOffsetStaging().stagedCount()).isZero();
    }

    @Test
    void equalEpochRestageAfterDecisionBelongsToTheNextTransaction() {
        coordinator.stageTxnOffsets("g1", P, PID, 5, offsets("orders", 0, 42L));
        coordinator.onTxnMarker(P, PID, 5, true, cache);

        // Same-session next transaction reuses the epoch.
        var out = coordinator.stageTxnOffsets("g1", P, PID, 5, offsets("orders", 0, 50L));
        assertThat(out).isEqualTo(TxnOffsetStaging.StageOutcome.OK);

        coordinator.onTxnMarker(P, PID, 5, true, cache);
        assertThat(cache.get("g1", "orders", 0).orElseThrow().offset()).isEqualTo(50L);
    }

    @Test
    void markerOnADifferentPartitionLeavesTheStageAlone() {
        coordinator.stageTxnOffsets("g1", P, PID, 5, offsets("orders", 0, 42L));

        var folded = coordinator.onTxnMarker(P + 1, PID, 5, true, cache);

        assertThat(folded).isEmpty();
        assertThat(cache.get("g1", "orders", 0)).isEmpty();
        assertThat(coordinator.txnOffsetStaging().stagedCount()).isEqualTo(1);
    }

    @Test
    void markerWithNothingStagedIsIdempotentNoop() {
        assertThat(coordinator.onTxnMarker(P, PID, 5, true, cache)).isEmpty();
        assertThat(cache.size()).isZero();
    }

    @Test
    void distinctGroupsUnderTheSameProducerFoldIndependently() {
        coordinator.stageTxnOffsets("g1", P, PID, 5, offsets("orders", 0, 42L));
        coordinator.stageTxnOffsets("g2", P, PID, 5, offsets("orders", 0, 17L));

        var folded = coordinator.onTxnMarker(P, PID, 5, true, cache);

        assertThat(folded).extracting(TxnOffsetStaging.FoldedOffsets::groupId).containsExactlyInAnyOrder("g1", "g2");
        assertThat(cache.get("g1", "orders", 0).orElseThrow().offset()).isEqualTo(42L);
        assertThat(cache.get("g2", "orders", 0).orElseThrow().offset()).isEqualTo(17L);
    }

    // ---------- helpers ----------

    private static Map<TopicPartition, OffsetCache.OffsetAndMetadata> offsets(
            String topic, int partition, long offset) {
        return Map.of(tp(topic, partition), new OffsetCache.OffsetAndMetadata(offset, 0, "", 100L));
    }

    private static TopicPartition tp(String topic, int partition) {
        return TopicPartition.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .build();
    }
}
