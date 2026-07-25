package jbroker.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class TransactionStateTest {

    @Test
    void lsoIsTheCallerBoundaryWhenNothingIsOngoing() {
        var state = new TransactionState();
        assertThat(state.lastStableOffset(0L)).isEqualTo(0L);
        assertThat(state.lastStableOffset(42L)).isEqualTo(42L);
        assertThat(state.firstOngoingOffset()).isEmpty();
    }

    @Test
    void openTransactionHoldsLsoAtItsFirstOffset() {
        var state = new TransactionState();
        state.onTransactionalData(1L, 10L, 14L);
        assertThat(state.lastStableOffset(50L)).isEqualTo(10L);
        assertThat(state.firstOngoingOffset()).hasValue(10L);
    }

    @Test
    void lsoNeverExceedsTheCallerBoundary() {
        var state = new TransactionState();
        state.onTransactionalData(1L, 10L, 14L);
        assertThat(state.lastStableOffset(5L)).isEqualTo(5L);
    }

    @Test
    void commitReleasesLso() {
        var state = new TransactionState();
        state.onTransactionalData(1L, 10L, 14L);
        state.onControl(1L, ControlRecord.Type.COMMIT);
        assertThat(state.lastStableOffset(50L)).isEqualTo(50L);
        assertThat(state.abortedTxnsIn(0L, Long.MAX_VALUE)).isEmpty();
    }

    @Test
    void abortReleasesLsoAndRecordsTheDataRange() {
        var state = new TransactionState();
        state.onTransactionalData(1L, 10L, 14L);
        state.onTransactionalData(1L, 20L, 24L);
        state.onControl(1L, ControlRecord.Type.ABORT);
        assertThat(state.lastStableOffset(50L)).isEqualTo(50L);
        assertThat(state.abortedTxnsIn(0L, Long.MAX_VALUE))
                .containsExactly(new TransactionState.AbortedTxn(1L, 10L, 24L));
    }

    @Test
    void extendingBatchesKeepTheFirstOffset() {
        var state = new TransactionState();
        state.onTransactionalData(1L, 10L, 11L);
        state.onTransactionalData(1L, 30L, 35L);
        assertThat(state.lastStableOffset(100L)).isEqualTo(10L);
    }

    @Test
    void interleavedProducersTrackTheEarliestOpenTransaction() {
        var state = new TransactionState();
        state.onTransactionalData(1L, 10L, 14L);
        state.onTransactionalData(2L, 20L, 24L);
        assertThat(state.lastStableOffset(100L)).isEqualTo(10L);

        state.onControl(1L, ControlRecord.Type.COMMIT);
        assertThat(state.lastStableOffset(100L))
                .as("earliest ongoing moves to pid 2")
                .isEqualTo(20L);

        state.onControl(2L, ControlRecord.Type.ABORT);
        assertThat(state.lastStableOffset(100L)).isEqualTo(100L);
        assertThat(state.abortedTxnsIn(0L, Long.MAX_VALUE))
                .containsExactly(new TransactionState.AbortedTxn(2L, 20L, 24L));
    }

    @Test
    void laterProducerCommittingFirstDoesNotMoveLso() {
        var state = new TransactionState();
        state.onTransactionalData(1L, 10L, 14L);
        state.onTransactionalData(2L, 20L, 24L);
        state.onControl(2L, ControlRecord.Type.COMMIT);
        assertThat(state.lastStableOffset(100L)).as("pid 1 still undecided").isEqualTo(10L);
    }

    @Test
    void markerForUnknownProducerIsANoop() {
        var state = new TransactionState();
        state.onControl(9L, ControlRecord.Type.ABORT);
        assertThat(state.lastStableOffset(5L)).isEqualTo(5L);
        assertThat(state.abortedTxnsIn(0L, Long.MAX_VALUE)).isEmpty();
    }

    @Test
    void abortedWindowQueryUsesHalfOpenOverlap() {
        var state = new TransactionState();
        state.onTransactionalData(1L, 5L, 9L);
        state.onControl(1L, ControlRecord.Type.ABORT);
        var txn = new TransactionState.AbortedTxn(1L, 5L, 9L);

        assertThat(state.abortedTxnsIn(0L, 5L))
                .as("window ends before the range")
                .isEmpty();
        assertThat(state.abortedTxnsIn(0L, 6L)).containsExactly(txn);
        assertThat(state.abortedTxnsIn(9L, 20L))
                .as("last offset still inside window")
                .containsExactly(txn);
        assertThat(state.abortedTxnsIn(10L, 20L))
                .as("window starts past the range")
                .isEmpty();
        assertThat(state.abortedTxnsIn(6L, 8L)).as("window inside the range").containsExactly(txn);
    }

    @Test
    void abortedResultsAreSortedByFirstOffsetEvenWhenAbortedOutOfOrder() {
        var state = new TransactionState();
        state.onTransactionalData(1L, 10L, 14L);
        state.onTransactionalData(2L, 20L, 24L);
        // pid 2 aborts before pid 1: insertion order is (2, then 1).
        state.onControl(2L, ControlRecord.Type.ABORT);
        state.onControl(1L, ControlRecord.Type.ABORT);
        assertThat(state.abortedTxnsIn(0L, Long.MAX_VALUE))
                .containsExactly(
                        new TransactionState.AbortedTxn(1L, 10L, 14L), new TransactionState.AbortedTxn(2L, 20L, 24L));
    }

    @Test
    void evictionDropsRangesWhollyBelowTheLogStart() {
        var state = new TransactionState();
        state.onTransactionalData(1L, 5L, 9L);
        state.onControl(1L, ControlRecord.Type.ABORT);
        state.onTransactionalData(2L, 15L, 24L);
        state.onControl(2L, ControlRecord.Type.ABORT);

        state.evictAbortedBelow(10L);
        assertThat(state.abortedTxnsIn(0L, Long.MAX_VALUE))
                .containsExactly(new TransactionState.AbortedTxn(2L, 15L, 24L));

        // Partially overlapping ranges survive: their tail is still fetchable.
        state.evictAbortedBelow(20L);
        assertThat(state.abortedTxnsIn(0L, Long.MAX_VALUE))
                .containsExactly(new TransactionState.AbortedTxn(2L, 15L, 24L));

        state.evictAbortedBelow(25L);
        assertThat(state.abortedTxnsIn(0L, Long.MAX_VALUE)).isEmpty();
    }

    @Test
    void negativeProducerIdRefusedOnDataPath() {
        var state = new TransactionState();
        assertThatThrownBy(() -> state.onTransactionalData(-1L, 0L, 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("producerId");
    }

    @Test
    void applyReplaysEventsAndIgnoresDataWithoutProducerIdentity() {
        var state = new TransactionState();
        state.apply(TransactionState.TxnEvent.data(-1L, 0L, 1L)); // foreign-stamped bit: ignored
        state.apply(TransactionState.TxnEvent.data(1L, 10L, 14L));
        state.apply(TransactionState.TxnEvent.data(2L, 20L, 24L));
        state.apply(TransactionState.TxnEvent.control(1L, 30L, ControlRecord.Type.ABORT));
        assertThat(state.lastStableOffset(100L)).isEqualTo(20L);
        assertThat(state.abortedTxnsIn(0L, Long.MAX_VALUE))
                .containsExactly(new TransactionState.AbortedTxn(1L, 10L, 14L));
        assertThat(state.ongoingCount()).isEqualTo(1);
    }

    @Test
    void clearForgetsEverything() {
        var state = new TransactionState();
        state.onTransactionalData(1L, 10L, 14L);
        state.onTransactionalData(2L, 20L, 24L);
        state.onControl(2L, ControlRecord.Type.ABORT);
        state.clear();
        assertThat(state.lastStableOffset(100L)).isEqualTo(100L);
        assertThat(state.abortedTxnsIn(0L, Long.MAX_VALUE)).isEmpty();
        assertThat(state.ongoingCount()).isZero();
    }
}
