package jbroker.broker.txn;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import jbroker.broker.ErrorCodes;
import jbroker.proto.common.TopicPartition;
import org.junit.jupiter.api.Test;

class TxnCoordinatorTest {

    private static final int COORD_EPOCH = 5;
    private static final TopicPartition ORDERS_0 = tp("orders", 0);
    private static final TopicPartition ORDERS_1 = tp("orders", 1);
    private static final TopicPartition PAYMENTS_2 = tp("payments", 2);

    private static TopicPartition tp(String topic, int partition) {
        return TopicPartition.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .build();
    }

    private static TxnCoordinator newCoordinator() {
        return new TxnCoordinator(COORD_EPOCH);
    }

    // ---------- Happy paths ----------

    @Test
    void fullCommitPathWalksEmptyOngoingPrepareCommitCompleteCommit() {
        var coord = newCoordinator();

        var init = coord.initTransactions("t1", 30_000, /*pidIfNew*/ 100L, /*now*/ 1_000L);
        assertThat(init.errorCode()).isEqualTo(ErrorCodes.NONE);
        assertThat(init.producerId()).isEqualTo(100L);
        assertThat(init.producerEpoch()).isZero();
        assertThat(init.markers()).isEmpty();
        var initRec = init.stateRecord().orElseThrow();
        assertThat(initRec.state()).isEqualTo(TxnState.EMPTY);
        assertThat(initRec.partitions()).isEmpty();
        assertThat(initRec.timeoutMs()).isEqualTo(30_000);
        assertThat(initRec.lastUpdateMs()).isEqualTo(1_000L);

        var add = coord.addPartitions("t1", 100L, 0, List.of(ORDERS_0, PAYMENTS_2), 2_000L);
        assertThat(add.errorCode()).isEqualTo(ErrorCodes.NONE);
        var addRec = add.stateRecord().orElseThrow();
        assertThat(addRec.state()).isEqualTo(TxnState.ONGOING);
        assertThat(addRec.partitions()).containsExactly(ORDERS_0, PAYMENTS_2);

        var end = coord.endTxn("t1", 100L, 0, /*commit*/ true, 3_000L);
        assertThat(end.errorCode()).isEqualTo(ErrorCodes.NONE);
        assertThat(end.stateRecord().orElseThrow().state()).isEqualTo(TxnState.PREPARE_COMMIT);
        assertThat(end.markers()).hasSize(2);
        for (var m : end.markers()) {
            assertThat(m.transactionalId()).isEqualTo("t1");
            assertThat(m.producerId()).isEqualTo(100L);
            assertThat(m.producerEpoch()).isZero();
            assertThat(m.commit()).isTrue();
            assertThat(m.coordinatorEpoch()).isEqualTo(COORD_EPOCH);
        }
        assertThat(end.markers().stream().map(TxnCoordinator.MarkerInstruction::tp))
                .containsExactly(ORDERS_0, PAYMENTS_2);

        var done = coord.markersDelivered("t1", 100L, 0, 4_000L);
        assertThat(done.errorCode()).isEqualTo(ErrorCodes.NONE);
        assertThat(done.stateRecord().orElseThrow().state()).isEqualTo(TxnState.COMPLETE_COMMIT);
        assertThat(coord.stateOf("t1").orElseThrow().state()).isEqualTo(TxnState.COMPLETE_COMMIT);
        assertThat(coord.pendingMarkers()).isEmpty();
    }

    @Test
    void fullAbortPathWalksEmptyOngoingPrepareAbortCompleteAbort() {
        var coord = newCoordinator();
        coord.initTransactions("t1", 30_000, 100L, 1_000L);
        coord.addPartitions("t1", 100L, 0, List.of(ORDERS_0), 2_000L);

        var end = coord.endTxn("t1", 100L, 0, /*commit*/ false, 3_000L);
        assertThat(end.errorCode()).isEqualTo(ErrorCodes.NONE);
        assertThat(end.stateRecord().orElseThrow().state()).isEqualTo(TxnState.PREPARE_ABORT);
        assertThat(end.markers()).singleElement().satisfies(m -> {
            assertThat(m.tp()).isEqualTo(ORDERS_0);
            assertThat(m.commit()).isFalse();
        });

        var done = coord.markersDelivered("t1", 100L, 0, 4_000L);
        assertThat(done.stateRecord().orElseThrow().state()).isEqualTo(TxnState.COMPLETE_ABORT);
    }

    @Test
    void nextTransactionOnSameIdStartsWithFreshPartitionSet() {
        var coord = newCoordinator();
        coord.initTransactions("t1", 30_000, 100L, 0L);
        coord.addPartitions("t1", 100L, 0, List.of(ORDERS_0), 1L);
        coord.endTxn("t1", 100L, 0, true, 2L);
        coord.markersDelivered("t1", 100L, 0, 3L);

        // Same epoch — a committed transaction doesn't fence the producer.
        var add = coord.addPartitions("t1", 100L, 0, List.of(PAYMENTS_2), 4L);
        assertThat(add.errorCode()).isEqualTo(ErrorCodes.NONE);
        var rec = add.stateRecord().orElseThrow();
        assertThat(rec.state()).isEqualTo(TxnState.ONGOING);
        assertThat(rec.partitions()).containsExactly(PAYMENTS_2); // ORDERS_0 belonged to the finished txn
    }

    @Test
    void addPartitionsExtendsOngoingAndDuplicateRetryIsANoOp() {
        var coord = newCoordinator();
        coord.initTransactions("t1", 30_000, 100L, 0L);
        coord.addPartitions("t1", 100L, 0, List.of(ORDERS_0), 1L);

        var extend = coord.addPartitions("t1", 100L, 0, List.of(ORDERS_0, ORDERS_1), 2L);
        assertThat(extend.errorCode()).isEqualTo(ErrorCodes.NONE);
        assertThat(extend.stateRecord().orElseThrow().partitions()).containsExactly(ORDERS_0, ORDERS_1);

        // Exact retry: success, but nothing new to persist.
        var retry = coord.addPartitions("t1", 100L, 0, List.of(ORDERS_0, ORDERS_1), 3L);
        assertThat(retry.errorCode()).isEqualTo(ErrorCodes.NONE);
        assertThat(retry.stateRecord()).isEmpty();
    }

    // ---------- Epoch fencing on every input ----------

    @Test
    void initBumpsEpochAndFencesEveryInputFromThePreviousEpoch() {
        var coord = newCoordinator();
        coord.initTransactions("t1", 30_000, 100L, 0L);

        var reinit = coord.initTransactions("t1", 30_000, 999L, 1L);
        assertThat(reinit.errorCode()).isEqualTo(ErrorCodes.NONE);
        assertThat(reinit.producerId()).isEqualTo(100L); // pid is stable; candidate ignored
        assertThat(reinit.producerEpoch()).isEqualTo(1);

        assertThat(coord.addPartitions("t1", 100L, 0, List.of(ORDERS_0), 2L).errorCode())
                .isEqualTo(ErrorCodes.PRODUCER_FENCED);
        assertThat(coord.endTxn("t1", 100L, 0, true, 2L).errorCode()).isEqualTo(ErrorCodes.PRODUCER_FENCED);
        assertThat(coord.markersDelivered("t1", 100L, 0, 2L).errorCode()).isEqualTo(ErrorCodes.PRODUCER_FENCED);
    }

    @Test
    void wrongProducerIdAndFutureEpochAreFencedToo() {
        var coord = newCoordinator();
        coord.initTransactions("t1", 30_000, 100L, 0L);

        assertThat(coord.addPartitions("t1", 777L, 0, List.of(ORDERS_0), 1L).errorCode())
                .isEqualTo(ErrorCodes.PRODUCER_FENCED);
        assertThat(coord.addPartitions("t1", 100L, 3, List.of(ORDERS_0), 1L).errorCode())
                .isEqualTo(ErrorCodes.PRODUCER_FENCED);
        assertThat(coord.endTxn("t1", 100L, 3, true, 1L).errorCode()).isEqualTo(ErrorCodes.PRODUCER_FENCED);
        assertThat(coord.markersDelivered("t1", 100L, 3, 1L).errorCode()).isEqualTo(ErrorCodes.PRODUCER_FENCED);
    }

    @Test
    void initWithOngoingTransactionAbortsItAndFencesTheZombie() {
        var coord = newCoordinator();
        coord.initTransactions("t1", 30_000, 100L, 0L);
        coord.addPartitions("t1", 100L, 0, List.of(ORDERS_0, PAYMENTS_2), 1L);

        var reinit = coord.initTransactions("t1", 30_000, 999L, 2L);
        assertThat(reinit.errorCode()).isEqualTo(ErrorCodes.NONE);
        assertThat(reinit.producerEpoch()).isEqualTo(1);
        var rec = reinit.stateRecord().orElseThrow();
        assertThat(rec.state()).isEqualTo(TxnState.PREPARE_ABORT);
        assertThat(rec.producerEpoch()).isEqualTo(1);
        assertThat(reinit.markers()).hasSize(2);
        for (var m : reinit.markers()) {
            assertThat(m.commit()).isFalse();
            assertThat(m.producerEpoch()).isEqualTo(1); // bumped epoch fences at the partitions too
        }

        // The new producer must wait out the abort completion...
        assertThat(coord.addPartitions("t1", 100L, 1, List.of(ORDERS_0), 3L).errorCode())
                .isEqualTo(ErrorCodes.CONCURRENT_TRANSACTIONS);
        // ...and the zombie is fenced outright.
        assertThat(coord.endTxn("t1", 100L, 0, true, 3L).errorCode()).isEqualTo(ErrorCodes.PRODUCER_FENCED);

        // Once delivery is confirmed the new producer proceeds normally.
        assertThat(coord.markersDelivered("t1", 100L, 1, 4L)
                        .stateRecord()
                        .orElseThrow()
                        .state())
                .isEqualTo(TxnState.COMPLETE_ABORT);
        assertThat(coord.addPartitions("t1", 100L, 1, List.of(ORDERS_1), 5L).errorCode())
                .isEqualTo(ErrorCodes.NONE);
    }

    @Test
    void initWhileCompletionInFlightIsRetriable() {
        var coord = newCoordinator();
        coord.initTransactions("t1", 30_000, 100L, 0L);
        coord.addPartitions("t1", 100L, 0, List.of(ORDERS_0), 1L);
        coord.endTxn("t1", 100L, 0, true, 2L);

        var reinit = coord.initTransactions("t1", 30_000, 999L, 3L);
        assertThat(reinit.errorCode()).isEqualTo(ErrorCodes.CONCURRENT_TRANSACTIONS);
        assertThat(reinit.stateRecord()).isEmpty();
        // The prepared commit is untouched by the failed init.
        assertThat(coord.stateOf("t1").orElseThrow().state()).isEqualTo(TxnState.PREPARE_COMMIT);

        coord.markersDelivered("t1", 100L, 0, 4L);
        var retry = coord.initTransactions("t1", 30_000, 999L, 5L);
        assertThat(retry.errorCode()).isEqualTo(ErrorCodes.NONE);
        assertThat(retry.producerEpoch()).isEqualTo(1);
    }

    // ---------- EndTxn idempotency ----------

    @Test
    void retriedEndTxnInPrepareReturnsTheSameMarkersWithoutANewRecord() {
        var coord = newCoordinator();
        coord.initTransactions("t1", 30_000, 100L, 0L);
        coord.addPartitions("t1", 100L, 0, List.of(ORDERS_0, PAYMENTS_2), 1L);
        var first = coord.endTxn("t1", 100L, 0, true, 2L);

        var retry = coord.endTxn("t1", 100L, 0, true, 3L);
        assertThat(retry.errorCode()).isEqualTo(ErrorCodes.NONE);
        assertThat(retry.stateRecord()).isEmpty(); // decision already logged
        assertThat(retry.markers()).isEqualTo(first.markers());

        // The opposite outcome can never sneak in after the decision.
        assertThat(coord.endTxn("t1", 100L, 0, false, 4L).errorCode()).isEqualTo(ErrorCodes.INVALID_TXN_STATE);
    }

    @Test
    void retriedEndTxnInCompleteIsABareSuccess() {
        var coord = newCoordinator();
        coord.initTransactions("t1", 30_000, 100L, 0L);
        coord.addPartitions("t1", 100L, 0, List.of(ORDERS_0), 1L);
        coord.endTxn("t1", 100L, 0, false, 2L);
        coord.markersDelivered("t1", 100L, 0, 3L);

        var retry = coord.endTxn("t1", 100L, 0, false, 4L);
        assertThat(retry.errorCode()).isEqualTo(ErrorCodes.NONE);
        assertThat(retry.stateRecord()).isEmpty();
        assertThat(retry.markers()).isEmpty(); // delivery already confirmed — nothing to redo

        assertThat(coord.endTxn("t1", 100L, 0, true, 5L).errorCode()).isEqualTo(ErrorCodes.INVALID_TXN_STATE);
    }

    @Test
    void duplicateMarkersDeliveredIsIdempotent() {
        var coord = newCoordinator();
        coord.initTransactions("t1", 30_000, 100L, 0L);
        coord.addPartitions("t1", 100L, 0, List.of(ORDERS_0), 1L);
        coord.endTxn("t1", 100L, 0, true, 2L);
        coord.markersDelivered("t1", 100L, 0, 3L);

        var dup = coord.markersDelivered("t1", 100L, 0, 4L);
        assertThat(dup.errorCode()).isEqualTo(ErrorCodes.NONE);
        assertThat(dup.stateRecord()).isEmpty();
    }

    // ---------- Protocol violations ----------

    @Test
    void invalidInputsAnswerInvalidTxnState() {
        var coord = newCoordinator();
        // Unknown transactional_id — init was skipped.
        assertThat(coord.addPartitions("nope", 1L, 0, List.of(ORDERS_0), 0L).errorCode())
                .isEqualTo(ErrorCodes.INVALID_TXN_STATE);
        assertThat(coord.endTxn("nope", 1L, 0, true, 0L).errorCode()).isEqualTo(ErrorCodes.INVALID_TXN_STATE);
        assertThat(coord.markersDelivered("nope", 1L, 0, 0L).errorCode()).isEqualTo(ErrorCodes.INVALID_TXN_STATE);

        coord.initTransactions("t1", 30_000, 100L, 0L);
        // Nothing to end, nothing can have been delivered, nothing to add.
        assertThat(coord.endTxn("t1", 100L, 0, true, 1L).errorCode()).isEqualTo(ErrorCodes.INVALID_TXN_STATE);
        assertThat(coord.markersDelivered("t1", 100L, 0, 1L).errorCode()).isEqualTo(ErrorCodes.INVALID_TXN_STATE);
        assertThat(coord.addPartitions("t1", 100L, 0, List.of(), 1L).errorCode())
                .isEqualTo(ErrorCodes.INVALID_TXN_STATE);
    }

    // ---------- Timeout sweep ----------

    @Test
    void tickAbortsExpiredOngoingAndLeavesLiveOnesAlone() {
        var coord = newCoordinator();
        coord.initTransactions("expired", 1_000, 100L, 0L);
        coord.addPartitions("expired", 100L, 0, List.of(ORDERS_0), 100L);
        coord.initTransactions("live", 60_000, 200L, 0L);
        coord.addPartitions("live", 200L, 0, List.of(PAYMENTS_2), 900L);

        // Inside the budget (exactly at it) — nothing fires.
        assertThat(coord.tick(1_100L)).isEmpty();

        var aborts = coord.tick(1_101L);
        assertThat(aborts).hasSize(1);
        var abort = aborts.get(0);
        assertThat(abort.transactionalId()).isEqualTo("expired");
        assertThat(abort.stateRecord().state()).isEqualTo(TxnState.PREPARE_ABORT);
        assertThat(abort.stateRecord().producerEpoch()).isEqualTo(1); // bumped — silent producer is fenced
        assertThat(abort.markers()).singleElement().satisfies(m -> {
            assertThat(m.tp()).isEqualTo(ORDERS_0);
            assertThat(m.commit()).isFalse();
            assertThat(m.producerEpoch()).isEqualTo(1);
        });
        assertThat(coord.stateOf("live").orElseThrow().state()).isEqualTo(TxnState.ONGOING);

        // The timed-out producer's next call is fenced.
        assertThat(coord.endTxn("expired", 100L, 0, true, 1_200L).errorCode()).isEqualTo(ErrorCodes.PRODUCER_FENCED);
        // Completion closes it out like any other abort.
        assertThat(coord.markersDelivered("expired", 100L, 1, 1_300L)
                        .stateRecord()
                        .orElseThrow()
                        .state())
                .isEqualTo(TxnState.COMPLETE_ABORT);
        // A second sweep finds nothing — Prepare*/Complete* never expire.
        assertThat(coord.tick(10_000L)).isEmpty();
    }

    @Test
    void initWithNonPositiveTimeoutUsesTheDefault() {
        var coord = newCoordinator();
        var init = coord.initTransactions("t1", 0, 100L, 0L);
        assertThat(init.stateRecord().orElseThrow().timeoutMs())
                .isEqualTo(TxnCoordinator.DEFAULT_TRANSACTION_TIMEOUT_MS);
    }

    // ---------- Replay ----------

    @Test
    void replayingAppendedRecordsReconstructsEveryStateExactly() {
        var live = newCoordinator();
        var appended = new java.util.ArrayList<TxnStateRecord>();

        appended.add(live.initTransactions("t1", 30_000, 100L, 0L).stateRecord().orElseThrow());
        assertReplayMatches(live, appended, "t1"); // EMPTY

        appended.add(live.addPartitions("t1", 100L, 0, List.of(ORDERS_0, PAYMENTS_2), 1L)
                .stateRecord()
                .orElseThrow());
        assertReplayMatches(live, appended, "t1"); // ONGOING

        var end = live.endTxn("t1", 100L, 0, true, 2L);
        appended.add(end.stateRecord().orElseThrow());
        assertReplayMatches(live, appended, "t1"); // PREPARE_COMMIT

        // The replayed coordinator can regenerate the exact marker set the
        // live one handed out — coordinator failover mid-two-phase re-derives
        // its delivery obligations from the log alone.
        var replayed = replay(appended);
        assertThat(replayed.pendingMarkers()).isEqualTo(end.markers());
        // And a retried EndTxn against the replayed coordinator behaves
        // identically to one against the live coordinator.
        var retry = replayed.endTxn("t1", 100L, 0, true, 3L);
        assertThat(retry.errorCode()).isEqualTo(ErrorCodes.NONE);
        assertThat(retry.markers()).isEqualTo(end.markers());

        appended.add(live.markersDelivered("t1", 100L, 0, 4L).stateRecord().orElseThrow());
        assertReplayMatches(live, appended, "t1"); // COMPLETE_COMMIT
        assertThat(replay(appended).pendingMarkers()).isEmpty();
    }

    @Test
    void replayReconstructsAbortStatesExactly() {
        var live = newCoordinator();
        var appended = new java.util.ArrayList<TxnStateRecord>();
        appended.add(live.initTransactions("t1", 30_000, 100L, 0L).stateRecord().orElseThrow());
        appended.add(live.addPartitions("t1", 100L, 0, List.of(ORDERS_0), 1L)
                .stateRecord()
                .orElseThrow());
        appended.add(live.endTxn("t1", 100L, 0, false, 2L).stateRecord().orElseThrow());
        assertReplayMatches(live, appended, "t1"); // PREPARE_ABORT
        appended.add(live.markersDelivered("t1", 100L, 0, 3L).stateRecord().orElseThrow());
        assertReplayMatches(live, appended, "t1"); // COMPLETE_ABORT
    }

    private static TxnCoordinator replay(List<TxnStateRecord> records) {
        var coord = newCoordinator();
        for (var rec : records) coord.restore(rec);
        return coord;
    }

    private static void assertReplayMatches(TxnCoordinator live, List<TxnStateRecord> appended, String txnId) {
        assertThat(replay(appended).stateOf(txnId)).isEqualTo(live.stateOf(txnId));
    }
}
