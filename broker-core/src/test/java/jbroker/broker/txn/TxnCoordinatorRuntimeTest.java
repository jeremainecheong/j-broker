package jbroker.broker.txn;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import jbroker.broker.ErrorCodes;
import jbroker.broker.TopicManager;
import jbroker.broker.replication.FollowerStateTracker;
import jbroker.proto.common.TopicPartition;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The coordinator runtime's contract, on a single-partition
 * {@code __transaction_state} (every transactional_id routes to partition
 * 0): append-before-answer, background marker delivery feeding
 * markersDelivered, replay-on-activation resuming a predecessor's pending
 * markers, and the timeout sweep.
 */
class TxnCoordinatorRuntimeTest {

    private static final int SELF = 1;

    /** Transport double: delivery succeeds only while {@code open} is true; every success is recorded. */
    private static final class FakeTransport implements TxnCoordinatorRuntime.MarkerTransport {
        volatile boolean open = true;
        final ConcurrentLinkedQueue<TxnCoordinator.MarkerInstruction> delivered = new ConcurrentLinkedQueue<>();

        @Override
        public boolean deliver(TxnCoordinator.MarkerInstruction instruction) {
            if (!open) return false;
            delivered.add(instruction);
            return true;
        }
    }

    @Test
    void commitFlowAppendsBeforeAnsweringAndCompletesThroughDelivery(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var tm = txnStateTm();
            var transport = new FakeTransport();
            var clock = new AtomicLong(1_000L);
            try (var runtime = runtime(lm, tm, transport, clock)) {
                var init = runtime.initTransactions(0, "t1", 0);
                assertThat(init.errorCode()).isEqualTo(ErrorCodes.NONE);
                assertThat(init.producerId()).isEqualTo(1L);
                assertThat(init.producerEpoch()).isZero();
                assertThat(replayedState(lm, "t1"))
                        .as("the init record must be in the log before the response")
                        .hasValueSatisfying(rec -> assertThat(rec.state()).isEqualTo(TxnState.EMPTY));

                int add = runtime.addPartitions(0, "t1", 1L, 0, List.of(tp("orders", 0), tp("orders", 1)));
                assertThat(add).isEqualTo(ErrorCodes.NONE);
                assertThat(replayedState(lm, "t1"))
                        .hasValueSatisfying(rec -> assertThat(rec.state()).isEqualTo(TxnState.ONGOING));

                int end = runtime.endTxn(0, "t1", 1L, 0, /*commit*/ true);
                assertThat(end).isEqualTo(ErrorCodes.NONE);

                awaitState(runtime, "t1", TxnState.COMPLETE_COMMIT);
                assertThat(transport.delivered).hasSize(2);
                assertThat(transport.delivered).allSatisfy(m -> {
                    assertThat(m.commit()).isTrue();
                    assertThat(m.producerId()).isEqualTo(1L);
                });
                // The in-memory Complete transition precedes its record
                // append, so the log assertion polls its own deadline.
                awaitReplayedState(lm, "t1", TxnState.COMPLETE_COMMIT);
            }
        }
    }

    @Test
    void endTxnAnswersWithPrepareDurableEvenIfDeliveryNeverSucceeds(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var tm = txnStateTm();
            var transport = new FakeTransport();
            transport.open = false; // markers cannot land
            try (var runtime = runtime(lm, tm, transport, new AtomicLong(1_000L))) {
                runtime.initTransactions(0, "t1", 0);
                runtime.addPartitions(0, "t1", 1L, 0, List.of(tp("orders", 0)));
                int end = runtime.endTxn(0, "t1", 1L, 0, true);
                assertThat(end).isEqualTo(ErrorCodes.NONE);
                assertThat(replayedState(lm, "t1"))
                        .as("PrepareCommit is durable before the client hears success")
                        .hasValueSatisfying(rec -> assertThat(rec.state()).isEqualTo(TxnState.PREPARE_COMMIT));
            }
        }
    }

    @Test
    void activationReplayResumesAPredecessorsPendingMarkers(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var tm = txnStateTm();
            // Predecessor: decides the txn but dies before any marker lands.
            var deadTransport = new FakeTransport();
            deadTransport.open = false;
            try (var predecessor = runtime(lm, tm, deadTransport, new AtomicLong(1_000L))) {
                predecessor.initTransactions(0, "t1", 0);
                predecessor.addPartitions(0, "t1", 1L, 0, List.of(tp("orders", 0), tp("orders", 1)));
                assertThat(predecessor.endTxn(0, "t1", 1L, 0, true)).isEqualTo(ErrorCodes.NONE);
            }

            // Successor over the same log: tick() activates, replays the
            // Prepare record, and resumes delivery.
            var transport = new FakeTransport();
            try (var successor = runtime(lm, tm, transport, new AtomicLong(2_000L))) {
                successor.tick();
                awaitState(successor, "t1", TxnState.COMPLETE_COMMIT);
                assertThat(transport.delivered).hasSize(2);
            }
        }
    }

    @Test
    void timeoutSweepAbortsIdleTransactionAndDeliversAbortMarkers(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var tm = txnStateTm();
            var transport = new FakeTransport();
            var clock = new AtomicLong(1_000L);
            try (var runtime = runtime(lm, tm, transport, clock)) {
                runtime.initTransactions(0, "t1", /*timeoutMs*/ 50);
                runtime.addPartitions(0, "t1", 1L, 0, List.of(tp("orders", 0)));
                clock.addAndGet(60L);
                runtime.tick();
                awaitState(runtime, "t1", TxnState.COMPLETE_ABORT);
                assertThat(transport.delivered).hasSize(1);
                var marker = transport.delivered.peek();
                assertThat(marker.commit()).isFalse();
                assertThat(marker.producerEpoch())
                        .as("the sweep bumps the epoch so the silent producer is fenced")
                        .isEqualTo(1);
            }
        }
    }

    @Test
    void nonLeaderPartitionIsNotCoordinated(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var tm = new TopicManager();
            tm.onTopicCommitted(TxnStateTopic.NAME, 1, 1, 0L);
            tm.onPartitionChange(TxnStateTopic.NAME, 0, /*leader*/ 2, List.of(2), 0);
            try (var runtime = runtime(lm, tm, new FakeTransport(), new AtomicLong(1_000L))) {
                var init = runtime.initTransactions(0, "t1", 0);
                assertThat(init.errorCode()).isEqualTo(ErrorCodes.COORDINATOR_NOT_AVAILABLE);
                assertThat(lm.logFor(TxnStateTopic.NAME, 0).nextOffset()).isZero();
            }
        }
    }

    // --- helpers ---

    private static void awaitState(TxnCoordinatorRuntime runtime, String txnId, TxnState expected) throws Exception {
        long deadline = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < deadline) {
            var state = runtime.stateOf(0, txnId);
            if (state.isPresent() && state.get().state() == expected) return;
            Thread.sleep(10);
        }
        throw new AssertionError("txn '" + txnId + "' did not reach " + expected + " within 5s; last state = "
                + runtime.stateOf(0, txnId));
    }

    private static void awaitReplayedState(LogManager lm, String txnId, TxnState expected) throws Exception {
        long deadline = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < deadline) {
            var state = replayedState(lm, txnId);
            if (state.isPresent() && state.get().state() == expected) return;
            Thread.sleep(10);
        }
        throw new AssertionError(
                "log never showed '" + txnId + "' at " + expected + "; last = " + replayedState(lm, txnId));
    }

    /** Latest state record for {@code txnId} as an independent replay of the partition log sees it. */
    private static Optional<TxnStateRecord> replayedState(LogManager lm, String txnId) throws Exception {
        var core = new TxnCoordinator(0);
        TxnStateRecovery.rebuild(lm, 0, core);
        return core.stateOf(txnId);
    }

    private static TopicManager txnStateTm() {
        var tm = new TopicManager();
        tm.onTopicCommitted(TxnStateTopic.NAME, 1, 1, 0L);
        tm.onPartitionChange(TxnStateTopic.NAME, 0, SELF, List.of(SELF), 0);
        return tm;
    }

    private static TxnCoordinatorRuntime runtime(
            LogManager lm, TopicManager tm, TxnCoordinatorRuntime.MarkerTransport transport, AtomicLong clock) {
        var pidCounter = new AtomicLong();
        return new TxnCoordinatorRuntime(
                lm,
                tm,
                new FollowerStateTracker(),
                SELF,
                /*clusterMinIsr*/ 2,
                pidCounter::incrementAndGet,
                transport,
                clock::get);
    }

    private static TopicPartition tp(String topic, int partition) {
        return TopicPartition.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .build();
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
