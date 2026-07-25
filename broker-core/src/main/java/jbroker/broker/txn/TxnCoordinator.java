package jbroker.broker.txn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import jbroker.broker.ErrorCodes;
import jbroker.proto.common.TopicPartition;

/**
 * Pure transaction-coordinator core for the transactional_ids whose
 * {@code __transaction_state} partitions this broker leads. Never performs
 * I/O: every input returns the {@link TxnStateRecord} the caller must
 * append to the coordinator partition and/or the {@link MarkerInstruction}s
 * the caller must deliver — the same returns-effects philosophy as
 * {@code jbroker.raft.core.RaftCore#step}. See the package javadoc for the
 * two-phase contract.
 *
 * <p>Caller obligations, in order:
 * <ol>
 *   <li>Append the returned state record (acks=all) before answering the
 *       client — the in-memory transition already happened, so a lost
 *       append plus a crash would forget it.</li>
 *   <li>Deliver returned markers (retrying until every partition acks),
 *       then feed {@link #markersDelivered} to close the transaction.</li>
 * </ol>
 *
 * <p>On gaining leadership of a coordinator partition, build a fresh core,
 * replay the partition through {@link #restore} (see
 * {@link TxnStateRecovery}), then drain {@link #pendingMarkers} — Prepare*
 * records whose delivery the previous coordinator may not have finished.
 *
 * <p>All methods are safe for concurrent use (single internal lock; the
 * per-input work is a few map operations).
 */
public final class TxnCoordinator {

    /** Applied when InitTransactions passes {@code timeoutMs <= 0}. */
    public static final int DEFAULT_TRANSACTION_TIMEOUT_MS = 60_000;

    /**
     * Producer epoch ceiling — the wire carries the epoch as an int32 but
     * the v2 batch header stores an int16, so [0, 65535] is the usable
     * range. Reaching the ceiling rolls the transactional_id onto a fresh
     * producer id at epoch 0.
     */
    public static final int MAX_PRODUCER_EPOCH = 65_535;

    /**
     * One marker the wiring must deliver to {@code tp}'s leader (the
     * {@code WriteTxnMarkers} inter-broker RPC). {@code coordinatorEpoch}
     * lets the receiving leader fence a deposed coordinator still flushing
     * its queue; {@code transactionalId} is bookkeeping so the wiring can
     * feed {@link #markersDelivered} once every partition acks.
     */
    public record MarkerInstruction(
            String transactionalId,
            TopicPartition tp,
            long producerId,
            int producerEpoch,
            boolean commit,
            int coordinatorEpoch) {}

    /**
     * Outcome of {@link #initTransactions}. On success ({@code errorCode ==
     * NONE}) the producer uses ({@code producerId}, {@code producerEpoch})
     * for everything that follows; {@code markers} is non-empty when the
     * epoch bump also aborted an in-flight transaction from the previous
     * epoch. On error the id/epoch are -1.
     */
    public record InitResult(
            int errorCode,
            long producerId,
            int producerEpoch,
            Optional<TxnStateRecord> stateRecord,
            List<MarkerInstruction> markers) {
        public InitResult {
            markers = List.copyOf(markers);
        }
    }

    public record AddPartitionsResult(int errorCode, Optional<TxnStateRecord> stateRecord) {}

    public record EndTxnResult(int errorCode, Optional<TxnStateRecord> stateRecord, List<MarkerInstruction> markers) {
        public EndTxnResult {
            markers = List.copyOf(markers);
        }
    }

    public record MarkersDeliveredResult(int errorCode, Optional<TxnStateRecord> stateRecord) {}

    /** One expired transaction the sweep aborted: append the record, deliver the markers. */
    public record TimeoutAbort(String transactionalId, TxnStateRecord stateRecord, List<MarkerInstruction> markers) {
        public TimeoutAbort {
            markers = List.copyOf(markers);
        }
    }

    private static final class TxnEntry {
        long producerId;
        int producerEpoch;
        TxnState state = TxnState.EMPTY;
        final LinkedHashSet<TopicPartition> partitions = new LinkedHashSet<>();
        int timeoutMs = DEFAULT_TRANSACTION_TIMEOUT_MS;
        long lastUpdateMs;
    }

    private final int coordinatorEpoch;
    private final Map<String, TxnEntry> txns = new HashMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * @param coordinatorEpoch the leader epoch of this broker's tenure over
     *     the coordinator partition, stamped into every marker instruction.
     *     A new leadership builds a new core with a higher epoch.
     */
    public TxnCoordinator(int coordinatorEpoch) {
        this.coordinatorEpoch = coordinatorEpoch;
    }

    /**
     * Register (or re-register) a transactional producer.
     *
     * <ul>
     *   <li>Unknown transactional_id → adopts {@code producerIdIfNew} at
     *       epoch 0. The candidate id is supplied by the caller (allocated
     *       through the controller's producer-id counter) so the core
     *       stays free of allocation I/O; it is ignored whenever the id
     *       already holds a producer id.</li>
     *   <li>EMPTY / COMPLETE_* → bumps the epoch, fencing any zombie
     *       holding the previous one. At the epoch ceiling the id rolls to
     *       {@code producerIdIfNew} at epoch 0.</li>
     *   <li>ONGOING → bumps the epoch and aborts the in-flight transaction
     *       (record + abort markers returned alongside the success).</li>
     *   <li>PREPARE_* → CONCURRENT_TRANSACTIONS: the previous
     *       transaction's completion is still in flight; retry shortly.</li>
     * </ul>
     */
    public InitResult initTransactions(String txnId, int timeoutMs, long producerIdIfNew, long nowMs) {
        int effectiveTimeout = timeoutMs > 0 ? timeoutMs : DEFAULT_TRANSACTION_TIMEOUT_MS;
        lock.lock();
        try {
            var entry = txns.get(txnId);
            if (entry == null) {
                entry = new TxnEntry();
                entry.producerId = producerIdIfNew;
                entry.producerEpoch = 0;
                entry.timeoutMs = effectiveTimeout;
                entry.lastUpdateMs = nowMs;
                txns.put(txnId, entry);
                return new InitResult(
                        ErrorCodes.NONE,
                        entry.producerId,
                        entry.producerEpoch,
                        Optional.of(recordOf(txnId, entry)),
                        List.of());
            }
            switch (entry.state) {
                case PREPARE_COMMIT, PREPARE_ABORT -> {
                    return new InitResult(ErrorCodes.CONCURRENT_TRANSACTIONS, -1L, -1, Optional.empty(), List.of());
                }
                case EMPTY, COMPLETE_COMMIT, COMPLETE_ABORT -> {
                    if (entry.producerEpoch >= MAX_PRODUCER_EPOCH) {
                        entry.producerId = producerIdIfNew;
                        entry.producerEpoch = 0;
                    } else {
                        entry.producerEpoch++;
                    }
                    entry.state = TxnState.EMPTY;
                    entry.partitions.clear();
                    entry.timeoutMs = effectiveTimeout;
                    entry.lastUpdateMs = nowMs;
                    return new InitResult(
                            ErrorCodes.NONE,
                            entry.producerId,
                            entry.producerEpoch,
                            Optional.of(recordOf(txnId, entry)),
                            List.of());
                }
                case ONGOING -> {
                    if (entry.producerEpoch >= MAX_PRODUCER_EPOCH) {
                        // Cannot bump within this producer id. Abort at the
                        // final epoch and make the client retry; the retry
                        // finds COMPLETE_ABORT and rolls to a fresh id.
                        entry.state = TxnState.PREPARE_ABORT;
                        entry.timeoutMs = effectiveTimeout;
                        entry.lastUpdateMs = nowMs;
                        // In-memory state moved, so the abort must still be
                        // persisted and delivered even though the client
                        // sees a retriable error.
                        return new InitResult(
                                ErrorCodes.CONCURRENT_TRANSACTIONS,
                                -1L,
                                -1,
                                Optional.of(recordOf(txnId, entry)),
                                markersOf(txnId, entry));
                    }
                    entry.producerEpoch++;
                    entry.state = TxnState.PREPARE_ABORT;
                    entry.timeoutMs = effectiveTimeout;
                    entry.lastUpdateMs = nowMs;
                    // Markers carry the bumped epoch: data-partition leaders
                    // treat it as >= the data's epoch and resolve the open
                    // transaction; the zombie holding the old epoch is fenced
                    // on its next coordinator call.
                    return new InitResult(
                            ErrorCodes.NONE,
                            entry.producerId,
                            entry.producerEpoch,
                            Optional.of(recordOf(txnId, entry)),
                            markersOf(txnId, entry));
                }
            }
            throw new AssertionError("unreachable");
        } finally {
            lock.unlock();
        }
    }

    /**
     * Declare partitions for the current transaction. First accepted call
     * opens it (EMPTY / COMPLETE_* → ONGOING); later calls extend the set.
     * A retry that adds nothing new is an idempotent no-op success with no
     * record to append.
     */
    public AddPartitionsResult addPartitions(
            String txnId, long producerId, int producerEpoch, List<TopicPartition> partitions, long nowMs) {
        if (partitions.isEmpty()) {
            return new AddPartitionsResult(ErrorCodes.INVALID_TXN_STATE, Optional.empty());
        }
        lock.lock();
        try {
            var entry = txns.get(txnId);
            if (entry == null) {
                // Client skipped InitTransactions (or the id was created on
                // a different coordinator epoch and never replayed here).
                return new AddPartitionsResult(ErrorCodes.INVALID_TXN_STATE, Optional.empty());
            }
            if (fenced(entry, producerId, producerEpoch)) {
                return new AddPartitionsResult(ErrorCodes.PRODUCER_FENCED, Optional.empty());
            }
            switch (entry.state) {
                case PREPARE_COMMIT, PREPARE_ABORT -> {
                    return new AddPartitionsResult(ErrorCodes.CONCURRENT_TRANSACTIONS, Optional.empty());
                }
                case EMPTY, COMPLETE_COMMIT, COMPLETE_ABORT -> {
                    // Fresh transaction: the previous set (if any) belongs to
                    // an already-completed transaction.
                    entry.partitions.clear();
                    entry.partitions.addAll(partitions);
                    entry.state = TxnState.ONGOING;
                    entry.lastUpdateMs = nowMs;
                    return new AddPartitionsResult(ErrorCodes.NONE, Optional.of(recordOf(txnId, entry)));
                }
                case ONGOING -> {
                    boolean changed = entry.partitions.addAll(partitions);
                    if (!changed) {
                        // Pure retry — nothing to persist, and the clock is
                        // deliberately NOT refreshed so in-memory state stays
                        // byte-identical to what a replay would rebuild.
                        return new AddPartitionsResult(ErrorCodes.NONE, Optional.empty());
                    }
                    entry.lastUpdateMs = nowMs;
                    return new AddPartitionsResult(ErrorCodes.NONE, Optional.of(recordOf(txnId, entry)));
                }
            }
            throw new AssertionError("unreachable");
        } finally {
            lock.unlock();
        }
    }

    /**
     * Decide the current transaction. ONGOING moves to the matching
     * Prepare* state and yields the marker instructions. Retries are
     * idempotent as long as the requested outcome matches the decided one:
     * in Prepare* the same instructions are returned again (so a caller
     * that crashed mid-delivery can resume), in Complete* the call is a
     * bare success. A retry with the <em>opposite</em> outcome is
     * INVALID_TXN_STATE — a decision is never reversed.
     */
    public EndTxnResult endTxn(String txnId, long producerId, int producerEpoch, boolean commit, long nowMs) {
        lock.lock();
        try {
            var entry = txns.get(txnId);
            if (entry == null) {
                return new EndTxnResult(ErrorCodes.INVALID_TXN_STATE, Optional.empty(), List.of());
            }
            if (fenced(entry, producerId, producerEpoch)) {
                return new EndTxnResult(ErrorCodes.PRODUCER_FENCED, Optional.empty(), List.of());
            }
            switch (entry.state) {
                case ONGOING -> {
                    entry.state = commit ? TxnState.PREPARE_COMMIT : TxnState.PREPARE_ABORT;
                    entry.lastUpdateMs = nowMs;
                    return new EndTxnResult(
                            ErrorCodes.NONE, Optional.of(recordOf(txnId, entry)), markersOf(txnId, entry));
                }
                case PREPARE_COMMIT, PREPARE_ABORT -> {
                    boolean decidedCommit = entry.state == TxnState.PREPARE_COMMIT;
                    if (commit != decidedCommit) {
                        return new EndTxnResult(ErrorCodes.INVALID_TXN_STATE, Optional.empty(), List.of());
                    }
                    return new EndTxnResult(ErrorCodes.NONE, Optional.empty(), markersOf(txnId, entry));
                }
                case COMPLETE_COMMIT, COMPLETE_ABORT -> {
                    boolean completedCommit = entry.state == TxnState.COMPLETE_COMMIT;
                    if (commit != completedCommit) {
                        return new EndTxnResult(ErrorCodes.INVALID_TXN_STATE, Optional.empty(), List.of());
                    }
                    return new EndTxnResult(ErrorCodes.NONE, Optional.empty(), List.of());
                }
                case EMPTY -> {
                    return new EndTxnResult(ErrorCodes.INVALID_TXN_STATE, Optional.empty(), List.of());
                }
            }
            throw new AssertionError("unreachable");
        } finally {
            lock.unlock();
        }
    }

    /**
     * The wiring confirms every marker for the transaction reached its
     * partition leader. Prepare* → the matching Complete*; already
     * Complete* is an idempotent success (duplicate confirmation).
     */
    public MarkersDeliveredResult markersDelivered(String txnId, long producerId, int producerEpoch, long nowMs) {
        lock.lock();
        try {
            var entry = txns.get(txnId);
            if (entry == null) {
                return new MarkersDeliveredResult(ErrorCodes.INVALID_TXN_STATE, Optional.empty());
            }
            if (fenced(entry, producerId, producerEpoch)) {
                return new MarkersDeliveredResult(ErrorCodes.PRODUCER_FENCED, Optional.empty());
            }
            switch (entry.state) {
                case PREPARE_COMMIT, PREPARE_ABORT -> {
                    entry.state =
                            entry.state == TxnState.PREPARE_COMMIT ? TxnState.COMPLETE_COMMIT : TxnState.COMPLETE_ABORT;
                    entry.lastUpdateMs = nowMs;
                    return new MarkersDeliveredResult(ErrorCodes.NONE, Optional.of(recordOf(txnId, entry)));
                }
                case COMPLETE_COMMIT, COMPLETE_ABORT -> {
                    return new MarkersDeliveredResult(ErrorCodes.NONE, Optional.empty());
                }
                case EMPTY, ONGOING -> {
                    // No decision was ever prepared — nothing can have been
                    // delivered.
                    return new MarkersDeliveredResult(ErrorCodes.INVALID_TXN_STATE, Optional.empty());
                }
            }
            throw new AssertionError("unreachable");
        } finally {
            lock.unlock();
        }
    }

    /**
     * Timeout sweep. Every ONGOING transaction idle past its timeout is
     * aborted: epoch bumped (fencing the silent producer), PREPARE_ABORT
     * logged, abort markers returned for delivery. Live transactions are
     * untouched; Prepare* transactions are never expired — their outcome
     * is already decided and only delivery remains.
     */
    public List<TimeoutAbort> tick(long nowMs) {
        var aborted = new ArrayList<TimeoutAbort>();
        lock.lock();
        try {
            for (var e : txns.entrySet()) {
                var entry = e.getValue();
                if (entry.state != TxnState.ONGOING) continue;
                if (nowMs - entry.lastUpdateMs <= entry.timeoutMs) continue;
                if (entry.producerEpoch < MAX_PRODUCER_EPOCH) {
                    entry.producerEpoch++;
                }
                entry.state = TxnState.PREPARE_ABORT;
                entry.lastUpdateMs = nowMs;
                aborted.add(new TimeoutAbort(e.getKey(), recordOf(e.getKey(), entry), markersOf(e.getKey(), entry)));
            }
        } finally {
            lock.unlock();
        }
        return List.copyOf(aborted);
    }

    /**
     * Replay one append-confirmed (or recovered) record into the cache,
     * overwriting any in-memory state for the key — the log is the source
     * of truth. Called in offset order by {@link TxnStateRecovery};
     * latest-record-per-key-wins falls out naturally.
     */
    public void restore(TxnStateRecord rec) {
        lock.lock();
        try {
            var entry = txns.computeIfAbsent(rec.transactionalId(), id -> new TxnEntry());
            entry.producerId = rec.producerId();
            entry.producerEpoch = rec.producerEpoch();
            entry.state = rec.state();
            entry.partitions.clear();
            entry.partitions.addAll(rec.partitions());
            entry.timeoutMs = rec.timeoutMs();
            entry.lastUpdateMs = rec.lastUpdateMs();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Marker instructions for every transaction sitting in Prepare* —
     * decided outcomes whose delivery is not yet confirmed. Drained by the
     * wiring right after replay-on-leadership: the previous coordinator
     * may have died anywhere between logging Prepare* and feeding
     * markersDelivered, and redelivery is safe because markers are
     * idempotent at the partitions.
     */
    public List<MarkerInstruction> pendingMarkers() {
        var out = new ArrayList<MarkerInstruction>();
        lock.lock();
        try {
            for (var e : txns.entrySet()) {
                if (e.getValue().state.isPrepare()) {
                    out.addAll(markersOf(e.getKey(), e.getValue()));
                }
            }
        } finally {
            lock.unlock();
        }
        return List.copyOf(out);
    }

    /** Snapshot of one transactional_id's state, or empty if unknown. Test/wiring accessor. */
    public Optional<TxnStateRecord> stateOf(String txnId) {
        lock.lock();
        try {
            var entry = txns.get(txnId);
            return entry == null ? Optional.empty() : Optional.of(recordOf(txnId, entry));
        } finally {
            lock.unlock();
        }
    }

    private static boolean fenced(TxnEntry entry, long producerId, int producerEpoch) {
        // Wrong id (a stale grant from before a pid roll) and wrong epoch in
        // EITHER direction both fence: a future epoch means the caller's
        // grant came from state this coordinator never saw, so it cannot be
        // trusted any more than a stale one. Re-init resolves both.
        return producerId != entry.producerId || producerEpoch != entry.producerEpoch;
    }

    private TxnStateRecord recordOf(String txnId, TxnEntry entry) {
        return new TxnStateRecord(
                txnId,
                entry.producerId,
                entry.producerEpoch,
                entry.state,
                List.copyOf(entry.partitions),
                entry.timeoutMs,
                entry.lastUpdateMs);
    }

    private List<MarkerInstruction> markersOf(String txnId, TxnEntry entry) {
        boolean commit = entry.state == TxnState.PREPARE_COMMIT || entry.state == TxnState.COMPLETE_COMMIT;
        var out = new ArrayList<MarkerInstruction>(entry.partitions.size());
        for (var tp : entry.partitions) {
            out.add(new MarkerInstruction(txnId, tp, entry.producerId, entry.producerEpoch, commit, coordinatorEpoch));
        }
        return out;
    }
}
