package jbroker.broker.txn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.LongSupplier;
import jbroker.broker.ErrorCodes;
import jbroker.broker.TopicManager;
import jbroker.broker.replication.FollowerStateTracker;
import jbroker.proto.common.TopicPartition;
import jbroker.storage.LogManager;
import jbroker.storage.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Life-cycle and I/O shell around {@link TxnCoordinator}: one active core
 * per {@code __transaction_state} partition this broker currently leads,
 * created by replaying the partition ({@link TxnStateRecovery}) at the
 * partition's leader epoch and dropped the moment leadership (or the
 * epoch) moves.
 *
 * <p>The core is pure and returns effects; this class executes them in
 * the pinned order:
 * <ol>
 *   <li><b>Append before answer.</b> Every returned state record is
 *       appended to the coordinator partition and replicated (acks=all)
 *       before the caller sees a response or any marker leaves this
 *       broker. A failed append deactivates the partition — the next
 *       access rebuilds in-memory state from the log, the only source of
 *       truth — and the client receives a retriable error.</li>
 *   <li><b>Deliver markers, then confirm.</b> Marker instructions run on
 *       a background task, retried per partition against the current
 *       leader (local append or the {@code WriteTxnMarkers} RPC behind
 *       {@link MarkerTransport}) until every partition confirms; only
 *       then is {@link TxnCoordinator#markersDelivered} fed back and the
 *       Complete record appended. A deposed coordinator's tasks stand
 *       down; the successor resumes from {@link TxnCoordinator#pendingMarkers}
 *       after replay.</li>
 * </ol>
 *
 * <p>{@link #tick} is the periodic driver: it prunes deposed partitions,
 * activates newly-led ones (resuming their pending marker deliveries —
 * this is what completes a predecessor's in-flight commits after
 * failover), and runs the transaction-timeout sweep.
 *
 * <p>Per-transaction monitors serialize {@code core-transition + append}
 * so the log's record order for a transactional_id matches the order the
 * core applied the transitions — otherwise a replay could resurrect an
 * older partition set over a newer one.
 */
public final class TxnCoordinatorRuntime implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TxnCoordinatorRuntime.class);

    /** Bound on the state-record replication wait, mirroring the produce path. */
    static final long STATE_APPEND_TIMEOUT_MS = 5_000L;

    /** Pause between delivery retry rounds while some partition refuses its marker. */
    static final long DELIVERY_RETRY_BACKOFF_MS = 200L;

    /** One attempt at landing one marker; true once the target partition's leader confirmed the control batch. */
    public interface MarkerTransport {
        boolean deliver(TxnCoordinator.MarkerInstruction instruction);
    }

    /** Allocates a fresh producer id through the controller's replicated counter. */
    public interface ProducerIdAllocator {
        long allocate() throws Exception;
    }

    /** Outcome of {@link #initTransactions} for the RPC layer. */
    public record InitOutcome(int errorCode, long producerId, int producerEpoch) {}

    private record Active(TxnCoordinator core, int coordinatorEpoch) {}

    private final LogManager logManager;
    private final TopicManager topicManager;
    private final FollowerStateTracker followerTracker;
    private final int selfBrokerId;
    private final int clusterMinInsyncReplicas;
    private final ProducerIdAllocator pidAllocator;
    private final MarkerTransport transport;
    private final LongSupplier wallClockMillis;

    private final ConcurrentHashMap<Integer, Active> active = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Object> activationLocks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> txnLocks = new ConcurrentHashMap<>();
    private final Set<String> inFlightDeliveries = ConcurrentHashMap.newKeySet();
    private final ExecutorService deliveryPool = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("txn-marker-delivery-", 0).factory());
    private volatile boolean closed;

    public TxnCoordinatorRuntime(
            LogManager logManager,
            TopicManager topicManager,
            FollowerStateTracker followerTracker,
            int selfBrokerId,
            int clusterMinInsyncReplicas,
            ProducerIdAllocator pidAllocator,
            MarkerTransport transport,
            LongSupplier wallClockMillis) {
        this.logManager = logManager;
        this.topicManager = topicManager;
        this.followerTracker = followerTracker;
        this.selfBrokerId = selfBrokerId;
        this.clusterMinInsyncReplicas = clusterMinInsyncReplicas;
        this.pidAllocator = pidAllocator;
        this.transport = transport;
        this.wallClockMillis = wallClockMillis;
    }

    /**
     * Wakeup hub for the state-record replication wait. Defaults to a
     * private unsignaled instance (sliced poll); the broker replaces it
     * with the shared hub so state appends complete on HWM advance.
     */
    private volatile jbroker.broker.replication.ReplicationSignals replicationSignals =
            new jbroker.broker.replication.ReplicationSignals();

    public void setReplicationSignals(jbroker.broker.replication.ReplicationSignals signals) {
        this.replicationSignals = java.util.Objects.requireNonNull(signals);
    }

    /**
     * InitTransactions against the coordinator of {@code partition}. The
     * candidate producer id is allocated up front (outside any lock — it
     * is a Raft proposal); ids burned on paths that don't use the
     * candidate are tolerated, matching the counter's gap policy.
     */
    public InitOutcome initTransactions(int partition, String txnId, int timeoutMs) {
        var actOpt = activeFor(partition);
        if (actOpt.isEmpty()) return new InitOutcome(ErrorCodes.COORDINATOR_NOT_AVAILABLE, -1L, -1);
        var act = actOpt.get();
        long candidatePid;
        try {
            candidatePid = pidAllocator.allocate();
        } catch (Exception e) {
            log.warn("producer-id allocation failed for txn '{}': {}", txnId, e.toString());
            return new InitOutcome(ErrorCodes.COORDINATOR_NOT_AVAILABLE, -1L, -1);
        }
        synchronized (txnLock(partition, txnId)) {
            var result = act.core().initTransactions(txnId, timeoutMs, candidatePid, wallClockMillis.getAsLong());
            if (result.stateRecord().isPresent()) {
                int appended =
                        appendStateRecord(partition, act, result.stateRecord().get());
                if (appended != ErrorCodes.NONE) return new InitOutcome(appended, -1L, -1);
            }
            // Markers only exist when the init aborted a previous epoch's
            // in-flight transaction; they carry the bumped epoch.
            startDelivery(partition, act, txnId, result.markers());
            return new InitOutcome(result.errorCode(), result.producerId(), result.producerEpoch());
        }
    }

    /** AddPartitionsToTxn against the coordinator of {@code partition}. */
    public int addPartitions(
            int partition, String txnId, long producerId, int producerEpoch, List<TopicPartition> partitions) {
        var actOpt = activeFor(partition);
        if (actOpt.isEmpty()) return ErrorCodes.COORDINATOR_NOT_AVAILABLE;
        var act = actOpt.get();
        synchronized (txnLock(partition, txnId)) {
            var result =
                    act.core().addPartitions(txnId, producerId, producerEpoch, partitions, wallClockMillis.getAsLong());
            if (result.stateRecord().isPresent()) {
                int appended =
                        appendStateRecord(partition, act, result.stateRecord().get());
                if (appended != ErrorCodes.NONE) return appended;
            }
            return result.errorCode();
        }
    }

    /**
     * EndTxn against the coordinator of {@code partition}. Answers once
     * the Prepare* record is durable; marker delivery continues in the
     * background per the RPC contract.
     */
    public int endTxn(int partition, String txnId, long producerId, int producerEpoch, boolean commit) {
        var actOpt = activeFor(partition);
        if (actOpt.isEmpty()) return ErrorCodes.COORDINATOR_NOT_AVAILABLE;
        var act = actOpt.get();
        synchronized (txnLock(partition, txnId)) {
            var result = act.core().endTxn(txnId, producerId, producerEpoch, commit, wallClockMillis.getAsLong());
            if (result.stateRecord().isPresent()) {
                int appended =
                        appendStateRecord(partition, act, result.stateRecord().get());
                if (appended != ErrorCodes.NONE) return appended;
            }
            startDelivery(partition, act, txnId, result.markers());
            return result.errorCode();
        }
    }

    /**
     * Periodic driver, called from the broker's housekeeping ticker:
     * prune deposed partitions, activate newly-led ones (which resumes a
     * dead predecessor's pending marker deliveries), sweep transaction
     * timeouts, and re-launch any Prepare* delivery that is not in
     * flight.
     */
    public void tick() {
        if (closed) return;
        var desc = topicManager.describe(TxnStateTopic.NAME);
        if (desc.isEmpty()) return;
        int partitionCount = desc.get().partitions();
        for (int p = 0; p < partitionCount; p++) {
            var ps = topicManager.partitionState(TxnStateTopic.NAME, p);
            if (ps.isEmpty() || ps.get().leader() != selfBrokerId) {
                invalidate(p);
                continue;
            }
            var actOpt = activeFor(p);
            if (actOpt.isEmpty()) continue;
            var act = actOpt.get();
            for (var abort : act.core().tick(wallClockMillis.getAsLong())) {
                synchronized (txnLock(p, abort.transactionalId())) {
                    int appended = appendStateRecord(p, act, abort.stateRecord());
                    if (appended != ErrorCodes.NONE) break; // deactivated; the next tick replays and re-aborts
                    startDelivery(p, act, abort.transactionalId(), abort.markers());
                }
            }
            resumePendingDeliveries(p, act);
        }
    }

    /** Test/observability accessor: the active coordinator's view of one transactional_id. */
    public Optional<TxnStateRecord> stateOf(int partition, String txnId) {
        var act = active.get(partition);
        return act == null ? Optional.empty() : act.core().stateOf(txnId);
    }

    @Override
    public void close() {
        closed = true;
        deliveryPool.shutdownNow();
        active.clear();
    }

    // --- activation ---

    private Optional<Active> activeFor(int partition) {
        var ps = topicManager.partitionState(TxnStateTopic.NAME, partition);
        if (ps.isEmpty() || ps.get().leader() != selfBrokerId) {
            invalidate(partition);
            return Optional.empty();
        }
        int epoch = ps.get().leaderEpoch();
        var current = active.get(partition);
        if (current != null && current.coordinatorEpoch() == epoch) return Optional.of(current);
        synchronized (activationLocks.computeIfAbsent(partition, k -> new Object())) {
            current = active.get(partition);
            if (current != null && current.coordinatorEpoch() == epoch) return Optional.of(current);
            var core = new TxnCoordinator(epoch);
            int applied;
            try {
                applied = TxnStateRecovery.rebuild(logManager, partition, core);
            } catch (IOException e) {
                log.warn("txn coordinator activation replay failed for partition {}: {}", partition, e.toString());
                return Optional.empty();
            }
            var fresh = new Active(core, epoch);
            active.put(partition, fresh);
            log.info(
                    "activated txn coordinator for __transaction_state-{} at epoch {} ({} records replayed)",
                    partition,
                    epoch,
                    applied);
            // Decided-but-unconfirmed transactions the predecessor left
            // behind: redeliver their markers (idempotent at the
            // partitions) and complete them.
            resumePendingDeliveries(partition, fresh);
            return Optional.of(fresh);
        }
    }

    private void invalidate(int partition) {
        var removed = active.remove(partition);
        if (removed != null) {
            log.info(
                    "deactivated txn coordinator for __transaction_state-{} (epoch {})",
                    partition,
                    removed.coordinatorEpoch());
        }
    }

    private Object txnLock(int partition, String txnId) {
        return txnLocks.computeIfAbsent(partition + ":" + txnId, k -> new Object());
    }

    // --- state-record durability ---

    /**
     * Append one state record to the coordinator partition and wait for
     * acks=all replication. Any failure deactivates the partition so the
     * next access rebuilds from the log — in-memory state has already
     * moved past what the log holds (or may hold), and the log wins.
     */
    private int appendStateRecord(int partition, Active act, TxnStateRecord rec) {
        var ps = topicManager.partitionState(TxnStateTopic.NAME, partition);
        if (ps.isEmpty() || ps.get().leader() != selfBrokerId || ps.get().leaderEpoch() != act.coordinatorEpoch()) {
            invalidate(partition);
            return ErrorCodes.NOT_COORDINATOR;
        }
        try {
            var stateLog = logManager.logFor(TxnStateTopic.NAME, partition);
            var record = new Record(
                    0, 0L, TxnStateTopic.keyForTxn(rec.transactionalId()), TxnStateTopic.valueForTxnState(rec));
            long last = stateLog.append(
                    List.of(record),
                    wallClockMillis.getAsLong(),
                    /*producerId*/ -1L,
                    /*producerEpoch*/ (short) -1,
                    /*baseSequence*/ -1,
                    ps.get().leaderEpoch());
            boolean replicated = IsrReplicationWait.await(
                    replicationSignals,
                    topicManager,
                    logManager,
                    followerTracker,
                    selfBrokerId,
                    clusterMinInsyncReplicas,
                    TxnStateTopic.NAME,
                    partition,
                    last,
                    STATE_APPEND_TIMEOUT_MS);
            if (!replicated) {
                log.warn(
                        "txn state record for '{}' did not replicate on __transaction_state-{}; rebuilding from log",
                        rec.transactionalId(),
                        partition);
                invalidate(partition);
                return ErrorCodes.COORDINATOR_NOT_AVAILABLE;
            }
            return ErrorCodes.NONE;
        } catch (IOException e) {
            log.warn(
                    "txn state append failed for '{}' on __transaction_state-{}: {}",
                    rec.transactionalId(),
                    partition,
                    e.toString());
            invalidate(partition);
            return ErrorCodes.COORDINATOR_NOT_AVAILABLE;
        }
    }

    // --- marker delivery ---

    private void resumePendingDeliveries(int partition, Active act) {
        var byTxn = new LinkedHashMap<String, List<TxnCoordinator.MarkerInstruction>>();
        for (var instruction : act.core().pendingMarkers()) {
            byTxn.computeIfAbsent(instruction.transactionalId(), k -> new ArrayList<>())
                    .add(instruction);
        }
        for (var e : byTxn.entrySet()) {
            startDelivery(partition, act, e.getKey(), e.getValue());
        }
    }

    private void startDelivery(
            int partition, Active act, String txnId, List<TxnCoordinator.MarkerInstruction> markers) {
        if (markers.isEmpty() || closed) return;
        String key = partition + ":" + txnId;
        if (!inFlightDeliveries.add(key)) return; // already being delivered
        long producerId = markers.get(0).producerId();
        int producerEpoch = markers.get(0).producerEpoch();
        try {
            deliveryPool.execute(() -> {
                try {
                    var remaining = new ArrayList<>(markers);
                    while (!closed && !remaining.isEmpty()) {
                        // Stand down when deposed or deactivated: the
                        // successor (or the reactivation replay) owns
                        // redelivery from here.
                        if (active.get(partition) != act) return;
                        remaining.removeIf(transport::deliver);
                        if (remaining.isEmpty()) break;
                        try {
                            Thread.sleep(DELIVERY_RETRY_BACKOFF_MS);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                    if (!closed && remaining.isEmpty()) {
                        completeDelivery(partition, act, txnId, producerId, producerEpoch);
                    }
                } finally {
                    inFlightDeliveries.remove(key);
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // Closing; the successor coordinator resumes delivery.
            inFlightDeliveries.remove(key);
        }
    }

    private void completeDelivery(int partition, Active act, String txnId, long producerId, int producerEpoch) {
        synchronized (txnLock(partition, txnId)) {
            var result = act.core().markersDelivered(txnId, producerId, producerEpoch, wallClockMillis.getAsLong());
            if (result.errorCode() != ErrorCodes.NONE) {
                log.warn(
                        "markersDelivered for '{}' answered {} on __transaction_state-{}",
                        txnId,
                        result.errorCode(),
                        partition);
                return;
            }
            if (result.stateRecord().isPresent()) {
                // On failure the partition is deactivated; reactivation
                // replays Prepare* and redelivers (idempotent), then
                // retries this confirmation.
                appendStateRecord(partition, act, result.stateRecord().get());
            }
        }
    }
}
