package jbroker.broker.txn;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import jbroker.broker.ErrorCodes;
import jbroker.broker.TopicManager;
import jbroker.broker.replication.FollowerStateTracker;
import jbroker.storage.ControlRecord;
import jbroker.storage.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Leader-local marker append: the single place a transaction marker
 * becomes a control batch in a partition log. Both delivery paths land
 * here — the coordinator's fast path for partitions this broker leads,
 * and the {@code WriteTxnMarkers} inter-broker RPC for markers arriving
 * from a remote coordinator.
 *
 * <p>Guarantees per append:
 * <ul>
 *   <li><b>Leadership check</b> — only the partition leader appends;
 *       everyone else answers {@link ErrorCodes#NOT_LEADER} so the
 *       coordinator re-resolves and retries.</li>
 *   <li><b>Coordinator fencing</b> — a marker carrying a lower
 *       {@code coordinator_epoch} than one already applied for the same
 *       (partition, producer) comes from a deposed coordinator still
 *       flushing its queue; refused with
 *       {@link ErrorCodes#INVALID_TXN_STATE}.</li>
 *   <li><b>Idempotent redelivery</b> — a retry identical to the last
 *       applied marker that is STILL the log's final entry re-runs only
 *       the replication wait instead of appending a second control batch,
 *       so per-partition retries and coordinator-failover redelivery
 *       don't pile up duplicates. Anything appended since — in
 *       particular the producer's next transaction, whose marker under a
 *       stable epoch is byte-shape-identical — forces a fresh control
 *       batch. (The memory is per-process; after a restart one duplicate
 *       may land, which the storage layer treats as a no-op.)</li>
 *   <li><b>Durability</b> — the append is confirmed only after the
 *       {@link IsrReplicationWait acks=all wait}: a marker confirmed to
 *       the coordinator but lost with its leader would leave the
 *       partition's transactional data undecided forever.</li>
 * </ul>
 */
public final class TxnMarkerWriter {

    private static final Logger log = LoggerFactory.getLogger(TxnMarkerWriter.class);

    /** Bound on the post-append replication wait, mirroring the produce path. */
    static final long REPLICATION_TIMEOUT_MS = 5_000L;

    private record Key(String topic, int partition, long producerId) {}

    private record LastMarker(long offset, int producerEpoch, boolean commit, int coordinatorEpoch) {}

    /**
     * Notified once per freshly-appended marker (idempotent redeliveries
     * that only re-run the replication wait do NOT fire — their side
     * effects already ran). The group-coordinator wiring hooks this to
     * fold staged transactional offsets when a marker lands on a
     * {@code __consumer_offsets} partition. Listener failures are logged
     * and swallowed: the marker append itself already succeeded, and the
     * fold is reproducible from the log (staged batch + marker) on the
     * next recovery walk.
     */
    @FunctionalInterface
    public interface MarkerListener {
        void onMarkerAppended(String topic, int partition, long producerId, int producerEpoch, boolean commit);
    }

    private volatile MarkerListener markerListener = (topic, partition, pid, epoch, commit) -> {};

    public void setMarkerListener(MarkerListener listener) {
        this.markerListener = listener == null ? (topic, partition, pid, epoch, commit) -> {} : listener;
    }

    private final LogManager logManager;
    private final TopicManager topicManager;
    private final FollowerStateTracker followerTracker;
    private final int selfBrokerId;
    private final int clusterMinInsyncReplicas;
    private final TxnPartitionEpochs txnEpochs;
    private final ConcurrentHashMap<Key, LastMarker> lastMarkers = new ConcurrentHashMap<>();

    public TxnMarkerWriter(
            LogManager logManager,
            TopicManager topicManager,
            FollowerStateTracker followerTracker,
            int selfBrokerId,
            int clusterMinInsyncReplicas,
            TxnPartitionEpochs txnEpochs) {
        this.logManager = logManager;
        this.topicManager = topicManager;
        this.followerTracker = followerTracker;
        this.selfBrokerId = selfBrokerId;
        this.clusterMinInsyncReplicas = clusterMinInsyncReplicas;
        this.txnEpochs = txnEpochs;
    }

    /**
     * Append (or idempotently re-confirm) one marker. Returns
     * {@link ErrorCodes#NONE} once the control batch is in the log AND
     * replicated per the ISR floor; any other code means the coordinator
     * must retry (or, for {@code INVALID_TXN_STATE}, stand down — it has
     * been fenced by a successor).
     */
    public int append(
            String topic, int partition, long producerId, int producerEpoch, boolean commit, int coordinatorEpoch) {
        var state = topicManager.partitionState(topic, partition);
        if (state.isEmpty() || state.get().leader() != selfBrokerId) {
            return ErrorCodes.NOT_LEADER;
        }
        var key = new Key(topic, partition, producerId);
        var last = lastMarkers.get(key);
        if (last != null && coordinatorEpoch < last.coordinatorEpoch()) {
            log.warn(
                    "refusing marker for {}-{} pid {} from deposed coordinator epoch {} (applied {})",
                    topic,
                    partition,
                    producerId,
                    coordinatorEpoch,
                    last.coordinatorEpoch());
            return ErrorCodes.INVALID_TXN_STATE;
        }
        long offset;
        if (last != null
                && last.producerEpoch() == producerEpoch
                && last.commit() == commit
                && last.coordinatorEpoch() == coordinatorEpoch
                && isStillLastAppend(topic, partition, last.offset())) {
            // Duplicate delivery (per-partition retry or failover
            // redelivery): the marker bytes are already in the log — only
            // the replication wait may still be outstanding. Shape equality
            // alone is NOT enough: consecutive transactions under a stable
            // producer epoch and coordinator produce identical-shaped
            // markers, and swallowing the next transaction's marker would
            // leave it undecided forever (LSO wedge). Only a marker that is
            // still the log's last entry — nothing appended since — is a
            // true redelivery.
            offset = last.offset();
        } else {
            try {
                offset = logManager
                        .logFor(topic, partition)
                        .appendControl(
                                producerId,
                                (short) producerEpoch,
                                new ControlRecord(
                                        commit ? ControlRecord.Type.COMMIT : ControlRecord.Type.ABORT,
                                        coordinatorEpoch),
                                System.currentTimeMillis(),
                                state.get().leaderEpoch());
            } catch (IOException e) {
                log.warn("marker append failed for {}-{} pid {}: {}", topic, partition, producerId, e.toString());
                return ErrorCodes.IO_ERROR;
            }
            lastMarkers.put(key, new LastMarker(offset, producerEpoch, commit, coordinatorEpoch));
            // The marker's (possibly bumped) epoch fences older produces on
            // this partition from now on.
            txnEpochs.observe(topic, partition, producerId, producerEpoch);
            try {
                markerListener.onMarkerAppended(topic, partition, producerId, producerEpoch, commit);
            } catch (RuntimeException e) {
                log.warn(
                        "marker listener failed for {}-{} pid {} (fold is replay-recoverable): {}",
                        topic,
                        partition,
                        producerId,
                        e.toString());
            }
        }
        try {
            boolean replicated = IsrReplicationWait.await(
                    topicManager,
                    logManager,
                    followerTracker,
                    selfBrokerId,
                    clusterMinInsyncReplicas,
                    topic,
                    partition,
                    offset,
                    REPLICATION_TIMEOUT_MS);
            return replicated ? ErrorCodes.NONE : ErrorCodes.NOT_ENOUGH_REPLICAS;
        } catch (IOException e) {
            return ErrorCodes.IO_ERROR;
        }
    }

    /** True when {@code offset} is the log's final entry — no data or marker has landed since. */
    private boolean isStillLastAppend(String topic, int partition, long offset) {
        try {
            return logManager.logFor(topic, partition).nextOffset() == offset + 1;
        } catch (IOException e) {
            // Can't tell — append fresh; a duplicate marker is a storage
            // no-op, a swallowed one is not.
            return false;
        }
    }
}
