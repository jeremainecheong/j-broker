package jbroker.broker.replication;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import jbroker.broker.ErrorCodes;
import jbroker.broker.ProducerStateManager;
import jbroker.proto.broker.ReplicaFetchRequest;
import jbroker.proto.broker.ReplicaFetchResponse;
import jbroker.storage.LogManager;
import jbroker.storage.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Follower-side pump that pulls committed record batches from the partition
 * leader via {@link Peer} and appends them byte-for-byte to the local
 * {@link jbroker.storage.Log}.
 *
 * <p>On {@link ErrorCodes#FENCED_EPOCH}, the fetcher calls
 * {@code OffsetsForLeaderEpoch} with its last-seen leader_epoch, truncates
 * any divergent suffix returned, and signals {@link PollResult#RECONCILED}
 * so the driver re-polls with the fresh epoch on the next tick.
 * Received batches are appended via {@link jbroker.storage.Log#appendRaw}
 * so producerId, producerEpoch, baseSequence, partitionLeaderEpoch, and
 * timestamps survive byte-identical.
 */
public final class ReplicaFetcher {

    private static final Logger log = LoggerFactory.getLogger(ReplicaFetcher.class);

    public interface Peer {
        ReplicaFetchResponse fetch(ReplicaFetchRequest req);

        long offsetsForLeaderEpoch(String topic, int partition, int leaderEpoch);
    }

    private final LogManager logManager;
    private final String topic;
    private final int partition;
    private final int selfBrokerId;
    private final Peer peer;
    private final ProducerStateManager producerState;
    private final AtomicLong highWatermark = new AtomicLong();

    public ReplicaFetcher(LogManager logManager, String topic, int partition, int selfBrokerId, Peer peer) {
        this(logManager, topic, partition, selfBrokerId, peer, null);
    }

    /**
     * Audit-finding #1 — TLS-aware constructor (misnomer aside, the extra
     * parameter is for idempotent-producer dedup state). Followers call
     * {@link ProducerStateManager#observeAppend} for every batch they
     * replicate so the broker's dedup view survives a leader failover onto
     * this peer.
     */
    public ReplicaFetcher(
            LogManager logManager,
            String topic,
            int partition,
            int selfBrokerId,
            Peer peer,
            ProducerStateManager producerState) {
        this.logManager = logManager;
        this.topic = topic;
        this.partition = partition;
        this.selfBrokerId = selfBrokerId;
        this.peer = peer;
        this.producerState = producerState;
    }

    /**
     * Outcome of a single {@code pollOnce}. The driver uses this to decide
     * whether to immediately schedule the next poll ({@code ADVANCED}),
     * reconcile-then-retry ({@code RECONCILED}), or back off
     * ({@code EMPTY} / {@code ERROR}).
     */
    public enum PollResult {
        ADVANCED,
        EMPTY,
        RECONCILED,
        ERROR
    }

    /**
     * Issue a single {@code ReplicaFetch} to the leader and append anything
     * returned. Called repeatedly by a driver thread (or by tests).
     */
    public PollResult pollOnce(int expectedLeaderEpoch) throws IOException {
        return pollOnce(expectedLeaderEpoch, DEFAULT_MAX_FETCH_BYTES);
    }

    /** Default per-fetch byte ceiling for an unthrottled follower. */
    public static final int DEFAULT_MAX_FETCH_BYTES = 1024 * 1024;

    /**
     * As {@link #pollOnce(int)} but with an explicit per-fetch byte ceiling.
     * A reassignment throttle passes a smaller ceiling for an "adding" replica
     * so its catch-up fetch cannot saturate the link; the leader returns at
     * most {@code maxBytes} of records per fetch.
     */
    public PollResult pollOnce(int expectedLeaderEpoch, int maxBytes) throws IOException {
        var local = logManager.logFor(topic, partition);
        long fetchOffset = local.nextOffset();
        var reqBuilder = ReplicaFetchRequest.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setFollowerBrokerId(selfBrokerId)
                .setLeaderEpoch(expectedLeaderEpoch)
                .setFetchOffset(fetchOffset)
                .setMaxBytes(maxBytes);
        // Advertise the epoch owning our last local batch (log LINEAGE, not
        // metadata) so the leader can fence a diverged tail before we glue
        // its records on top of one. Absent for empty logs and logs
        // predating checkpoint maintenance — the leader skips the check.
        if (fetchOffset > 0) {
            logManager
                    .leaderEpochCheckpoint(topic, partition)
                    .epochFor(fetchOffset - 1)
                    .ifPresent(e -> reqBuilder.setLastFetchedEpoch(e.epoch()));
        }
        var resp = peer.fetch(reqBuilder.build());
        if (resp.hasError() && resp.getError().getCode() != ErrorCodes.NONE) {
            int code = resp.getError().getCode();
            if (code == ErrorCodes.FENCED_EPOCH) {
                return reconcileOnFencedEpoch(expectedLeaderEpoch, resp.getCurrentLeaderEpoch());
            }
            log.warn(
                    "replica fetch error for {}-{}: {}",
                    topic,
                    partition,
                    resp.getError().getMessage());
            return PollResult.ERROR;
        }
        var records = resp.getRecords();
        if (records.isEmpty()) {
            highWatermark.set(resp.getHighWatermark());
            return PollResult.EMPTY;
        }
        byte[] bytes = records.toByteArray();
        var buf = ByteBuffer.wrap(bytes);
        while (buf.remaining() >= RecordBatch.BATCH_OVERHEAD) {
            int startPos = buf.position();
            RecordBatch.Parsed decoded;
            try {
                decoded = RecordBatch.decode(buf);
            } catch (IllegalArgumentException e) {
                buf.position(startPos);
                break;
            }
            int endPos = buf.position();
            // Leader's LogSegment.transferTo streams from the sparse-index
            // floor, so batches whose lastOffset < fetchOffset can appear
            // in the response. Skip them (also skips empty-record batches).
            if (decoded.records().isEmpty() || decoded.lastOffset() < fetchOffset) {
                continue;
            }
            // Byte-identical append — preserves every header field the
            // leader wrote. appendRaw verifies baseOffset matches the
            // follower's current LEO.
            byte[] slice = new byte[endPos - startPos];
            System.arraycopy(bytes, startPos, slice, 0, slice.length);
            local.appendRaw(slice, decoded.baseOffset());
            // Derive our leader-epoch checkpoint from the replicated batch
            // headers: (epoch, baseOffset) on every epoch increase. This is
            // the offset-accurate lineage record that both sides of the
            // fetch-time divergence check depend on — the metadata-apply
            // listener only records entries on the broker that IS the new
            // leader, so followers get theirs here.
            recordLineage(decoded.partitionLeaderEpoch(), decoded.baseOffset());
            // Audit-finding #1 — every applied idempotent batch advances our
            // local ProducerStateManager so a future takeover as leader
            // already has the dedup state needed to recognize retries.
            if (producerState != null && decoded.producerId() > 0) {
                var key = new ProducerStateManager.DedupKey(
                        topic, partition, decoded.producerId(), (int) decoded.producerEpoch());
                producerState.observeAppend(
                        key,
                        decoded.baseSequence(),
                        decoded.records().size(),
                        decoded.baseOffset(),
                        decoded.lastOffset());
            }
        }
        highWatermark.set(resp.getHighWatermark());
        return PollResult.ADVANCED;
    }

    private PollResult reconcileOnFencedEpoch(int followerEpoch, int leaderCurrentEpoch) throws IOException {
        var local = logManager.logFor(topic, partition);
        // Reconcile by LOG lineage, not metadata: ask the leader where the
        // epoch owning our last local batch ends in ITS history. Using the
        // metadata epoch here would ask about an epoch our log may not
        // even contain, missing the divergent range entirely. Falls back
        // to the metadata epoch for logs without lineage entries.
        int reconcileEpoch = local.nextOffset() > 0
                ? logManager
                        .leaderEpochCheckpoint(topic, partition)
                        .epochFor(local.nextOffset() - 1)
                        .map(jbroker.storage.LeaderEpochCheckpoint.Entry::epoch)
                        .orElse(followerEpoch)
                : followerEpoch;
        log.warn(
                "replica fetch fenced for {}-{}: follower epoch {} (lineage {}), leader at {}; reconciling",
                topic,
                partition,
                followerEpoch,
                reconcileEpoch,
                leaderCurrentEpoch);
        long endOffset = peer.offsetsForLeaderEpoch(topic, partition, reconcileEpoch);
        if (endOffset == UNDEFINED_EPOCH_OFFSET) {
            // Leader remembers nothing at or below our lineage epoch: our
            // entire log predates its history. Rebuild from zero.
            endOffset = 0L;
        }
        if (endOffset < local.nextOffset()) {
            log.warn(
                    "truncating {}-{} from {} to {} after OffsetsForLeaderEpoch",
                    topic,
                    partition,
                    local.nextOffset(),
                    endOffset);
            local.truncateTo(endOffset);
            // The checkpoint must shrink with the log — entries describing
            // the wiped range would keep answering with a lineage we no
            // longer have, and assign()'s monotonicity guard would refuse
            // the corrected entries on refetch.
            logManager.leaderEpochCheckpoint(topic, partition).truncateFrom(endOffset);
        }
        return PollResult.RECONCILED;
    }

    /** Sentinel from {@code OffsetsForLeaderEpoch}: requested epoch predates the leader's history. */
    private static final long UNDEFINED_EPOCH_OFFSET = -1L;

    /**
     * Record an (epoch, startOffset) lineage entry when replicated batches
     * cross into a higher epoch. Failures are logged, not thrown — lineage
     * is a safety net; replication itself must not stall on checkpoint IO.
     */
    private void recordLineage(int batchEpoch, long baseOffset) {
        try {
            var cp = logManager.leaderEpochCheckpoint(topic, partition);
            var entries = cp.entries();
            if (entries.isEmpty() || entries.get(entries.size() - 1).epoch() < batchEpoch) {
                cp.assign(batchEpoch, baseOffset);
            }
        } catch (IOException e) {
            log.warn("failed to record lineage ({}, {}) for {}-{}: {}", batchEpoch, baseOffset, topic, partition, e);
        }
    }

    public long highWatermark() {
        return highWatermark.get();
    }
}
