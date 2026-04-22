package jbroker.broker.replication;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import jbroker.broker.ErrorCodes;
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
 * <p>Phase 6.4: on {@link ErrorCodes#FENCED_EPOCH}, the fetcher calls
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
    private final AtomicLong highWatermark = new AtomicLong();

    public ReplicaFetcher(LogManager logManager, String topic, int partition, int selfBrokerId, Peer peer) {
        this.logManager = logManager;
        this.topic = topic;
        this.partition = partition;
        this.selfBrokerId = selfBrokerId;
        this.peer = peer;
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
        var local = logManager.logFor(topic, partition);
        long fetchOffset = local.nextOffset();
        var req = ReplicaFetchRequest.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setFollowerBrokerId(selfBrokerId)
                .setLeaderEpoch(expectedLeaderEpoch)
                .setFetchOffset(fetchOffset)
                .setMaxBytes(1024 * 1024)
                .build();
        var resp = peer.fetch(req);
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
        }
        highWatermark.set(resp.getHighWatermark());
        return PollResult.ADVANCED;
    }

    private PollResult reconcileOnFencedEpoch(int followerEpoch, int leaderCurrentEpoch) throws IOException {
        log.warn(
                "replica fetch fenced for {}-{}: follower epoch {}, leader at {}; reconciling",
                topic,
                partition,
                followerEpoch,
                leaderCurrentEpoch);
        long endOffset = peer.offsetsForLeaderEpoch(topic, partition, followerEpoch);
        var local = logManager.logFor(topic, partition);
        if (endOffset < local.nextOffset()) {
            log.warn(
                    "truncating {}-{} from {} to {} after OffsetsForLeaderEpoch",
                    topic,
                    partition,
                    local.nextOffset(),
                    endOffset);
            local.truncateTo(endOffset);
        }
        return PollResult.RECONCILED;
    }

    public long highWatermark() {
        return highWatermark.get();
    }
}
