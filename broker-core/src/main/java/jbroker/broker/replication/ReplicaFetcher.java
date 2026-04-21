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
 * leader via {@link Peer} and appends them to the local {@link jbroker.storage.Log}.
 *
 * <p>Phase 6.2 is minimal: single partition, happy-path replication only.
 * The follower decodes each received batch and re-appends the constituent
 * records locally so offsets match deterministically as long as the
 * follower starts from offset 0 and receives all batches in order.
 * Truncation on leadership change is P6.4's job (OffsetsForLeaderEpoch).
 */
public final class ReplicaFetcher {

    private static final Logger log = LoggerFactory.getLogger(ReplicaFetcher.class);

    public interface Peer {
        ReplicaFetchResponse fetch(ReplicaFetchRequest req);
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
     * Issue a single {@code ReplicaFetch} to the leader and append anything
     * returned. Called repeatedly by a driver thread (or by tests).
     */
    public void pollOnce(int expectedLeaderEpoch) throws IOException {
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
                log.warn(
                        "replica fetch fenced for {}-{}: leader epoch {}, local {}",
                        topic,
                        partition,
                        resp.getCurrentLeaderEpoch(),
                        expectedLeaderEpoch);
                // P6.4 will truncate and reconcile; P6.2 just waits.
                return;
            }
            log.warn(
                    "replica fetch error for {}-{}: {}",
                    topic,
                    partition,
                    resp.getError().getMessage());
            return;
        }
        var records = resp.getRecords();
        if (records.isEmpty()) return;
        var buf = ByteBuffer.wrap(records.toByteArray());
        while (buf.remaining() >= RecordBatch.BATCH_OVERHEAD) {
            int mark = buf.position();
            RecordBatch.Parsed decoded;
            try {
                decoded = RecordBatch.decode(buf);
            } catch (IllegalArgumentException e) {
                buf.position(mark);
                break;
            }
            // Leader's LogSegment.transferTo streams from the sparse-index floor
            // entry for fetchOffset, so batches whose lastOffset < fetchOffset
            // can end up in the response. Skip them. Also skip empty-record
            // batches — Log.append rejects them — so a future peer can't
            // wedge this loop.
            if (decoded.records().isEmpty() || decoded.lastOffset() < fetchOffset) {
                continue;
            }
            // P6.2 renumbers offsets via Log.append; offsets converge only
            // because the follower starts at 0 and receives batches in order.
            // Timestamps, producer_id/epoch, base_sequence, and
            // partition_leader_epoch from the leader's batch are lost here;
            // P6.4 will replace this with a raw-batch append that preserves
            // all metadata byte-for-byte.
            local.append(decoded.records(), decoded.firstTimestamp());
        }
        highWatermark.set(resp.getHighWatermark());
    }

    public long highWatermark() {
        return highWatermark.get();
    }
}
