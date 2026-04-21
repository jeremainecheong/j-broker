package jbroker.broker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import jbroker.proto.broker.ProduceRequest;
import jbroker.proto.broker.ProduceResponse;
import jbroker.storage.LogManager;
import jbroker.storage.RecordBatch;

/**
 * Handles {@code Produce} RPCs: decodes the inbound record-batch bytes,
 * re-encodes them with the assigned {@code baseOffset} from the target
 * partition's {@link jbroker.storage.Log}, and appends.
 *
 * <p>Phase 6.7 adds idempotent-producer dedup: when a request carries a
 * real {@code producer_id} ({@code >= 0}), the handler tracks the
 * highest-seen base sequence per {@code (topic, partition, producer_id,
 * producer_epoch)} and no-ops duplicates, returning the cached offsets.
 * Out-of-order sequences are rejected with {@link ErrorCodes#OUT_OF_ORDER_SEQUENCE}.
 * Legacy requests ({@code producer_id == -1}) skip dedup entirely.
 */
public final class ProduceHandler {

    private final LogManager logManager;
    private final TopicManager topicManager;
    private final int selfBrokerId;
    private final ConcurrentHashMap<DedupKey, DedupEntry> dedup = new ConcurrentHashMap<>();

    public ProduceHandler(LogManager logManager, TopicManager topicManager, int selfBrokerId) {
        this.logManager = logManager;
        this.topicManager = topicManager;
        this.selfBrokerId = selfBrokerId;
    }

    public ProduceResponse handle(ProduceRequest req) {
        var topic = topicManager.describe(req.getTopic());
        if (topic.isEmpty()) {
            return err(ErrorCodes.UNKNOWN_TOPIC, "unknown topic: " + req.getTopic());
        }
        if (req.getPartition() < 0 || req.getPartition() >= topic.get().partitions()) {
            return err(ErrorCodes.INVALID_PARTITION, "invalid partition: " + req.getPartition());
        }
        // Read (leader, epoch) atomically via partitionState so the two can't
        // straddle a concurrent PartitionChangeRecord apply. Check-then-
        // append still has a race: a leadership change committed between
        // this snapshot and log.append can let a deposed leader write one
        // batch locally. P6.4's leader-epoch in Log.append closes the gap.
        var state = topicManager.partitionState(req.getTopic(), req.getPartition());
        if (state.isEmpty()) {
            return err(ErrorCodes.NOT_LEADER, "no leader for partition " + req.getTopic() + "-" + req.getPartition());
        }
        if (state.get().leader() != selfBrokerId) {
            return err(
                    ErrorCodes.NOT_LEADER,
                    "leader is broker " + state.get().leader() + " for " + req.getTopic() + "-" + req.getPartition());
        }

        // Idempotent-producer path: only engages when producer_id >= 0.
        boolean idempotent = req.getProducerId() >= 0;
        DedupKey key = idempotent
                ? new DedupKey(req.getTopic(), req.getPartition(), req.getProducerId(), req.getProducerEpoch())
                : null;
        if (idempotent) {
            var cached = dedup.get(key);
            if (cached != null && req.getBaseSequence() == cached.lastBaseSequence) {
                // Duplicate — return cached offsets without appending.
                return ProduceResponse.newBuilder()
                        .setBaseOffset(cached.baseOffset)
                        .setLastOffset(cached.lastOffset)
                        .build();
            }
        }

        try {
            var incoming = ByteBuffer.wrap(req.getBatch().toByteArray());
            var parsed = RecordBatch.decode(incoming);
            if (idempotent) {
                var cached = dedup.get(key);
                // First batch for a (producer_id, epoch) pair establishes the
                // baseline — base_sequence can be any non-negative int. Once
                // a cache entry exists, subsequent batches must be strictly
                // contiguous so a lost-retry doesn't silently create a gap.
                if (cached != null) {
                    int expected = cached.lastBaseSequence + cached.recordCount;
                    if (req.getBaseSequence() != expected) {
                        return err(
                                ErrorCodes.OUT_OF_ORDER_SEQUENCE,
                                "expected base_sequence " + expected + ", got " + req.getBaseSequence());
                    }
                }
            }
            var log = logManager.logFor(req.getTopic(), req.getPartition());
            long now = System.currentTimeMillis();
            long last = log.append(parsed.records(), now);
            long first = last - (parsed.records().size() - 1);
            if (idempotent) {
                dedup.put(
                        key,
                        new DedupEntry(req.getBaseSequence(), parsed.records().size(), first, last));
            }
            return ProduceResponse.newBuilder()
                    .setBaseOffset(first)
                    .setLastOffset(last)
                    .build();
        } catch (IllegalArgumentException | IOException e) {
            return err(ErrorCodes.CORRUPT_BATCH, e.getMessage() == null ? e.toString() : e.getMessage());
        }
    }

    private static ProduceResponse err(int code, String message) {
        return ProduceResponse.newBuilder()
                .setError(jbroker.proto.broker.Error.newBuilder()
                        .setCode(code)
                        .setMessage(message)
                        .build())
                .build();
    }

    private record DedupKey(String topic, int partition, long producerId, int producerEpoch) {}

    private record DedupEntry(int lastBaseSequence, int recordCount, long baseOffset, long lastOffset) {}
}
