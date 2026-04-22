package jbroker.broker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import jbroker.broker.replication.FollowerStateTracker;
import jbroker.proto.broker.ProduceRequest;
import jbroker.proto.broker.ProduceResponse;
import jbroker.storage.LogManager;
import jbroker.storage.RecordBatch;

/**
 * Handles {@code Produce} RPCs: decodes the inbound record-batch bytes,
 * re-encodes them with the assigned {@code baseOffset} from the target
 * partition's {@link jbroker.storage.Log}, and appends.
 *
 * <p>Milestone 6.7 adds idempotent-producer dedup. A request is considered
 * idempotent iff {@code producer_id > 0} (proto3 default {@code 0} and the
 * explicit legacy sentinel {@code -1} both bypass dedup — see
 * {@link ProducerIdRegistry}, whose first allocation is {@code 1}).
 *
 * <p>The handler tracks the highest-seen base sequence per
 * {@code (topic, partition, producer_id, producer_epoch)} and no-ops
 * duplicates, returning the cached offsets. Out-of-order sequences,
 * including a retry with a different record count, are rejected with
 * {@link ErrorCodes#OUT_OF_ORDER_SEQUENCE}.
 */
public final class ProduceHandler {

    private static final int ACKS_ALL = -1;
    private static final long ACKS_ALL_TIMEOUT_MS = 5_000L;
    private static final long ACKS_ALL_POLL_MS = 10L;

    private final LogManager logManager;
    private final TopicManager topicManager;
    private final int selfBrokerId;
    private final FollowerStateTracker followerTracker;

    // TODO(): idle-producer-state expiration. This map grows unbounded
    // across (topic, partition, producer_id, producer_epoch) pairs ever
    // seen. A long-running broker with many transient producers leaks
    // memory; chaos scripts in will need an LRU cap.
    private final ConcurrentHashMap<DedupKey, DedupEntry> dedup = new ConcurrentHashMap<>();

    public ProduceHandler(
            LogManager logManager, TopicManager topicManager, int selfBrokerId, FollowerStateTracker followerTracker) {
        this.logManager = logManager;
        this.topicManager = topicManager;
        this.selfBrokerId = selfBrokerId;
        this.followerTracker = followerTracker;
    }

    /** Back-compat: single-broker path where acks=all isn't meaningful. */
    public ProduceHandler(LogManager logManager, TopicManager topicManager, int selfBrokerId) {
        this(logManager, topicManager, selfBrokerId, new FollowerStateTracker());
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
        // batch locally. 's leader-epoch in Log.append closes the gap.
        var state = topicManager.partitionState(req.getTopic(), req.getPartition());
        if (state.isEmpty()) {
            return err(ErrorCodes.NOT_LEADER, "no leader for partition " + req.getTopic() + "-" + req.getPartition());
        }
        if (state.get().leader() != selfBrokerId) {
            return err(
                    ErrorCodes.NOT_LEADER,
                    "leader is broker " + state.get().leader() + " for " + req.getTopic() + "-" + req.getPartition());
        }

        // Decode once up front so the dedup path can compare record count.
        RecordBatch.Parsed parsed;
        try {
            parsed = RecordBatch.decode(ByteBuffer.wrap(req.getBatch().toByteArray()));
        } catch (IllegalArgumentException e) {
            return err(ErrorCodes.CORRUPT_BATCH, e.getMessage() == null ? e.toString() : e.getMessage());
        }

        boolean idempotent = req.getProducerId() > 0;
        if (!idempotent) {
            return appendAndRespond(req, parsed);
        }

        // Atomic dedup+append under a per-(producer, partition) key lock so
        // two concurrent retries for the same sequence can't both append.
        // The returned response is either a cached-duplicate or a fresh
        // append; the IOException wrapper below unwraps the IO failure.
        var result = new ProduceResult();
        var key = new DedupKey(req.getTopic(), req.getPartition(), req.getProducerId(), req.getProducerEpoch());
        dedup.compute(key, (k, cached) -> {
            if (cached != null
                    && cached.lastBaseSequence == req.getBaseSequence()
                    && cached.recordCount == parsed.records().size()) {
                result.response = ProduceResponse.newBuilder()
                        .setBaseOffset(cached.baseOffset)
                        .setLastOffset(cached.lastOffset)
                        .build();
                return cached;
            }
            // A retry with the same baseSequence but a different record count
            // is treated as out-of-order — the client re-batched and we
            // refuse to return offsets that don't cover the retry's payload.
            if (cached != null) {
                int expected = cached.lastBaseSequence + cached.recordCount;
                if (req.getBaseSequence() != expected) {
                    result.response = err(
                            ErrorCodes.OUT_OF_ORDER_SEQUENCE,
                            "expected base_sequence " + expected + ", got " + req.getBaseSequence());
                    return cached;
                }
            }
            // First batch or contiguous next batch: append.
            var appendResult = appendAndRespond(req, parsed);
            result.response = appendResult;
            if (!appendResult.hasError()) {
                return new DedupEntry(
                        req.getBaseSequence(),
                        parsed.records().size(),
                        appendResult.getBaseOffset(),
                        appendResult.getLastOffset());
            }
            return cached;
        });
        return result.response;
    }

    private ProduceResponse appendAndRespond(ProduceRequest req, RecordBatch.Parsed parsed) {
        try {
            var log = logManager.logFor(req.getTopic(), req.getPartition());
            long now = System.currentTimeMillis();
            long last = log.append(parsed.records(), now);
            long first = last - (parsed.records().size() - 1);
            if (req.getAcks() == ACKS_ALL) {
                var wait = waitForIsrReplication(req.getTopic(), req.getPartition(), last);
                if (wait != null) return wait;
            }
            return ProduceResponse.newBuilder()
                    .setBaseOffset(first)
                    .setLastOffset(last)
                    .build();
        } catch (IllegalArgumentException | IOException e) {
            return err(ErrorCodes.CORRUPT_BATCH, e.getMessage() == null ? e.toString() : e.getMessage());
        }
    }

    /**
     * Poll until the partition's HWM advances past {@code producedLastOffset},
     * bounded by {@link #ACKS_ALL_TIMEOUT_MS}. Returns {@code null} on
     * success; on timeout or leadership loss returns an error response.
     */
    private ProduceResponse waitForIsrReplication(String topic, int partition, long producedLastOffset)
            throws IOException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ACKS_ALL_TIMEOUT_MS);
        while (true) {
            var state = topicManager.partitionState(topic, partition);
            if (state.isEmpty() || state.get().leader() != selfBrokerId) {
                return err(
                        ErrorCodes.NOT_LEADER,
                        "lost leadership for " + topic + "-" + partition + " while waiting on acks=all");
            }
            long leaderLeo = logManager.logFor(topic, partition).nextOffset();
            long hwm = followerTracker.computeHwm(topic, partition, state.get().isr(), selfBrokerId, leaderLeo);
            // HWM convention: the first offset NOT yet durably replicated.
            // producedLastOffset is the highest offset in the produced batch,
            // so acks=all is satisfied when HWM > producedLastOffset.
            if (hwm > producedLastOffset) return null;
            if (System.nanoTime() >= deadline) {
                return err(
                        ErrorCodes.NOT_ENOUGH_REPLICAS,
                        "ISR did not replicate up to offset " + producedLastOffset + " within " + ACKS_ALL_TIMEOUT_MS
                                + "ms");
            }
            try {
                Thread.sleep(ACKS_ALL_POLL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return err(ErrorCodes.IO_ERROR, "interrupted while waiting on acks=all");
            }
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

    private static final class ProduceResult {
        ProduceResponse response;
    }
}
