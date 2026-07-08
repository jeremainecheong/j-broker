package jbroker.broker;

import java.io.IOException;
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
 * <p>Supports idempotent-producer dedup. A request is considered
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
    private final BrokerMetrics metrics;
    private final jbroker.broker.quota.QuotaEnforcer quotaEnforcer;
    private final ProducerStateManager producerState;

    public ProduceHandler(
            LogManager logManager,
            TopicManager topicManager,
            int selfBrokerId,
            FollowerStateTracker followerTracker,
            BrokerMetrics metrics) {
        this(logManager, topicManager, selfBrokerId, followerTracker, metrics, jbroker.broker.quota.QuotaEnforcer.NOOP);
    }

    /** Constructor with a {@link jbroker.broker.quota.QuotaEnforcer}. */
    public ProduceHandler(
            LogManager logManager,
            TopicManager topicManager,
            int selfBrokerId,
            FollowerStateTracker followerTracker,
            BrokerMetrics metrics,
            jbroker.broker.quota.QuotaEnforcer quotaEnforcer) {
        this(
                logManager,
                topicManager,
                selfBrokerId,
                followerTracker,
                metrics,
                quotaEnforcer,
                new ProducerStateManager());
    }

    /**
     * Audit-finding #1 — constructor accepting a shared {@link ProducerStateManager}
     * so the follower's replica-fetch apply path can observe every applied batch,
     * keeping idempotent-producer dedup state correct across leader failover.
     */
    public ProduceHandler(
            LogManager logManager,
            TopicManager topicManager,
            int selfBrokerId,
            FollowerStateTracker followerTracker,
            BrokerMetrics metrics,
            jbroker.broker.quota.QuotaEnforcer quotaEnforcer,
            ProducerStateManager producerState) {
        this.logManager = logManager;
        this.topicManager = topicManager;
        this.selfBrokerId = selfBrokerId;
        this.followerTracker = followerTracker;
        this.metrics = metrics == null ? new BrokerMetrics() : metrics;
        this.quotaEnforcer = quotaEnforcer == null ? jbroker.broker.quota.QuotaEnforcer.NOOP : quotaEnforcer;
        this.producerState = producerState == null ? new ProducerStateManager() : producerState;
    }

    /** Back-compat overload: omits metrics (tests that don't care). */
    public ProduceHandler(
            LogManager logManager, TopicManager topicManager, int selfBrokerId, FollowerStateTracker followerTracker) {
        this(logManager, topicManager, selfBrokerId, followerTracker, null);
    }

    /** Back-compat: single-broker path where acks=all isn't meaningful. */
    public ProduceHandler(LogManager logManager, TopicManager topicManager, int selfBrokerId) {
        this(logManager, topicManager, selfBrokerId, new FollowerStateTracker(), null);
    }

    public ProduceResponse handle(ProduceRequest req) {
        // Admission check. Uses "anonymous" until a later pass adds
        // authenticated principal extraction from gRPC metadata. Byte budget
        // is the serialized batch size so the budget matches the wire cost.
        var decision = quotaEnforcer.check(
                "anonymous",
                jbroker.broker.quota.QuotaEnforcer.Op.PRODUCE,
                req.getBatch().size());
        if (!decision.allow()) {
            return err(
                    ErrorCodes.QUOTA_VIOLATED,
                    "produce quota exceeded: " + decision.quotaBytesPerSec() + " B/s; retry in "
                            + decision.throttleMillis() + "ms");
        }
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
        // batch locally. The leader-epoch check in Log.append closes the gap.
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
        // Use ByteString.asReadOnlyByteBuffer() — zero-copy view of protobuf's
        // internal byte array. The prior toByteArray() copied every inbound
        // batch before decode; on the hot path that was allocation + memcpy
        // per produce with no other consumer of the raw bytes.
        RecordBatch.Parsed parsed;
        try {
            parsed = RecordBatch.decode(req.getBatch().asReadOnlyByteBuffer());
        } catch (IllegalArgumentException e) {
            return err(ErrorCodes.CORRUPT_BATCH, e.getMessage() == null ? e.toString() : e.getMessage());
        }

        boolean idempotent = req.getProducerId() > 0;
        if (!idempotent) {
            return appendAndRespond(req, parsed);
        }

        // The dedup map is in-memory only: after a restart this broker has
        // a full log but an empty map, and trusting the empty map turns
        // producer retries into double-appends (found by the chaos-with-
        // load soak; violates the no-duplicate-delivery guarantee for
        // idempotent producers). First idempotent produce per
        // partition rebuilds the map from the log before any dedup check.
        ensureProducerStateRecovered(req.getTopic(), req.getPartition());

        // Atomic dedup+append routed through the shared ProducerStateManager
        // so every broker (leader + followers) keeps the same view of applied
        // (pid, epoch, baseSequence) batches — that's what makes idempotent
        // retries survive leader failover.
        var key = new ProducerStateManager.DedupKey(
                req.getTopic(), req.getPartition(), req.getProducerId(), req.getProducerEpoch());
        var appendHolder = new AppendHolder();
        var result = producerState.dedupOrAppend(
                key, req.getBaseSequence(), parsed.records().size(), () -> {
                    var resp = appendAndRespond(req, parsed);
                    appendHolder.response = resp;
                    if (resp.hasError()) {
                        return null;
                    }
                    return new long[] {resp.getBaseOffset(), resp.getLastOffset()};
                });
        if (result.hasError()) {
            // Out-of-order sequence, or append IOException. Either way:
            // surface the error from the append side when present, else
            // OUT_OF_ORDER from the dedup check.
            if (appendHolder.response != null && appendHolder.response.hasError()) {
                return appendHolder.response;
            }
            return err(ErrorCodes.OUT_OF_ORDER_SEQUENCE, result.errorMessage());
        }
        if (result.cached()) {
            return ProduceResponse.newBuilder()
                    .setBaseOffset(result.baseOffset())
                    .setLastOffset(result.lastOffset())
                    .build();
        }
        // Fresh append — appendAndRespond already built the full response
        // (including latency metric + JFR event), just surface it verbatim.
        return appendHolder.response;
    }

    /** Capture slot so the Appender lambda can hand its full response back to the caller. */
    private static final class AppendHolder {
        ProduceResponse response;
    }

    /**
     * Partitions whose producer state has been rebuilt from the log this
     * process lifetime. computeIfAbsent serializes concurrent first
     * touches of the same partition; a failed rebuild stays unmarked so
     * the next produce retries it.
     */
    private final java.util.concurrent.ConcurrentHashMap<String, Boolean> producerStateRecovered =
            new java.util.concurrent.ConcurrentHashMap<>();

    private void ensureProducerStateRecovered(String topic, int partition) {
        producerStateRecovered.computeIfAbsent(topic + "-" + partition, tp -> {
            try {
                ProducerStateRecovery.rebuild(logManager, topic, partition, producerState);
                return Boolean.TRUE;
            } catch (IOException e) {
                // Leave unmarked; the append itself will surface disk
                // problems. Do not trust an empty map on a failed rebuild.
                org.slf4j.LoggerFactory.getLogger(ProduceHandler.class)
                        .warn("producer-state rebuild failed for {}-{}: {}", topic, partition, e.toString());
                return null;
            }
        });
    }

    private ProduceResponse appendAndRespond(ProduceRequest req, RecordBatch.Parsed parsed) {
        long startNs = System.nanoTime();
        try {
            var log = logManager.logFor(req.getTopic(), req.getPartition());
            long now = System.currentTimeMillis();
            // Audit-finding #1 — preserve producer id / epoch / baseSequence in
            // the on-disk batch so followers replicating this batch can observe
            // the dedup state (and a leader rebooting from its own log can
            // rebuild state without help).
            long last = log.append(
                    parsed.records(), now, req.getProducerId(), (short) req.getProducerEpoch(), req.getBaseSequence());
            long first = last - (parsed.records().size() - 1);
            if (req.getAcks() == ACKS_ALL) {
                var wait = waitForIsrReplication(req.getTopic(), req.getPartition(), last);
                if (wait != null) return wait;
            }
            long latencyNanos = System.nanoTime() - startNs;
            int bytes = req.getBatch().size();
            metrics.recordProduce(latencyNanos, bytes);
            var jfr = new jbroker.broker.jfr.ProduceLatencyEvent();
            if (jfr.shouldCommit()) {
                jfr.topic = req.getTopic();
                jfr.partition = req.getPartition();
                jfr.latencyNanos = latencyNanos;
                jfr.bytes = bytes;
                jfr.acks = req.getAcks();
                jfr.commit();
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
}
