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
 *
 * <p>Dedup state reflects the LOG, not the ack: a batch that appended but
 * failed its acks=all replication wait stays recorded, so a retry dedups
 * to the cached offsets instead of appending a second copy — and then
 * re-runs the replication wait on those offsets, because "seen before"
 * does not imply "committed."
 */
public final class ProduceHandler {

    private static final int ACKS_ALL = -1;
    private static final long ACKS_ALL_TIMEOUT_MS = 5_000L;
    private static final long ACKS_ALL_POLL_MS = 10L;

    /**
     * Cluster-wide default for the acks=all durability floor. Two is the
     * point of the feature: with a floor of one, an ISR shrunk to just the
     * leader satisfies acks=all with a single copy, and a subsequent
     * leader loss loses acked records (#115). Topics override per-topic
     * via {@link TopicDescription#MIN_INSYNC_REPLICAS_CONFIG}; topics with
     * replication factor 1 are clamped down so dev setups keep working.
     */
    public static final int DEFAULT_MIN_INSYNC_REPLICAS = 2;

    private final LogManager logManager;
    private final TopicManager topicManager;
    private final int selfBrokerId;
    private final FollowerStateTracker followerTracker;
    private final BrokerMetrics metrics;
    private final jbroker.broker.quota.QuotaEnforcer quotaEnforcer;
    private final ProducerStateManager producerState;
    private final int minInsyncReplicas;

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
        this(
                logManager,
                topicManager,
                selfBrokerId,
                followerTracker,
                metrics,
                quotaEnforcer,
                producerState,
                DEFAULT_MIN_INSYNC_REPLICAS);
    }

    /**
     * Full constructor — adds the cluster-default {@code
     * min.insync.replicas} floor for acks=all produces (per-topic config
     * overrides it; see {@link TopicDescription#effectiveMinInsyncReplicas}).
     */
    public ProduceHandler(
            LogManager logManager,
            TopicManager topicManager,
            int selfBrokerId,
            FollowerStateTracker followerTracker,
            BrokerMetrics metrics,
            jbroker.broker.quota.QuotaEnforcer quotaEnforcer,
            ProducerStateManager producerState,
            int minInsyncReplicas) {
        this.logManager = logManager;
        this.topicManager = topicManager;
        this.selfBrokerId = selfBrokerId;
        this.followerTracker = followerTracker;
        this.metrics = metrics == null ? new BrokerMetrics() : metrics;
        this.quotaEnforcer = quotaEnforcer == null ? jbroker.broker.quota.QuotaEnforcer.NOOP : quotaEnforcer;
        this.producerState = producerState == null ? new ProducerStateManager() : producerState;
        if (minInsyncReplicas < 1) {
            throw new IllegalArgumentException("minInsyncReplicas must be ≥ 1, got " + minInsyncReplicas);
        }
        this.minInsyncReplicas = minInsyncReplicas;
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
        // acks=all durability floor (#115): refuse the write BEFORE the
        // append when the ISR is already below min.insync.replicas. An ISR
        // of one trivially satisfies "all in-sync replicas have it", so
        // without this floor a produce can be acked with a single copy and
        // lost when that leader dies. Rejecting pre-append keeps the error
        // retriable with no divergence risk.
        if (req.getAcks() == ACKS_ALL) {
            int floor = topic.get().effectiveMinInsyncReplicas(minInsyncReplicas);
            int isrSize = state.get().isr().size();
            if (isrSize < floor) {
                return err(
                        ErrorCodes.NOT_ENOUGH_REPLICAS,
                        "in-sync replicas " + isrSize + " below min.insync.replicas " + floor + " for " + req.getTopic()
                                + "-" + req.getPartition() + "; rejected before append");
            }
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

        long startNs = System.nanoTime();
        // Stamped into the batch header: the log's lineage marker.
        // Followers derive their leader-epoch checkpoints from it, which is
        // what makes divergent tails detectable on replica fetch.
        int partitionLeaderEpoch = state.get().leaderEpoch();
        boolean idempotent = req.getProducerId() > 0;
        if (!idempotent) {
            var appended = appendOnly(req, parsed, partitionLeaderEpoch);
            if (appended.error != null) return appended.error;
            return finishProduce(req, appended.first, appended.last, startNs);
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
        //
        // Only the APPEND runs under the dedup lock. The acks=all wait
        // happens after, for fresh and cached results alike, because dedup
        // state must reflect the LOG, not the ack: an append that landed
        // but failed its replication wait (floor rejection, HWM timeout)
        // is on disk, and a retry that re-appended it would fill the log
        // with duplicate batches — soak v5 hit exactly that, eight copies
        // of one record at consecutive offsets seeding permanent replica
        // divergence. The retry instead dedups to the cached offsets and
        // re-runs the replication wait on them.
        var key = new ProducerStateManager.DedupKey(
                req.getTopic(), req.getPartition(), req.getProducerId(), req.getProducerEpoch());
        var appendHolder = new AppendHolder();
        var result = producerState.dedupOrAppend(
                key, req.getBaseSequence(), parsed.records().size(), () -> {
                    var appended = appendOnly(req, parsed, partitionLeaderEpoch);
                    appendHolder.error = appended.error;
                    if (appended.error != null) {
                        // Nothing on disk (corrupt batch / IO / fenced
                        // epoch) — state must not advance.
                        return null;
                    }
                    return new long[] {appended.first, appended.last};
                });
        if (result.hasError()) {
            // Out-of-order sequence, or append failure. Either way:
            // surface the error from the append side when present, else
            // OUT_OF_ORDER from the dedup check.
            if (appendHolder.error != null) {
                return appendHolder.error;
            }
            return err(ErrorCodes.OUT_OF_ORDER_SEQUENCE, result.errorMessage());
        }
        // Fresh append or cached retry: identical from here. The cached
        // path MUST NOT short-circuit acks=all — the original attempt may
        // have failed its replication wait, so "I have seen this batch"
        // does not imply "this batch is committed."
        return finishProduce(req, result.baseOffset(), result.lastOffset(), startNs);
    }

    /** Capture slot so the Appender lambda can hand its append error back to the caller. */
    private static final class AppendHolder {
        ProduceResponse error;
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

    /** Outcome of the bare append: offsets on success, an error response otherwise. */
    private record Appended(long first, long last, ProduceResponse error) {
        static Appended ok(long first, long last) {
            return new Appended(first, last, null);
        }

        static Appended failed(ProduceResponse error) {
            return new Appended(-1L, -1L, error);
        }
    }

    /**
     * Append the batch to the local log — nothing else. The acks=all
     * replication wait deliberately lives in {@link #finishProduce}, outside
     * the idempotent-dedup lock, so that dedup state always reflects what is
     * on disk regardless of how the ack turns out.
     */
    private Appended appendOnly(ProduceRequest req, RecordBatch.Parsed parsed, int partitionLeaderEpoch) {
        try {
            var log = logManager.logFor(req.getTopic(), req.getPartition());
            long now = System.currentTimeMillis();
            // Audit-finding #1 — preserve producer id / epoch / baseSequence in
            // the on-disk batch so followers replicating this batch can observe
            // the dedup state (and a leader rebooting from its own log can
            // rebuild state without help).
            long last = log.append(
                    parsed.records(),
                    now,
                    req.getProducerId(),
                    (short) req.getProducerEpoch(),
                    req.getBaseSequence(),
                    partitionLeaderEpoch);
            long first = last - (parsed.records().size() - 1);
            return Appended.ok(first, last);
        } catch (IllegalArgumentException | IOException e) {
            return Appended.failed(
                    err(ErrorCodes.CORRUPT_BATCH, e.getMessage() == null ? e.toString() : e.getMessage()));
        }
    }

    /**
     * Complete a produce whose batch is known to sit at {@code [first, last]}
     * in the log (freshly appended or dedup-cached): run the acks=all
     * replication wait if requested, then record metrics and build the
     * success response.
     */
    private ProduceResponse finishProduce(ProduceRequest req, long first, long last, long startNs) {
        if (req.getAcks() == ACKS_ALL) {
            ProduceResponse wait;
            try {
                wait = waitForIsrReplication(req.getTopic(), req.getPartition(), last);
            } catch (IOException e) {
                return err(ErrorCodes.IO_ERROR, e.getMessage() == null ? e.toString() : e.getMessage());
            }
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
    }

    /**
     * Poll until the partition's HWM advances past {@code producedLastOffset}
     * with the ISR still at or above the {@code min.insync.replicas} floor,
     * bounded by {@link #ACKS_ALL_TIMEOUT_MS}. Returns {@code null} on
     * success; on timeout, leadership loss, or an ISR that drops below the
     * floor, returns an error response.
     *
     * <p>The floor re-check inside the loop is load-bearing: an ISR shrink
     * to just the leader makes {@code computeHwm} jump to the leader's LEO
     * (min over a one-element set), which is exactly the false-ack
     * mechanism behind #115. HWM advance therefore only counts when the
     * ISR that produced it is still floor-sized. The rejection is
     * conservative — the batch may in fact sit on enough replicas when the
     * shrink raced the ack — so the error stays retriable and idempotent
     * producers dedup the retry.
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
            // Recomputed per iteration: a concurrent UpdateTopicConfig can
            // change the floor mid-wait. Topic deleted mid-wait → floor 1;
            // the partition-state check above resolves the request first.
            int floor = topicManager
                    .describe(topic)
                    .map(t -> t.effectiveMinInsyncReplicas(minInsyncReplicas))
                    .orElse(1);
            int isrSize = state.get().isr().size();
            long leaderLeo = logManager.logFor(topic, partition).nextOffset();
            long hwm = followerTracker.computeHwm(topic, partition, state.get().isr(), selfBrokerId, leaderLeo);
            // HWM convention: the first offset NOT yet durably replicated.
            // producedLastOffset is the highest offset in the produced batch,
            // so acks=all is satisfied when HWM > producedLastOffset.
            if (hwm > producedLastOffset) {
                if (isrSize >= floor) return null;
                return err(
                        ErrorCodes.NOT_ENOUGH_REPLICAS,
                        "ISR shrank to " + isrSize + " (below min.insync.replicas " + floor + ") after append for "
                                + topic + "-" + partition + "; not acked — retry the produce");
            }
            if (isrSize < floor) {
                return err(
                        ErrorCodes.NOT_ENOUGH_REPLICAS,
                        "in-sync replicas " + isrSize + " below min.insync.replicas " + floor + " after append for "
                                + topic + "-" + partition + "; not acked — retry the produce");
            }
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
