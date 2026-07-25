package jbroker.broker;

import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import jbroker.proto.broker.FetchRequest;
import jbroker.proto.broker.FetchResponse;
import jbroker.proto.common.TopicPartition;
import jbroker.storage.LogManager;

/**
 * Handles {@code Fetch} RPCs: looks up the partition's log and streams up to
 * {@code maxBytes} of raw batch bytes starting at {@code offset}. Uses
 * {@code FileChannel.transferTo} under the hood for zero-copy IO.
 *
 * <p>Isolation levels: {@code read_uncommitted} (the default) serves the
 * transferred bytes as-is. {@code read_committed} caps the read at the
 * partition's {@linkplain jbroker.storage.Log#lastStableOffset(long) last
 * stable offset} — batches not wholly below it are cut from the response by
 * a plaintext-header frame scan (baseOffset / batchLength /
 * lastOffsetDelta; no record decode) — and attaches the aborted-transaction
 * ranges overlapping the returned window plus the LSO itself.
 *
 * <p>Control-batch contract: the zero-copy transfer ships transaction
 * markers (control batches) inline with the data in BOTH isolation levels —
 * stripping them would force a decode/re-encode of every fetched byte.
 * Clients MUST skip batches carrying the control attribute bit and never
 * surface their records to the application; under read_committed the ABORT
 * markers in the stream terminate the attached aborted ranges (Kafka's
 * consumer follows the same protocol). The LSO cap is computed after the
 * transfer, which is safe: the LSO only advances, a batch it newly admits
 * was already durable in the transferred bytes, and if the deciding marker
 * was an abort the range is present in the aborted list computed in the
 * same pass.
 *
 * <p>Incremental fetch sessions: requests arriving with
 * {@code session_id=0} get a fresh id allocated and returned in the
 * response; requests echoing a previously-allocated id update their cached
 * per-(topic, partition) state and increment the
 * {@link BrokerMetrics#incrementalFetchHits()} counter. A request that
 * carries a session id we've since evicted gets
 * {@code FETCH_SESSION_ID_NOT_FOUND} so the client can reset to 0 and retry.
 *
 * <p>Byte-rate quotas: when a {@link jbroker.broker.quota.QuotaEnforcer}
 * is wired, every served fetch is charged against the principal's FETCH
 * budget and an over-budget request is refused with retriable
 * {@code QUOTA_VIOLATED} carrying a back-off hint — the same shape the
 * produce deny path uses. Only client fetches pass through here:
 * follower replication rides the separate {@code ReplicaFetch} RPC
 * ({@link ReplicaFetchHandler}), which has no quota gate, so a fetch
 * quota can never throttle intra-cluster replication or starve the ISR.
 */
public final class FetchHandler {

    private final LogManager logManager;
    private final TopicManager topicManager;
    private final FetchSessionCache sessionCache;
    private final BrokerMetrics metrics;
    private final jbroker.broker.quota.QuotaEnforcer quotaEnforcer;

    /** ACL gate, wired by the broker; {@link jbroker.broker.auth.Authorizer#OPEN} keeps test harnesses open. */
    private jbroker.broker.auth.Authorizer authorizer = jbroker.broker.auth.Authorizer.OPEN;

    public void setAuthorizer(jbroker.broker.auth.Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    public FetchHandler(LogManager logManager, TopicManager topicManager) {
        this(logManager, topicManager, new FetchSessionCache(), new BrokerMetrics());
    }

    public FetchHandler(
            LogManager logManager, TopicManager topicManager, FetchSessionCache sessionCache, BrokerMetrics metrics) {
        this(logManager, topicManager, sessionCache, metrics, jbroker.broker.quota.QuotaEnforcer.NOOP);
    }

    /** Constructor with a {@link jbroker.broker.quota.QuotaEnforcer}. */
    public FetchHandler(
            LogManager logManager,
            TopicManager topicManager,
            FetchSessionCache sessionCache,
            BrokerMetrics metrics,
            jbroker.broker.quota.QuotaEnforcer quotaEnforcer) {
        this.logManager = logManager;
        this.topicManager = topicManager;
        this.sessionCache = sessionCache;
        this.metrics = metrics;
        this.quotaEnforcer = quotaEnforcer == null ? jbroker.broker.quota.QuotaEnforcer.NOOP : quotaEnforcer;
    }

    public FetchResponse handle(FetchRequest req) {
        long startNs = System.nanoTime();
        // Authorization precedes everything — an unauthorized principal
        // learns nothing about the topic, not even whether it exists.
        if (!authorizer.allowsCurrent("topic", req.getTopic(), "consume")) {
            return FetchResponse.newBuilder()
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.UNAUTHORIZED)
                            .setMessage("principal " + jbroker.broker.auth.AuthContext.principalOrAnonymous()
                                    + " is not authorized to consume from topic " + req.getTopic())
                            .build())
                    .build();
        }
        var topic = topicManager.describe(req.getTopic());
        if (topic.isEmpty()) {
            return FetchResponse.newBuilder()
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.UNKNOWN_TOPIC)
                            .setMessage("unknown topic: " + req.getTopic())
                            .build())
                    .build();
        }
        if (req.getPartition() < 0 || req.getPartition() >= topic.get().partitions()) {
            return FetchResponse.newBuilder()
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.INVALID_PARTITION)
                            .setMessage("invalid partition: " + req.getPartition())
                            .build())
                    .build();
        }

        int sessionId;
        if (req.getSessionId() == 0) {
            sessionId = sessionCache.allocate();
        } else {
            sessionId = req.getSessionId();
            if (sessionCache.get(sessionId).isEmpty()) {
                return FetchResponse.newBuilder()
                        .setError(jbroker.proto.broker.Error.newBuilder()
                                .setCode(ErrorCodes.FETCH_SESSION_ID_NOT_FOUND)
                                .setMessage("fetch session " + sessionId + " evicted or unknown")
                                .build())
                        .build();
            }
            metrics.recordIncrementalFetchHit();
        }

        try {
            var log = logManager.logFor(req.getTopic(), req.getPartition());
            var baos = new ByteArrayOutputStream();
            int maxBytes = req.getMaxBytes() > 0 ? req.getMaxBytes() : 64 * 1024;
            log.transferTo(req.getOffset(), maxBytes, baos);
            byte[] bytes = baos.toByteArray();
            boolean readCommitted = req.getIsolationLevel() == 1;
            long lastStable = 0;
            java.util.List<jbroker.storage.TransactionState.AbortedTxn> abortedTxns = java.util.List.of();
            if (readCommitted) {
                lastStable = log.lastStableOffset(log.nextOffset());
                var capped = capAtLastStable(bytes, req.getOffset(), lastStable);
                bytes = capped.bytes();
                abortedTxns = log.abortedTxnsIn(req.getOffset(), capped.endOffset());
            }
            // Admission is charged on the bytes actually read, not maxBytes,
            // so empty polls stay free and the back-off hint reflects real
            // spend. A denial returns before the session state advances —
            // the client got nothing, so its next fetch retries in place.
            var decision = quotaEnforcer.check(
                    jbroker.broker.auth.AuthContext.principalOrAnonymous(),
                    jbroker.broker.quota.QuotaEnforcer.Op.FETCH,
                    bytes.length);
            if (!decision.allow()) {
                metrics.recordQuotaDenial(jbroker.broker.quota.QuotaEnforcer.Op.FETCH, decision.throttleMillis());
                return FetchResponse.newBuilder()
                        .setError(jbroker.proto.broker.Error.newBuilder()
                                .setCode(ErrorCodes.QUOTA_VIOLATED)
                                .setMessage("fetch quota exceeded: " + decision.quotaBytesPerSec() + " B/s; retry in "
                                        + decision.throttleMillis() + "ms")
                                .build())
                        .setSessionId(sessionId)
                        .build();
            }
            long hwm = log.nextOffset();
            var tp = TopicPartition.newBuilder()
                    .setTopic(req.getTopic())
                    .setPartition(req.getPartition())
                    .build();
            sessionCache.update(sessionId, tp, req.getOffset(), /*leaderEpoch*/ 0);
            long latencyNanos = System.nanoTime() - startNs;
            metrics.recordFetch(latencyNanos, bytes.length);
            var jfr = new jbroker.broker.jfr.FetchLatencyEvent();
            if (jfr.shouldCommit()) {
                jfr.topic = req.getTopic();
                jfr.partition = req.getPartition();
                jfr.latencyNanos = latencyNanos;
                jfr.bytes = bytes.length;
                jfr.commit();
            }
            var resp = FetchResponse.newBuilder()
                    .setRecords(ByteString.copyFrom(bytes))
                    .setHighWatermark(hwm)
                    .setSessionId(sessionId);
            if (readCommitted) {
                resp.setLastStableOffset(lastStable);
                for (var txn : abortedTxns) {
                    resp.addAbortedTxns(jbroker.proto.broker.AbortedTxn.newBuilder()
                            .setProducerId(txn.producerId())
                            .setFirstOffset(txn.firstOffset())
                            .build());
                }
            }
            return resp.build();
        } catch (IOException e) {
            return FetchResponse.newBuilder()
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.IO_ERROR)
                            .setMessage(e.getMessage() == null ? e.toString() : e.getMessage())
                            .build())
                    .setSessionId(sessionId)
                    .build();
        }
    }

    /** Result of {@link #capAtLastStable}: the kept prefix and the offset one past its last batch. */
    private record CappedRead(byte[] bytes, long endOffset) {}

    /**
     * Cut the transferred bytes at the first batch that is not wholly below
     * {@code lastStable}. Walks the v2 batch frames reading only plaintext
     * header fields (baseOffset, batchLength, lastOffsetDelta) — no CRC, no
     * decompression. A partial trailing frame (the transfer hit maxBytes
     * mid-batch) is dropped too: under read_committed only complete batches
     * proven below the LSO ship, and the client simply re-fetches the tail.
     * {@code endOffset} reports one past the last kept batch's lastOffset
     * (or {@code fetchOffset} when nothing survives) — the aborted-txn
     * window bound.
     */
    private static CappedRead capAtLastStable(byte[] bytes, long fetchOffset, long lastStable) {
        var buf = java.nio.ByteBuffer.wrap(bytes);
        int pos = 0;
        long end = fetchOffset;
        while (bytes.length - pos >= jbroker.storage.RecordBatch.BATCH_OVERHEAD) {
            long baseOffset = buf.getLong(pos);
            int batchLength = buf.getInt(pos + 8);
            int frame = 12 + batchLength;
            if (frame <= 12 || pos + frame > bytes.length) break; // corrupt or partial tail
            long lastOffset = baseOffset + buf.getInt(pos + 23); // lastOffsetDelta
            if (lastOffset >= lastStable) break;
            pos += frame;
            end = lastOffset + 1;
        }
        if (pos == bytes.length) return new CappedRead(bytes, end);
        return new CappedRead(java.util.Arrays.copyOf(bytes, pos), end);
    }
}
