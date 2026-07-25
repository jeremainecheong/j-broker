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
            // Admission is charged on the bytes actually read, not maxBytes,
            // so empty polls stay free and the back-off hint reflects real
            // spend. A denial returns before the session state advances —
            // the client got nothing, so its next fetch retries in place.
            var decision = quotaEnforcer.check(
                    jbroker.broker.auth.AuthContext.principalOrAnonymous(),
                    jbroker.broker.quota.QuotaEnforcer.Op.FETCH,
                    bytes.length);
            if (!decision.allow()) {
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
            return FetchResponse.newBuilder()
                    .setRecords(ByteString.copyFrom(bytes))
                    .setHighWatermark(hwm)
                    .setSessionId(sessionId)
                    .build();
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
}
