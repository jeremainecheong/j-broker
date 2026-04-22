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
 * <p>adds incremental fetch sessions: requests arriving with
 * {@code session_id=0} get a fresh id allocated and returned in the
 * response; requests echoing a previously-allocated id update their cached
 * per-(topic, partition) state and increment the
 * {@link BrokerMetrics#incrementalFetchHits()} counter. A request that
 * carries a session id we've since evicted gets
 * {@code FETCH_SESSION_ID_NOT_FOUND} so the client can reset to 0 and retry.
 */
public final class FetchHandler {

    private final LogManager logManager;
    private final TopicManager topicManager;
    private final FetchSessionCache sessionCache;
    private final BrokerMetrics metrics;

    public FetchHandler(LogManager logManager, TopicManager topicManager) {
        this(logManager, topicManager, new FetchSessionCache(), new BrokerMetrics());
    }

    public FetchHandler(
            LogManager logManager, TopicManager topicManager, FetchSessionCache sessionCache, BrokerMetrics metrics) {
        this.logManager = logManager;
        this.topicManager = topicManager;
        this.sessionCache = sessionCache;
        this.metrics = metrics;
    }

    public FetchResponse handle(FetchRequest req) {
        long startNs = System.nanoTime();
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
            long hwm = log.nextOffset();
            var tp = TopicPartition.newBuilder()
                    .setTopic(req.getTopic())
                    .setPartition(req.getPartition())
                    .build();
            sessionCache.update(sessionId, tp, req.getOffset(), /*leaderEpoch*/ 0);
            byte[] bytes = baos.toByteArray();
            metrics.recordFetch(System.nanoTime() - startNs, bytes.length);
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
