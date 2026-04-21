package jbroker.broker;

import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import jbroker.proto.broker.FetchRequest;
import jbroker.proto.broker.FetchResponse;
import jbroker.storage.LogManager;

/**
 * Handles {@code Fetch} RPCs: looks up the partition's log and streams up to
 * {@code maxBytes} of raw batch bytes starting at {@code offset}. Uses
 * {@code FileChannel.transferTo} under the hood for zero-copy IO.
 */
public final class FetchHandler {

    private final LogManager logManager;
    private final TopicManager topicManager;

    public FetchHandler(LogManager logManager, TopicManager topicManager) {
        this.logManager = logManager;
        this.topicManager = topicManager;
    }

    public FetchResponse handle(FetchRequest req) {
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
        try {
            var log = logManager.logFor(req.getTopic(), req.getPartition());
            var baos = new ByteArrayOutputStream();
            int maxBytes = req.getMaxBytes() > 0 ? req.getMaxBytes() : 64 * 1024;
            log.transferTo(req.getOffset(), maxBytes, baos);
            long hwm = log.nextOffset();
            return FetchResponse.newBuilder()
                    .setRecords(ByteString.copyFrom(baos.toByteArray()))
                    .setHighWatermark(hwm)
                    .build();
        } catch (IOException e) {
            return FetchResponse.newBuilder()
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.IO_ERROR)
                            .setMessage(e.getMessage() == null ? e.toString() : e.getMessage())
                            .build())
                    .build();
        }
    }
}
