package jbroker.broker;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import jbroker.proto.broker.ProduceRequest;
import jbroker.proto.broker.ProduceResponse;
import jbroker.storage.LogManager;
import jbroker.storage.RecordBatch;

/**
 * Handles {@code Produce} RPCs: decodes the inbound record-batch bytes,
 * re-encodes them with the assigned {@code baseOffset} from the target
 * partition's {@link jbroker.storage.Log}, and appends.
 *
 * <p>Phase 5 is single-node: no replication, no leader check, no ISR. The
 * broker blindly accepts any {@code (topic, partition)} for which the topic
 * exists in {@link TopicManager}.
 */
public final class ProduceHandler {

    private final LogManager logManager;
    private final TopicManager topicManager;

    public ProduceHandler(LogManager logManager, TopicManager topicManager) {
        this.logManager = logManager;
        this.topicManager = topicManager;
    }

    public ProduceResponse handle(ProduceRequest req) {
        var topic = topicManager.describe(req.getTopic());
        if (topic.isEmpty()) {
            return ProduceResponse.newBuilder()
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.UNKNOWN_TOPIC)
                            .setMessage("unknown topic: " + req.getTopic())
                            .build())
                    .build();
        }
        if (req.getPartition() < 0 || req.getPartition() >= topic.get().partitions()) {
            return ProduceResponse.newBuilder()
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.INVALID_PARTITION)
                            .setMessage("invalid partition: " + req.getPartition())
                            .build())
                    .build();
        }
        try {
            var incoming = ByteBuffer.wrap(req.getBatch().toByteArray());
            var parsed = RecordBatch.decode(incoming);
            var log = logManager.logFor(req.getTopic(), req.getPartition());
            long now = System.currentTimeMillis();
            long last = log.append(parsed.records(), now);
            long first = last - (parsed.records().size() - 1);
            return ProduceResponse.newBuilder()
                    .setBaseOffset(first)
                    .setLastOffset(last)
                    .build();
        } catch (IllegalArgumentException | IOException e) {
            return ProduceResponse.newBuilder()
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.CORRUPT_BATCH)
                            .setMessage(e.getMessage() == null ? e.toString() : e.getMessage())
                            .build())
                    .build();
        }
    }

    @SuppressWarnings("unused")
    private static ByteString empty() {
        return ByteString.EMPTY;
    }
}
