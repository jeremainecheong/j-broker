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
 * <p>Phase 6.1 adds a leader check: the handler rejects with
 * {@link ErrorCodes#NOT_LEADER} if the partition has no leader yet or if
 * the leader isn't this broker. Replication itself arrives in later slices.
 */
public final class ProduceHandler {

    private final LogManager logManager;
    private final TopicManager topicManager;
    private final int selfBrokerId;

    public ProduceHandler(LogManager logManager, TopicManager topicManager, int selfBrokerId) {
        this.logManager = logManager;
        this.topicManager = topicManager;
        this.selfBrokerId = selfBrokerId;
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
        var leader = topicManager.partitionLeader(req.getTopic(), req.getPartition());
        if (leader.isEmpty()) {
            return ProduceResponse.newBuilder()
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.NOT_LEADER)
                            .setMessage("no leader for partition " + req.getTopic() + "-" + req.getPartition())
                            .build())
                    .build();
        }
        if (leader.get() != selfBrokerId) {
            return ProduceResponse.newBuilder()
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.NOT_LEADER)
                            .setMessage("leader is broker " + leader.get())
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
