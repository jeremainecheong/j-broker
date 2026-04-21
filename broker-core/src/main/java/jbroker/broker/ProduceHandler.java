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
        // Read (leader, epoch) atomically via partitionState so the two can't
        // straddle a concurrent PartitionChangeRecord apply. Note: this check
        // is still not sufficient on its own — a leadership change can commit
        // *between* this snapshot and the log.append() below, so a deposed
        // leader could still write one batch locally. P6.2+ closes that
        // window by passing the observed leader epoch down to Log.append,
        // which will reject if the log's recorded epoch has advanced.
        var state = topicManager.partitionState(req.getTopic(), req.getPartition());
        if (state.isEmpty()) {
            return ProduceResponse.newBuilder()
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.NOT_LEADER)
                            .setMessage("no leader for partition " + req.getTopic() + "-" + req.getPartition())
                            .build())
                    .build();
        }
        if (state.get().leader() != selfBrokerId) {
            return ProduceResponse.newBuilder()
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.NOT_LEADER)
                            .setMessage("leader is broker " + state.get().leader() + " for " + req.getTopic() + "-"
                                    + req.getPartition())
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
