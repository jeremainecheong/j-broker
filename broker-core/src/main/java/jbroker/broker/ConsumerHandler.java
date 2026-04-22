package jbroker.broker;

import java.io.IOException;
import jbroker.proto.broker.CommitOffsetsRequest;
import jbroker.proto.broker.CommitOffsetsResponse;
import jbroker.proto.broker.CommitResult;
import jbroker.proto.broker.ConsumerGroupHeartbeatRequest;
import jbroker.proto.broker.ConsumerGroupHeartbeatResponse;
import jbroker.proto.broker.FetchOffsetsRequest;
import jbroker.proto.broker.FetchOffsetsResponse;
import jbroker.proto.broker.FindCoordinatorRequest;
import jbroker.proto.broker.FindCoordinatorResponse;
import jbroker.proto.broker.ListOffsetsRequest;
import jbroker.proto.broker.ListOffsetsResponse;
import jbroker.proto.broker.ListOffsetsResult;
import jbroker.proto.broker.OffsetFetchResult;
import jbroker.proto.common.ErrorCode;
import jbroker.storage.LogManager;

/**
 * Routes the Phase 7 consumer-group RPCs ({@code FindCoordinator},
 * {@code ConsumerGroupHeartbeat}, {@code CommitOffsets},
 * {@code FetchOffsets}, {@code ListOffsets}).
 *
 * <p>P7.1 wires the surface only — coordinator-routed RPCs return
 * {@link ErrorCode#COORDINATOR_NOT_AVAILABLE} until the
 * {@code GroupCoordinator} lands in P7.4 / P7.5. {@code FetchOffsets}
 * returns {@link ErrorCode#OFFSET_OUT_OF_RANGE} until P7.6 plugs in the
 * persisted offset cache.
 *
 * <p>{@code ListOffsets} IS implemented in P7.1 — it just delegates to
 * {@link LogManager} and returns the partition's {@code nextOffset()} for
 * the latest sentinel ({@code timestamp == -1}), {@code logStartOffset()}
 * (currently always 0) for the earliest sentinel ({@code timestamp == -2}),
 * and ignores by-timestamp lookups for now (returns latest as a safe
 * upper bound — refined in a later milestone when the time index gains
 * a public lookup API).
 */
public final class ConsumerHandler {

    private final TopicManager topicManager;
    private final LogManager logManager;

    @SuppressWarnings("unused") // wired now so P7.3 can use it without re-touching Broker.start
    private final BrokerRegistry brokerRegistry;

    public ConsumerHandler(TopicManager topicManager, LogManager logManager, BrokerRegistry brokerRegistry) {
        this.topicManager = topicManager;
        this.logManager = logManager;
        this.brokerRegistry = brokerRegistry;
    }

    public FindCoordinatorResponse findCoordinator(FindCoordinatorRequest req) {
        return FindCoordinatorResponse.newBuilder()
                .setError(ErrorCode.COORDINATOR_NOT_AVAILABLE)
                .build();
    }

    public ConsumerGroupHeartbeatResponse consumerGroupHeartbeat(ConsumerGroupHeartbeatRequest req) {
        return ConsumerGroupHeartbeatResponse.newBuilder()
                .setError(ErrorCode.COORDINATOR_NOT_AVAILABLE)
                .build();
    }

    public CommitOffsetsResponse commitOffsets(CommitOffsetsRequest req) {
        var b = CommitOffsetsResponse.newBuilder();
        for (var commit : req.getCommitsList()) {
            b.addResults(CommitResult.newBuilder()
                    .setTp(commit.getTp())
                    .setError(ErrorCode.COORDINATOR_NOT_AVAILABLE)
                    .build());
        }
        return b.build();
    }

    public FetchOffsetsResponse fetchOffsets(FetchOffsetsRequest req) {
        var b = FetchOffsetsResponse.newBuilder();
        for (var tp : req.getTpsList()) {
            b.addResults(OffsetFetchResult.newBuilder()
                    .setTp(tp)
                    .setOffset(-1L)
                    .setError(ErrorCode.OFFSET_OUT_OF_RANGE)
                    .build());
        }
        return b.build();
    }

    public ListOffsetsResponse listOffsets(ListOffsetsRequest req) {
        var b = ListOffsetsResponse.newBuilder();
        for (var part : req.getPartitionsList()) {
            var tp = part.getTp();
            var topic = topicManager.describe(tp.getTopic());
            if (topic.isEmpty()) {
                b.addResults(ListOffsetsResult.newBuilder()
                        .setTp(tp)
                        .setError(ErrorCode.UNKNOWN)
                        .build());
                continue;
            }
            if (tp.getPartition() < 0 || tp.getPartition() >= topic.get().partitions()) {
                b.addResults(ListOffsetsResult.newBuilder()
                        .setTp(tp)
                        .setError(ErrorCode.UNKNOWN)
                        .build());
                continue;
            }
            try {
                long offset;
                long timestamp = part.getTimestamp();
                if (timestamp == -2) {
                    // Earliest. Log compaction is a Phase 9+ concern, so the
                    // log start offset is always 0 today.
                    offset = 0L;
                } else {
                    // -1 (latest) and any positive ts both fall back to LEO
                    // for now. Refining by-ts lookups requires a public
                    // search API on TimeIndex — left for a follow-up slice
                    // when a real client needs it.
                    offset = logManager.logFor(tp.getTopic(), tp.getPartition()).nextOffset();
                }
                b.addResults(ListOffsetsResult.newBuilder()
                        .setTp(tp)
                        .setError(ErrorCode.OK)
                        .setOffset(offset)
                        .setTimestamp(timestamp)
                        .build());
            } catch (IOException e) {
                b.addResults(ListOffsetsResult.newBuilder()
                        .setTp(tp)
                        .setError(ErrorCode.UNKNOWN)
                        .build());
            }
        }
        return b.build();
    }
}
