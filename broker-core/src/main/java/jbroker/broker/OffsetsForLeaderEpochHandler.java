package jbroker.broker;

import java.io.IOException;
import jbroker.proto.broker.OffsetsForLeaderEpochRequest;
import jbroker.proto.broker.OffsetsForLeaderEpochResponse;
import jbroker.storage.LogManager;

/**
 * Answers the follower-reconciliation RPC (P6.4): given a follower's
 * last-seen {@code leader_epoch}, returns the end offset of that epoch in
 * the leader's log. The follower compares the response against its local
 * LEO to decide whether to truncate before resuming {@code ReplicaFetch}.
 *
 * <p>Semantics:
 * <ul>
 *   <li>If the requested epoch is the latest recorded epoch, return the
 *       current {@code log.nextOffset()}.</li>
 *   <li>Otherwise return the {@code startOffset} of the next recorded
 *       epoch — the requested epoch ended there.</li>
 *   <li>If the checkpoint has no record of the requested epoch (or any
 *       epoch at all), return current LEO as a safe default.</li>
 * </ul>
 */
public final class OffsetsForLeaderEpochHandler {

    private final LogManager logManager;
    private final TopicManager topicManager;
    private final int selfBrokerId;

    public OffsetsForLeaderEpochHandler(LogManager logManager, TopicManager topicManager, int selfBrokerId) {
        this.logManager = logManager;
        this.topicManager = topicManager;
        this.selfBrokerId = selfBrokerId;
    }

    public OffsetsForLeaderEpochResponse handle(OffsetsForLeaderEpochRequest req) {
        var state = topicManager.partitionState(req.getTopic(), req.getPartition());
        if (state.isEmpty() || state.get().leader() != selfBrokerId) {
            return OffsetsForLeaderEpochResponse.newBuilder()
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.NOT_LEADER)
                            .setMessage("not leader for " + req.getTopic() + "-" + req.getPartition())
                            .build())
                    .build();
        }
        try {
            var log = logManager.logFor(req.getTopic(), req.getPartition());
            long leo = log.nextOffset();
            var cp = logManager.leaderEpochCheckpoint(req.getTopic(), req.getPartition());
            var entries = cp.entries();
            long endOffset = leo;
            for (int i = 0; i < entries.size(); i++) {
                if (entries.get(i).epoch() == req.getLeaderEpoch()) {
                    endOffset = i + 1 < entries.size() ? entries.get(i + 1).startOffset() : leo;
                    break;
                }
            }
            return OffsetsForLeaderEpochResponse.newBuilder()
                    .setEndOffset(endOffset)
                    .build();
        } catch (IOException e) {
            return OffsetsForLeaderEpochResponse.newBuilder()
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.IO_ERROR)
                            .setMessage(e.toString())
                            .build())
                    .build();
        }
    }
}
