package jbroker.broker;

import java.io.IOException;
import jbroker.proto.broker.OffsetsForLeaderEpochRequest;
import jbroker.proto.broker.OffsetsForLeaderEpochResponse;
import jbroker.storage.LogManager;

/**
 * Answers the follower-reconciliation RPC (): given a follower's
 * last-seen {@code leader_epoch}, returns the end offset of that epoch in
 * the leader's log. The follower compares the response against its local
 * LEO to decide whether to truncate before resuming {@code ReplicaFetch}.
 *
 * <p>Kafka-style semantics — descending scan for the largest recorded
 * epoch {@code <=} the requested one:
 * <ul>
 *   <li>If that recorded epoch has a successor in the checkpoint, return
 *       the successor's {@code startOffset} — the requested epoch ended
 *       there.</li>
 *   <li>If that recorded epoch is the latest, return the current
 *       {@code log.nextOffset()} (the leader has not yet closed the
 *       requested epoch).</li>
 *   <li>If no recorded epoch is {@code <=} the requested one — i.e. the
 *       checkpoint is empty, or the follower is asking about an epoch
 *       older than anything the leader remembers — return
 *       {@link #UNDEFINED_EPOCH_OFFSET}. The follower truncates to zero.</li>
 * </ul>
 */
public final class OffsetsForLeaderEpochHandler {

    /**
     * Sentinel returned when the requested epoch is older than anything the
     * leader's checkpoint remembers (or the checkpoint is empty). The
     * follower treats this as "truncate to zero and rebuild from LEO 0".
     */
    public static final long UNDEFINED_EPOCH_OFFSET = -1L;

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
            int floor = -1;
            for (int i = entries.size() - 1; i >= 0; i--) {
                if (entries.get(i).epoch() <= req.getLeaderEpoch()) {
                    floor = i;
                    break;
                }
            }
            long endOffset;
            if (floor < 0) {
                endOffset = UNDEFINED_EPOCH_OFFSET;
            } else if (floor + 1 < entries.size()) {
                endOffset = entries.get(floor + 1).startOffset();
            } else {
                endOffset = leo;
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
