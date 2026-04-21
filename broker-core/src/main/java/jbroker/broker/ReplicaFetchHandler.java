package jbroker.broker;

import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import jbroker.proto.broker.ReplicaFetchRequest;
import jbroker.proto.broker.ReplicaFetchResponse;
import jbroker.storage.LogManager;

/**
 * Serves the broker-internal {@code ReplicaFetch} RPC: a follower pulls
 * record batches from the partition leader. Analogous to {@link FetchHandler}
 * but additionally validates the requester's {@code leader_epoch} against
 * the leader's current view, returning {@link ErrorCodes#FENCED_EPOCH} on
 * mismatch so the follower can reconcile before retrying.
 */
public final class ReplicaFetchHandler {

    private final LogManager logManager;
    private final TopicManager topicManager;
    private final int selfBrokerId;

    public ReplicaFetchHandler(LogManager logManager, TopicManager topicManager, int selfBrokerId) {
        this.logManager = logManager;
        this.topicManager = topicManager;
        this.selfBrokerId = selfBrokerId;
    }

    public ReplicaFetchResponse handle(ReplicaFetchRequest req) {
        var state = topicManager.partitionState(req.getTopic(), req.getPartition());
        if (state.isEmpty() || state.get().leader() != selfBrokerId) {
            int currentEpoch = state.map(PartitionState::leaderEpoch).orElse(-1);
            return ReplicaFetchResponse.newBuilder()
                    .setCurrentLeaderEpoch(currentEpoch)
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.NOT_LEADER)
                            .setMessage("not leader for " + req.getTopic() + "-" + req.getPartition())
                            .build())
                    .build();
        }
        // Reject impostor brokers not in the replica set for this partition.
        // tracks only ISR (shrink/expand lands in ); until a replica
        // set separate from ISR exists, out-of-ISR brokers can't catch up.
        // That's fine for the tests (ISR=full replica set at topic create).
        if (!state.get().isr().contains(req.getFollowerBrokerId())) {
            return ReplicaFetchResponse.newBuilder()
                    .setCurrentLeaderEpoch(state.get().leaderEpoch())
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.NOT_LEADER)
                            .setMessage("broker " + req.getFollowerBrokerId() + " is not a replica of " + req.getTopic()
                                    + "-" + req.getPartition())
                            .build())
                    .build();
        }
        int currentEpoch = state.get().leaderEpoch();
        if (req.getLeaderEpoch() != currentEpoch) {
            return ReplicaFetchResponse.newBuilder()
                    .setCurrentLeaderEpoch(currentEpoch)
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.FENCED_EPOCH)
                            .setMessage("follower epoch " + req.getLeaderEpoch() + " != current " + currentEpoch)
                            .build())
                    .build();
        }
        try {
            var log = logManager.logFor(req.getTopic(), req.getPartition());
            long logEnd = log.nextOffset();
            var baos = new ByteArrayOutputStream();
            // Skip the transferTo call when the follower is caught up; avoids
            // an unnecessary index lookup and IO syscall, and keeps the
            // response deterministically empty-bytes.
            if (req.getFetchOffset() < logEnd) {
                int maxBytes = req.getMaxBytes() > 0 ? req.getMaxBytes() : 1024 * 1024;
                log.transferTo(req.getFetchOffset(), maxBytes, baos);
            }
            return ReplicaFetchResponse.newBuilder()
                    .setRecords(ByteString.copyFrom(baos.toByteArray()))
                    .setHighWatermark(logEnd)
                    .setCurrentLeaderEpoch(currentEpoch)
                    .build();
        } catch (IOException e) {
            return ReplicaFetchResponse.newBuilder()
                    .setCurrentLeaderEpoch(currentEpoch)
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.IO_ERROR)
                            .setMessage(e.toString())
                            .build())
                    .build();
        }
    }
}
