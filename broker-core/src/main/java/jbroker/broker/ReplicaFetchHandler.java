package jbroker.broker;

import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.function.LongSupplier;
import jbroker.broker.replication.FollowerStateTracker;
import jbroker.proto.broker.ReplicaFetchRequest;
import jbroker.proto.broker.ReplicaFetchResponse;
import jbroker.storage.LogManager;

/**
 * Serves the broker-internal {@code ReplicaFetch} RPC: a follower pulls
 * record batches from the partition leader. Validates {@code leader_epoch}
 * and ISR membership, records the follower's LEO in the shared
 * {@link FollowerStateTracker}, then returns records + advanced HWM.
 *
 * <p>HWM is computed as {@code min(LEO across ISR)}; the follower uses it
 * to know what's safe for consumers (Phase 6.6 enforces this on the
 * Consumer.Fetch path).
 */
public final class ReplicaFetchHandler {

    /** P9.2 — minimum lag (records) for ReplicationLagEvent emission. */
    private static final long LAG_EVENT_THRESHOLD_RECORDS = 10L;

    private final LogManager logManager;
    private final TopicManager topicManager;
    private final int selfBrokerId;
    private final FollowerStateTracker tracker;
    private final LongSupplier clock;

    public ReplicaFetchHandler(
            LogManager logManager,
            TopicManager topicManager,
            int selfBrokerId,
            FollowerStateTracker tracker,
            LongSupplier clock) {
        this.logManager = logManager;
        this.topicManager = topicManager;
        this.selfBrokerId = selfBrokerId;
        this.tracker = tracker;
        this.clock = clock;
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
        // Reject impostor brokers not in the replica set. Out-of-ISR
        // replicas still pass — they need to fetch in order to catch up
        // and have the IsrManager expand ISR back to them.
        if (!state.get().replicas().contains(req.getFollowerBrokerId())) {
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
        // Record the follower's LEO — the fetch_offset is the first offset
        // it still needs, which equals its local LEO. Only recorded for
        // accepted fetches so a fenced / impostor peer can't drag HWM down.
        tracker.record(
                req.getTopic(), req.getPartition(), req.getFollowerBrokerId(), req.getFetchOffset(), clock.getAsLong());
        try {
            var log = logManager.logFor(req.getTopic(), req.getPartition());
            long leaderLeo = log.nextOffset();
            var baos = new ByteArrayOutputStream();
            if (req.getFetchOffset() < leaderLeo) {
                int maxBytes = req.getMaxBytes() > 0 ? req.getMaxBytes() : 1024 * 1024;
                log.transferTo(req.getFetchOffset(), maxBytes, baos);
            }
            long hwm = tracker.computeHwm(
                    req.getTopic(), req.getPartition(), state.get().isr(), selfBrokerId, leaderLeo);
            // P9.2 — emit ReplicationLag when the follower is meaningfully
            // behind the leader. Leader-side because only the leader has the
            // authoritative leaderLeo; suppressed below a small threshold to
            // keep JFR volume manageable at steady state.
            long lag = Math.max(0L, leaderLeo - req.getFetchOffset());
            if (lag >= LAG_EVENT_THRESHOLD_RECORDS) {
                var jfr = new jbroker.broker.jfr.ReplicationLagEvent();
                if (jfr.shouldCommit()) {
                    jfr.topic = req.getTopic();
                    jfr.partition = req.getPartition();
                    jfr.followerBrokerId = req.getFollowerBrokerId();
                    jfr.lagRecords = lag;
                    jfr.commit();
                }
            }
            return ReplicaFetchResponse.newBuilder()
                    .setRecords(ByteString.copyFrom(baos.toByteArray()))
                    .setHighWatermark(hwm)
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
