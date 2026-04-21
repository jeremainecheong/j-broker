package jbroker.broker.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import jbroker.broker.PartitionState;
import jbroker.broker.TopicManager;
import jbroker.proto.raft.MetadataRecord;
import jbroker.proto.raft.PartitionChangeRecord;
import jbroker.storage.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodic housekeeper for each leader-owned partition's ISR. Shrinks the
 * ISR when a follower's last-fetch timestamp exceeds {@code lagTimeoutMs};
 * expands it when an out-of-ISR replica's LEO catches up to the partition
 * HWM.
 *
 * <p>The decision logic lives in {@link #decideChanges} as a pure function
 * of {@code (TopicManager, LogManager, FollowerStateTracker, now)} so it
 * can be unit-tested without a running Raft cluster. The caller wires the
 * proposal bytes into a {@code MetadataProposer}.
 */
public final class IsrManager {

    private static final Logger log = LoggerFactory.getLogger(IsrManager.class);

    private final int selfBrokerId;
    private final TopicManager topicManager;
    private final LogManager logManager;
    private final FollowerStateTracker tracker;
    private final long lagTimeoutMs;

    public IsrManager(
            int selfBrokerId,
            TopicManager topicManager,
            LogManager logManager,
            FollowerStateTracker tracker,
            long lagTimeoutMs) {
        this.selfBrokerId = selfBrokerId;
        this.topicManager = topicManager;
        this.logManager = logManager;
        this.tracker = tracker;
        this.lagTimeoutMs = lagTimeoutMs;
    }

    /**
     * Inspect every known partition and return the list of serialized
     * {@link MetadataRecord}s the caller should propose to flip the ISR.
     * Returns empty when no change is warranted.
     */
    public List<byte[]> decideChanges(long nowMillis) {
        var out = new ArrayList<byte[]>();
        for (var topic : topicManager.list()) {
            for (int p = 0; p < topic.partitions(); p++) {
                var state = topicManager.partitionState(topic.topic(), p).orElse(null);
                if (state == null) continue;
                if (state.leader() != selfBrokerId) continue;
                var proposal = decideForPartition(topic.topic(), p, state, nowMillis);
                if (proposal != null) out.add(proposal);
            }
        }
        return out;
    }

    private byte[] decideForPartition(String topic, int partition, PartitionState state, long nowMillis) {
        long leaderLeo;
        try {
            leaderLeo = logManager.logFor(topic, partition).nextOffset();
        } catch (IOException e) {
            log.warn("unable to read leader LEO for {}-{}: {}", topic, partition, e.toString());
            return null;
        }
        long hwm = tracker.computeHwm(topic, partition, state.isr(), selfBrokerId, leaderLeo);

        // Shrink: ISR members other than self whose last fetch is stale.
        var nonLeaderIsr = new ArrayList<Integer>();
        for (int b : state.isr()) {
            if (b != selfBrokerId) nonLeaderIsr.add(b);
        }
        var laggards = tracker.laggardsOf(topic, partition, nonLeaderIsr, nowMillis, lagTimeoutMs);

        // Expand: replicas outside ISR whose LEO has caught up to HWM.
        var catchupCandidates = new ArrayList<Integer>();
        for (int b : state.replicas()) {
            if (b == selfBrokerId || state.isr().contains(b)) continue;
            var m = tracker.get(topic, partition, b);
            if (m.isPresent() && m.get().leo() >= hwm) {
                catchupCandidates.add(b);
            }
        }

        if (laggards.isEmpty() && catchupCandidates.isEmpty()) return null;

        var nextIsr = new ArrayList<Integer>(state.isr());
        nextIsr.removeAll(laggards);
        nextIsr.addAll(catchupCandidates);

        // Invariant: ISR must always contain the leader. If we'd shrink the
        // leader out, skip the proposal — that's a data-loss footgun the
        // controller-driven failover (P6.5) will resolve.
        if (!nextIsr.contains(selfBrokerId) || nextIsr.isEmpty()) return null;

        // ISR-only flip: keep leader_epoch so followers don't trigger
        // P6.4's OffsetsForLeaderEpoch reconciliation cycle; bump
        // partition_epoch so every follower sees it as newer metadata.
        var change = PartitionChangeRecord.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setLeader(selfBrokerId)
                .addAllIsr(nextIsr)
                .addAllReplicas(state.replicas())
                .setLeaderEpoch(state.leaderEpoch())
                .setPartitionEpoch(state.partitionEpoch() + 1)
                .build();
        return MetadataRecord.newBuilder().setPartitionChange(change).build().toByteArray();
    }
}
