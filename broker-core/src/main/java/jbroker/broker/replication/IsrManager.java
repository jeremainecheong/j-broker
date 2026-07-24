package jbroker.broker.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * <p>A follower with no fetch record at all gets a first-fetch grace of
 * {@code lagTimeoutMs} before it can be shrunk: the tracker is local to
 * this broker, so right after a leader promotion (or leader restart) every
 * follower looks never-heard-from even when it is healthy and about to
 * re-point its fetcher. The grace clock starts when this leader first
 * observes the missing record; a follower WITH a record keeps the plain
 * staleness check.
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

    private record FollowerKey(String topic, int partition, int brokerId) {}

    /** When this leader first saw an ISR member with no fetch record; anchors the first-fetch grace. */
    private final Map<FollowerKey, Long> firstSeenWithoutRecord = new HashMap<>();

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
        // listAll, not list: internal topics (__consumer_offsets) need ISR
        // housekeeping like any other — list() filters them for the admin
        // surface, and using it here left their ISR frozen at creation:
        // laggards never shrank out, and a replica added by reassignment
        // could never join, wedging any drain that touched them.
        for (var topic : topicManager.listAll()) {
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

    /**
     * True while an ISR member with no fetch record is still inside its
     * first-fetch grace. "No record" is ambiguous between "dead" and
     * "hasn't reached this leader yet" — the same {@code lagTimeoutMs}
     * allowance a recorded follower gets from its last fetch applies here,
     * measured from the first tick that observed the record missing. Once
     * a record exists the grace anchor is dropped; staleness takes over.
     */
    private boolean withinFirstFetchGrace(String topic, int partition, int brokerId, long nowMillis) {
        var key = new FollowerKey(topic, partition, brokerId);
        if (tracker.get(topic, partition, brokerId).isPresent()) {
            firstSeenWithoutRecord.remove(key);
            return false;
        }
        long firstSeen = firstSeenWithoutRecord.computeIfAbsent(key, k -> nowMillis);
        return nowMillis - firstSeen < lagTimeoutMs;
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
        var laggards = new ArrayList<>(tracker.laggardsOf(topic, partition, nonLeaderIsr, nowMillis, lagTimeoutMs));
        laggards.removeIf(b -> withinFirstFetchGrace(topic, partition, b, nowMillis));

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
        // controller-driven failover will resolve.
        if (!nextIsr.contains(selfBrokerId) || nextIsr.isEmpty()) return null;

        // ISR-only flip: keep leader_epoch so followers don't trigger
        // the OffsetsForLeaderEpoch reconciliation cycle; bump
        // partition_epoch so every follower sees it as newer metadata.
        // CAS-guarded: if a leader change or another flip lands between
        // this read and the apply, the record is dropped and the next
        // tick re-derives from fresh state.
        var change = PartitionChangeRecord.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setLeader(selfBrokerId)
                .addAllIsr(nextIsr)
                .addAllReplicas(state.replicas())
                .setLeaderEpoch(state.leaderEpoch())
                .setPartitionEpoch(state.partitionEpoch() + 1)
                .setPriorLeaderEpoch(state.leaderEpoch())
                .setPriorPartitionEpoch(state.partitionEpoch())
                .build();
        log.debug(
                "ISR flip proposed for {}-{}: shrink={}, expand={}, new isr={}, partition_epoch {}->{}",
                topic,
                partition,
                laggards,
                catchupCandidates,
                nextIsr,
                state.partitionEpoch(),
                state.partitionEpoch() + 1);
        return MetadataRecord.newBuilder().setPartitionChange(change).build().toByteArray();
    }
}
