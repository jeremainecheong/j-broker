package jbroker.broker.metrics;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.ToLongFunction;
import jbroker.broker.PartitionAssignment;
import jbroker.broker.TopicManager;
import jbroker.broker.replication.FollowerStateTracker;
import jbroker.storage.LogManager;

/**
 * default provider: enumerates leader-hosted partitions from
 * {@link TopicManager}, reads leader LEO from {@link LogManager},
 * computes HWM via {@link FollowerStateTracker#computeHwm}, and
 * derives per-follower lag from tracker state.
 *
 * <p>Lag is reported in records (not bytes) on the wire; an approximate
 * record-size multiplier can be applied admin-side when a byte-accurate
 * count isn't available without re-reading segments.
 */
public final class DefaultPartitionMetricsProvider implements PartitionMetricsProvider {

    private final int selfBrokerId;
    private final TopicManager topicManager;
    private final LogManager logManager;
    private final FollowerStateTracker followerTracker;

    public DefaultPartitionMetricsProvider(
            int selfBrokerId, TopicManager topicManager, LogManager logManager, FollowerStateTracker followerTracker) {
        this.selfBrokerId = selfBrokerId;
        this.topicManager = topicManager;
        this.logManager = logManager;
        this.followerTracker = followerTracker;
    }

    @Override
    public List<PartitionMetricsSnapshot> snapshot() {
        var out = new CopyOnWriteArrayList<PartitionMetricsSnapshot>();
        List<PartitionAssignment> all = topicManager.allPartitionAssignments();
        ToLongFunction<PartitionAssignment> leaderLeoOf = pa -> {
            try {
                return logManager.logFor(pa.topic(), pa.partition()).nextOffset();
            } catch (IOException e) {
                return 0L;
            }
        };
        for (var pa : all) {
            var state = pa.state();
            if (state.leader() != selfBrokerId) continue;
            long leaderLeo = leaderLeoOf.applyAsLong(pa);
            long hwm = followerTracker.computeHwm(pa.topic(), pa.partition(), state.isr(), selfBrokerId, leaderLeo);
            Map<Integer, Long> lag = new HashMap<>();
            for (int broker : state.replicas()) {
                if (broker == selfBrokerId) continue;
                long followerLeo = followerTracker
                        .get(pa.topic(), pa.partition(), broker)
                        .map(FollowerStateTracker.Match::leo)
                        .orElse(0L);
                lag.put(broker, Math.max(0L, leaderLeo - followerLeo));
            }
            out.add(new PartitionMetricsSnapshot(
                    pa.topic(), pa.partition(), state.isr().size(), hwm, leaderLeo, lag));
        }
        return List.copyOf(out);
    }
}
