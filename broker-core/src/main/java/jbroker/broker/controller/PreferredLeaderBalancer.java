package jbroker.broker.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import jbroker.broker.PartitionState;
import jbroker.broker.TopicManager;

/**
 * P9.7 — periodic preferred-leader rebalance task. For each partition
 * whose current leader is not {@code replicas[0]}, the balancer proposes
 * a {@code PartitionChangeRecord} moving the leader back to its
 * preferred replica. Skipped when the preferred replica is out of ISR
 * (it can't safely take leadership), or when the last leader change
 * happened within the stability window (avoids flapping under churn).
 *
 * <p>This class is the pure-decision engine — wiring to the controller's
 * propose path + the leader-change clock sits in the broker-app layer
 * so broker-core stays framework-free.
 */
public final class PreferredLeaderBalancer {

    /** Proposal emitted when the balancer decides to move leadership. */
    public record Proposal(String topic, int partition, int newLeader, int leaderEpoch) {}

    private final TopicManager topicManager;
    private final BooleanSupplier isActiveController;
    private final LongSupplier lastLeaderChangeMillis;
    private final long stabilityWindowMillis;

    public PreferredLeaderBalancer(
            TopicManager topicManager,
            BooleanSupplier isActiveController,
            LongSupplier lastLeaderChangeMillis,
            long stabilityWindowMillis) {
        this.topicManager = topicManager;
        this.isActiveController = isActiveController;
        this.lastLeaderChangeMillis = lastLeaderChangeMillis;
        this.stabilityWindowMillis = Math.max(0L, stabilityWindowMillis);
    }

    /** Compute proposals for the current snapshot. Does not mutate state. */
    public List<Proposal> proposeRebalances(long nowMillis) {
        var proposals = new ArrayList<Proposal>();
        if (!isActiveController.getAsBoolean()) return proposals;
        long since = nowMillis - lastLeaderChangeMillis.getAsLong();
        if (since < stabilityWindowMillis) return proposals;
        for (var pa : topicManager.allPartitionAssignments()) {
            PartitionState s = pa.state();
            if (s.replicas().isEmpty()) continue;
            int preferred = s.replicas().get(0);
            if (s.leader() == preferred) continue;
            if (!s.isr().contains(preferred)) continue;
            proposals.add(new Proposal(pa.topic(), pa.partition(), preferred, s.leaderEpoch() + 1));
        }
        return proposals;
    }
}
