package jbroker.broker.replication;

import java.util.function.Supplier;
import jbroker.broker.BrokerLiveness;
import jbroker.broker.TopicManager;
import jbroker.proto.raft.MetadataRecord;
import jbroker.proto.raft.PartitionChangeRecord;
import jbroker.raft.core.Role;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Active-controller-only loop that fences brokers whose last heartbeat is
 * older than {@code staleThresholdNanos}. For each partition the stale
 * broker leads, proposes a {@link PartitionChangeRecord} with a surviving
 * ISR member as the new leader and a bumped {@code leader_epoch}.
 *
 * <p>Runs as a no-op when {@code roleSupplier} returns anything other than
 * {@link Role#LEADER} — two brokers can't both be Raft leader, so the
 * single-writer invariant for partition-change proposals is preserved.
 *
 * <p>Self is never fenced: a bogus self-entry in {@link BrokerLiveness}
 * (clock skew, stuck thread) can't cascade into losing our own
 * leaderships.
 *
 * <p>A stale broker with no surviving ISR members emits a proposal with
 * {@code leader = -1} (sentinel "no leader"); the partition becomes
 * unavailable until that broker returns and re-joins the ISR. P6.6's
 * {@code acks=all} treats leader=-1 as "reject write."
 */
public final class BrokerFencer {

    private static final Logger log = LoggerFactory.getLogger(BrokerFencer.class);
    private static final int NO_LEADER = -1;

    @FunctionalInterface
    public interface MetadataProposer {
        void propose(byte[] payload);
    }

    private final int selfBrokerId;
    private final TopicManager topicManager;
    private final BrokerLiveness liveness;
    private final MetadataProposer proposer;
    private final Supplier<Role> roleSupplier;
    private final long staleThresholdNanos;

    public BrokerFencer(
            int selfBrokerId,
            TopicManager topicManager,
            BrokerLiveness liveness,
            MetadataProposer proposer,
            Supplier<Role> roleSupplier,
            long staleThresholdNanos) {
        this.selfBrokerId = selfBrokerId;
        this.topicManager = topicManager;
        this.liveness = liveness;
        this.proposer = proposer;
        this.roleSupplier = roleSupplier;
        this.staleThresholdNanos = staleThresholdNanos;
    }

    /**
     * Scan the liveness map, identify stale non-self brokers that lead any
     * partition, and propose a fencing {@link PartitionChangeRecord} for
     * each such partition.
     */
    public void tick(long nowNanos) {
        if (roleSupplier.get() != Role.LEADER) return;
        for (var assignment : topicManager.allPartitionAssignments()) {
            int leader = assignment.state().leader();
            if (leader == selfBrokerId) continue;
            var sig = liveness.lastSignal(leader);
            if (sig.isEmpty()) continue;
            if (nowNanos - sig.get().wallClockNanos() <= staleThresholdNanos) continue;

            int newLeader = NO_LEADER;
            var newIsr = new java.util.ArrayList<Integer>();
            for (int member : assignment.state().isr()) {
                if (member == leader) continue;
                newIsr.add(member);
                if (newLeader == NO_LEADER) newLeader = member;
            }
            int newLeaderEpoch = assignment.state().leaderEpoch() + 1;
            var record = PartitionChangeRecord.newBuilder()
                    .setTopic(assignment.topic())
                    .setPartition(assignment.partition())
                    .setLeader(newLeader)
                    .addAllIsr(newIsr)
                    .addAllReplicas(assignment.state().replicas())
                    .setLeaderEpoch(newLeaderEpoch)
                    .setPartitionEpoch(0)
                    .build();
            var payload = MetadataRecord.newBuilder()
                    .setPartitionChange(record)
                    .build()
                    .toByteArray();
            log.warn(
                    "fencing broker {} from {}-{}; new leader={}, leader_epoch={} isr={}",
                    leader,
                    assignment.topic(),
                    assignment.partition(),
                    newLeader,
                    newLeaderEpoch,
                    newIsr);
            proposer.propose(payload);
        }
    }
}
