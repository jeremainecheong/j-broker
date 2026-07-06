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
 * unavailable until that broker returns and re-joins the ISR. 's
 * {@code acks=all} treats leader=-1 as "reject write."
 *
 * <p><b>Never-heard-from leaders are fenceable.</b> A partition leader with
 * no {@link BrokerLiveness} entry at all (it died before its first
 * heartbeat reached this controller — reachable in practice because the
 * sender's gRPC channel backs off exponentially after startup connect
 * failures) starts a grace clock at first observation and is fenced once
 * {@code staleThresholdNanos} elapses with still no signal. Without this,
 * such a leader was skipped silently forever and its partitions never
 * failed over. Grace clocks measure continuous observation as controller:
 * they reset when this node is not the Raft leader, and are discarded the
 * moment a real signal arrives.
 */
public final class BrokerFencer {

    private static final Logger log = LoggerFactory.getLogger(BrokerFencer.class);
    private static final int NO_LEADER = -1;

    /**
     * brokerId → nanos when this controller first observed the broker
     * leading a partition with no liveness entry. Only touched from the
     * single fencer ticker thread; a plain map is safe.
     */
    private final java.util.HashMap<Integer, Long> neverSeenFirstObservedNanos = new java.util.HashMap<>();

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
        if (roleSupplier.get() != Role.LEADER) {
            neverSeenFirstObservedNanos.clear();
            return;
        }
        for (var assignment : topicManager.allPartitionAssignments()) {
            int leader = assignment.state().leader();
            if (leader == selfBrokerId) continue;
            var sig = liveness.lastSignal(leader);
            long lastSignalNanos;
            if (sig.isEmpty()) {
                // Never heard from this leader. Start (or continue) the
                // grace clock; fence only once the clock exceeds the same
                // staleness threshold a real signal would be held to.
                lastSignalNanos = neverSeenFirstObservedNanos.computeIfAbsent(leader, id -> {
                    log.warn(
                            "partition leader {} of {}-{} has no liveness signal; "
                                    + "fencing in {}ms unless a heartbeat arrives",
                            leader,
                            assignment.topic(),
                            assignment.partition(),
                            staleThresholdNanos / 1_000_000);
                    return nowNanos;
                });
            } else {
                neverSeenFirstObservedNanos.remove(leader);
                lastSignalNanos = sig.get().wallClockNanos();
            }
            if (nowNanos - lastSignalNanos <= staleThresholdNanos) continue;

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
