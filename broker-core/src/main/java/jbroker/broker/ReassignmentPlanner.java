package jbroker.broker;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import jbroker.proto.raft.MetadataRecord;
import jbroker.proto.raft.PartitionChangeRecord;
import jbroker.proto.raft.PartitionReassignmentRecord;

/**
 * Decides the next metadata record that advances a partition reassignment,
 * as a pure function of the partition's current state and its pending target.
 * Mirrors {@link jbroker.broker.replication.IsrManager}'s design: the
 * controller ticks it for each pending reassignment and proposes whatever it
 * returns; empty means "wait".
 *
 * <p>The reassignment runs in three moves, leaning on the ISR machinery that
 * already exists:
 * <ol>
 *   <li><b>Expand</b> — set the replica set to {@code current ∪ target} so the
 *       newcomers start fetching. {@code IsrManager} then grows them into the
 *       ISR as each catches up to the high-water mark.</li>
 *   <li><b>Contract</b> — once every target replica is in the ISR, set the
 *       replica set to {@code target}, moving leadership into the target if the
 *       current leader is leaving. This is the point of no return; a cancel is
 *       only honored before it (by pointing the target back at the original).</li>
 *   <li><b>Clear</b> — with the replica set equal to the target, emit an empty
 *       reassignment record so the driver stops.</li>
 * </ol>
 *
 * <p>Every {@link PartitionChangeRecord} carries the epochs it was derived
 * from ({@code prior_*}) so a stale proposal is dropped on apply and re-derived
 * next tick — the same compare-and-set guard the ISR path uses.
 */
public final class ReassignmentPlanner {

    private ReassignmentPlanner() {}

    /**
     * The next record to propose for {@code pending}'s partition, or empty when
     * the reassignment is mid-catch-up and nothing should change yet.
     */
    public static Optional<MetadataRecord> plan(PartitionState state, ReassignmentStore.Pending pending) {
        String topic = pending.topic();
        int partition = pending.partition();
        List<Integer> target = pending.target();
        List<Integer> replicas = state.replicas();
        List<Integer> union = union(replicas, target);

        // 1. Expand: pull the newcomers into the replica set so they fetch.
        if (!sameSet(replicas, union)) {
            return Optional.of(partitionChange(topic, partition, state.leader(), state.isr(), union, state));
        }

        // 2. Contract: every target replica is in the ISR — switch to the
        //    target set, moving leadership into it if the leader is leaving.
        if (state.isr().containsAll(target) && !sameSet(replicas, target)) {
            int newLeader = target.contains(state.leader()) ? state.leader() : target.get(0);
            return Optional.of(partitionChange(topic, partition, newLeader, target, target, state));
        }

        // 3. Done: replica set already equals the target — clear the pending
        //    entry so the driver stops. (Idempotent; safe to re-emit.)
        if (sameSet(replicas, target)) {
            return Optional.of(MetadataRecord.newBuilder()
                    .setPartitionReassignment(PartitionReassignmentRecord.newBuilder()
                            .setTopic(topic)
                            .setPartition(partition)
                            .build())
                    .build());
        }

        // Mid-catch-up: newcomers are in the replica set but not yet all in the
        // ISR. Wait for IsrManager to grow them.
        return Optional.empty();
    }

    private static MetadataRecord partitionChange(
            String topic, int partition, int leader, List<Integer> isr, List<Integer> replicas, PartitionState from) {
        boolean leaderChanged = leader != from.leader();
        // A leader move bumps leader_epoch so followers reconcile via
        // OffsetsForLeaderEpoch; a replica-set-only change keeps it and only
        // bumps partition_epoch, exactly as an ISR flip does.
        int leaderEpoch = leaderChanged ? from.leaderEpoch() + 1 : from.leaderEpoch();
        var change = PartitionChangeRecord.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setLeader(leader)
                .addAllIsr(leaderFirst(leader, isr))
                .addAllReplicas(replicas)
                .setLeaderEpoch(leaderEpoch)
                .setPartitionEpoch(leaderChanged ? 0 : from.partitionEpoch() + 1)
                .setPriorLeaderEpoch(from.leaderEpoch())
                .setPriorPartitionEpoch(from.partitionEpoch())
                .build();
        return MetadataRecord.newBuilder().setPartitionChange(change).build();
    }

    /** Distinct union preserving first-seen order (current replicas, then newcomers). */
    private static List<Integer> union(List<Integer> a, List<Integer> b) {
        var out = new LinkedHashSet<Integer>(a);
        out.addAll(b);
        return new ArrayList<>(out);
    }

    /** {@code members} with {@code leader} first — the state machine requires isr[0] == leader. */
    private static List<Integer> leaderFirst(int leader, List<Integer> members) {
        var out = new ArrayList<Integer>(members.size());
        out.add(leader);
        for (int m : members) {
            if (m != leader) out.add(m);
        }
        return out;
    }

    private static boolean sameSet(List<Integer> a, List<Integer> b) {
        return a.size() == b.size() && new java.util.HashSet<>(a).equals(new java.util.HashSet<>(b));
    }
}
