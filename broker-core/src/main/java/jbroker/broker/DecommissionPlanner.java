package jbroker.broker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Plans the drain phase of a broker decommission: for every partition hosting
 * a replica on the leaving broker, pick a replacement and emit the
 * reassignment target that moves the replica off — preserving each
 * partition's replication factor exactly. A pure function over the replicated
 * partition assignments, in the mold of {@link ReassignmentPlanner}: the
 * caller starts the resulting reassignments and watches them drain.
 *
 * <p>All-or-nothing: if any partition has no eligible replacement (a live
 * broker not already hosting it), the whole plan is refused up front rather
 * than leaving the cluster half-drained. Replacements are spread across the
 * eligible brokers by current replica count so the drain doesn't pile every
 * moved replica onto one node.
 *
 * <p>Leadership needs no special handling here: when the leaving broker leads
 * a partition, {@link ReassignmentPlanner}'s contract step moves leadership
 * into the target once the newcomer is in sync.
 */
public final class DecommissionPlanner {

    /** One partition's move: reassign to {@code target} (same size as the current replica set). */
    public record Move(String topic, int partition, List<Integer> target) {
        public Move {
            target = List.copyOf(target);
        }
    }

    /** Outcome: either every affected partition has a move, or a refusal naming the first blocker. */
    public sealed interface Result {
        record Plan(List<Move> moves) implements Result {
            public Plan {
                moves = List.copyOf(moves);
            }
        }

        record Refused(String reason) implements Result {}
    }

    private DecommissionPlanner() {}

    /**
     * Plan the reassignments that drain broker {@code leaving}.
     *
     * @param leaving     broker id being decommissioned
     * @param liveBrokers brokers eligible to receive replicas (registered and
     *                    alive; the caller excludes any it distrusts)
     * @param assignments the cluster's partition assignments
     */
    public static Result plan(int leaving, Set<Integer> liveBrokers, List<PartitionAssignment> assignments) {
        // Spread replacements: start from each candidate's current replica
        // count and charge every planned move against its target, so a drain
        // of N partitions doesn't dump all N onto the least-loaded broker.
        var load = new HashMap<Integer, Integer>();
        for (int b : liveBrokers) {
            if (b != leaving) load.put(b, 0);
        }
        for (var a : assignments) {
            for (int r : a.state().replicas()) {
                load.computeIfPresent(r, (k, v) -> v + 1);
            }
        }

        var moves = new ArrayList<Move>();
        for (var a : assignments) {
            var replicas = a.state().replicas();
            if (!replicas.contains(leaving)) continue;

            // Deterministic pick: fewest replicas, ties to the lowest broker
            // id — so the same inputs always plan the same drain.
            Integer replacement = null;
            for (int candidate : load.keySet().stream().sorted().toList()) {
                if (replicas.contains(candidate)) continue;
                if (replacement == null || load.get(candidate) < load.get(replacement)) {
                    replacement = candidate;
                }
            }
            if (replacement == null) {
                return new Result.Refused("partition " + a.topic() + "-" + a.partition()
                        + " has no eligible replacement broker; decommissioning " + leaving
                        + " would reduce its replication factor");
            }
            load.computeIfPresent(replacement, (k, v) -> v + 1);

            var target = new ArrayList<Integer>(replicas.size());
            for (int r : replicas) {
                if (r != leaving) target.add(r);
            }
            target.add(replacement);
            moves.add(new Move(a.topic(), a.partition(), target));
        }
        return new Result.Plan(moves);
    }
}
