package jbroker.broker;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Chooses a topic's replica set from the candidate brokers. A pure function
 * so placement policy is unit-testable apart from the admin path that calls
 * it on topic creation.
 *
 * <p>Rack-blind clusters (fewer than two distinct racks among the
 * candidates) get exactly the original policy: the first {@code rf}
 * candidates in the order given. With two or more racks the choice
 * round-robins the racks so the replica set spans as many as it can —
 * a whole-zone outage then cannot take out every copy of a partition.
 *
 * <p>Deterministic by construction: racks are visited in sorted order
 * (rackless brokers grouped last) and, within a rack, brokers by ascending
 * replica load with ties to the lowest id. The candidate at index 0 — the
 * proposing controller — is always chosen first, preserving the invariant
 * that it leads the partitions it creates.
 */
public final class ReplicaPlacer {

    private ReplicaPlacer() {}

    /**
     * Pick {@code rf} replicas out of {@code candidates}.
     *
     * @param candidates broker ids to choose from; index 0 is the anchor
     *                   (always chosen first) and the list's order is the
     *                   tiebreak when racks don't apply
     * @param rackOf     rack label per broker id; unlabeled brokers absent
     * @param load       current replica count per broker id; absent = 0
     * @param rf         replica count wanted; must be ≤ candidates.size()
     */
    public static List<Integer> place(
            List<Integer> candidates, Map<Integer, String> rackOf, Map<Integer, Integer> load, int rf) {
        if (rf > candidates.size()) {
            throw new IllegalArgumentException("rf " + rf + " exceeds candidate count " + candidates.size());
        }
        var distinctRacks = new HashSet<String>();
        for (int b : candidates) {
            var rack = rackOf.getOrDefault(b, "");
            if (!rack.isEmpty()) distinctRacks.add(rack);
        }
        if (distinctRacks.size() < 2) {
            return List.copyOf(candidates.subList(0, rf));
        }

        int anchor = candidates.get(0);
        var grouped = new HashMap<String, List<Integer>>();
        for (int i = 1; i < candidates.size(); i++) {
            int b = candidates.get(i);
            grouped.computeIfAbsent(rackOf.getOrDefault(b, ""), r -> new ArrayList<>())
                    .add(b);
        }
        var rackOrder = grouped.keySet().stream()
                .filter(r -> !r.isEmpty())
                .sorted()
                .collect(java.util.stream.Collectors.toCollection(ArrayList::new));
        if (grouped.containsKey("")) rackOrder.add("");

        var queues = new HashMap<String, ArrayDeque<Integer>>();
        for (var e : grouped.entrySet()) {
            var sorted = new ArrayList<>(e.getValue());
            sorted.sort(java.util.Comparator.comparingInt((Integer b) -> load.getOrDefault(b, 0))
                    .thenComparingInt(b -> b));
            queues.put(e.getKey(), new ArrayDeque<>(sorted));
        }

        var chosen = new ArrayList<Integer>(rf);
        chosen.add(anchor);
        // Start the rotation on the rack after the anchor's so the second
        // replica always lands off the anchor's rack (indexOf is -1 for an
        // anchor whose rack holds no other candidate; the +1 then starts
        // at the first rack, which is equally off-anchor).
        int start = rackOrder.indexOf(rackOf.getOrDefault(anchor, "")) + 1;
        for (int step = 0; chosen.size() < rf && step < rackOrder.size() * rf; step++) {
            var queue = queues.get(rackOrder.get((start + step) % rackOrder.size()));
            if (!queue.isEmpty()) chosen.add(queue.poll());
        }
        return List.copyOf(chosen);
    }
}
