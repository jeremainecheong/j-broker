package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Rack-aware replacement choice: a drain prefers a broker that keeps the
 * partition spanning distinct racks, and falls back to the rack-blind
 * least-load rule when no such broker exists.
 */
class DecommissionPlannerRackTest {

    private static PartitionAssignment assign(String topic, int partition, int leader, List<Integer> replicas) {
        return new PartitionAssignment(topic, partition, new PartitionState(leader, replicas, replicas, 1, 0));
    }

    private static DecommissionPlanner.Result.Plan planOf(DecommissionPlanner.Result r) {
        assertThat(r).isInstanceOf(DecommissionPlanner.Result.Plan.class);
        return (DecommissionPlanner.Result.Plan) r;
    }

    @Test
    void replacementRestoresTheRackTheLeaverVacates() {
        // t-0 spans zone-a (1) and zone-b (2). Draining 2, the rack-blind
        // rule would pick 3 (lowest id, equal load) — but 3 sits in the
        // survivor's zone-a, collapsing the spread. 4 in zone-b must win.
        var racks = Map.of(1, "zone-a", 2, "zone-b", 3, "zone-a", 4, "zone-b");

        var plan = planOf(
                DecommissionPlanner.plan(2, Set.of(1, 2, 3, 4), List.of(assign("t", 0, 1, List.of(1, 2))), racks));

        assertThat(plan.moves()).hasSize(1);
        assertThat(plan.moves().get(0).target()).containsExactly(1, 4);
    }

    @Test
    void fallsBackToLeastLoadWhenNoCandidatePreservesSpread() {
        // Every candidate shares the survivor's rack, so the rack-blind
        // rule decides: lowest id on the load tie.
        var racks = Map.of(1, "zone-a", 2, "zone-b", 3, "zone-a", 4, "zone-a");

        var plan = planOf(
                DecommissionPlanner.plan(2, Set.of(1, 2, 3, 4), List.of(assign("t", 0, 1, List.of(1, 2))), racks));

        assertThat(plan.moves()).hasSize(1);
        assertThat(plan.moves().get(0).target()).containsExactly(1, 3);
    }

    @Test
    void emptyRackMapPlansExactlyLikeTheRackBlindOverload() {
        var assignments = List.of(assign("t", 0, 1, List.of(1, 2)), assign("t", 1, 1, List.of(1, 2)));

        var blind = planOf(DecommissionPlanner.plan(2, Set.of(1, 2, 3, 4), assignments));
        var mapped = planOf(DecommissionPlanner.plan(2, Set.of(1, 2, 3, 4), assignments, Map.of()));

        assertThat(mapped.moves()).isEqualTo(blind.moves());
    }

    @Test
    void spreadPreservingTiesBreakByLoadThenLowestId() {
        // Both 4 and 6 sit in the uncovered zone-b; 6 is lighter (4 already
        // hosts u-0), so 6 wins despite the higher id.
        var racks = Map.of(1, "zone-a", 2, "zone-b", 4, "zone-b", 6, "zone-b");

        var plan = planOf(DecommissionPlanner.plan(
                2,
                Set.of(1, 2, 4, 6),
                List.of(assign("t", 0, 1, List.of(1, 2)), assign("u", 0, 4, List.of(4))),
                racks));

        assertThat(plan.moves()).hasSize(1);
        assertThat(plan.moves().get(0).target()).containsExactly(1, 6);
    }

    @Test
    void racklessCandidatesNeverCountAsSpreadPreserving() {
        // 3 has no rack label; preferring it would fake a spread. The
        // labeled zone-b broker 4 wins even though 3 has the lower id.
        var racks = Map.of(1, "zone-a", 2, "zone-b", 4, "zone-b");

        var plan = planOf(
                DecommissionPlanner.plan(2, Set.of(1, 2, 3, 4), List.of(assign("t", 0, 1, List.of(1, 2))), racks));

        assertThat(plan.moves()).hasSize(1);
        assertThat(plan.moves().get(0).target()).containsExactly(1, 4);
    }

    @Test
    void refusalStillHoldsWithRacks() {
        var racks = Map.of(1, "zone-a", 2, "zone-b", 3, "zone-c");

        var result = DecommissionPlanner.plan(1, Set.of(1, 2, 3), List.of(assign("t", 0, 1, List.of(1, 2, 3))), racks);

        assertThat(result).isInstanceOf(DecommissionPlanner.Result.Refused.class);
    }
}
