package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class DecommissionPlannerTest {

    private static PartitionAssignment assign(String topic, int partition, int leader, List<Integer> replicas) {
        return new PartitionAssignment(topic, partition, new PartitionState(leader, replicas, replicas, 1, 0));
    }

    private static DecommissionPlanner.Result.Plan planOf(DecommissionPlanner.Result r) {
        assertThat(r).isInstanceOf(DecommissionPlanner.Result.Plan.class);
        return (DecommissionPlanner.Result.Plan) r;
    }

    @Test
    void replacesTheLeavingBrokerPreservingReplicationFactor() {
        var plan = planOf(DecommissionPlanner.plan(2, Set.of(1, 2, 3, 4), List.of(assign("t", 0, 1, List.of(1, 2)))));

        assertThat(plan.moves()).hasSize(1);
        var move = plan.moves().get(0);
        assertThat(move.target()).hasSize(2).contains(1).doesNotContain(2);
        assertThat(move.target().get(1)).isIn(3, 4);
    }

    @Test
    void spreadsReplacementsAcrossCandidatesByLoad() {
        // Two empty candidates, two moves: the first replacement lands on 3
        // (fewest replicas, lowest id on the tie), which makes 4 the
        // less-loaded pick for the second — one each, not both on one node.
        var plan = planOf(DecommissionPlanner.plan(
                2, Set.of(1, 2, 3, 4), List.of(assign("t", 0, 1, List.of(1, 2)), assign("t", 1, 1, List.of(1, 2)))));

        assertThat(plan.moves()).hasSize(2);
        var replacements = plan.moves().stream()
                .map(m -> m.target().get(m.target().size() - 1))
                .toList();
        assertThat(replacements).containsExactlyInAnyOrder(3, 4);
    }

    @Test
    void avoidsAnAlreadyLoadedCandidateWhenALighterOneExists() {
        // Broker 4 already hosts u-0; broker 3 hosts nothing. The single
        // replacement must land on 3.
        var plan = planOf(DecommissionPlanner.plan(
                2, Set.of(1, 2, 3, 4), List.of(assign("t", 0, 1, List.of(1, 2)), assign("u", 0, 4, List.of(4)))));

        assertThat(plan.moves()).hasSize(1);
        assertThat(plan.moves().get(0).target()).containsExactlyInAnyOrder(1, 3);
    }

    @Test
    void refusesWhenNoBrokerCanTakeAReplicaWithoutShrinkingRf() {
        // RF 3 on a 3-broker cluster: nowhere to move broker 1's replica.
        var result = DecommissionPlanner.plan(1, Set.of(1, 2, 3), List.of(assign("t", 0, 1, List.of(1, 2, 3))));

        assertThat(result).isInstanceOf(DecommissionPlanner.Result.Refused.class);
        assertThat(((DecommissionPlanner.Result.Refused) result).reason()).contains("t-0");
    }

    @Test
    void refusalIsAllOrNothingEvenWhenOtherPartitionsHaveMoves() {
        var result = DecommissionPlanner.plan(
                1,
                Set.of(1, 2, 3, 4),
                List.of(assign("movable", 0, 1, List.of(1, 2)), assign("stuck", 0, 1, List.of(1, 2, 3, 4))));

        assertThat(result).isInstanceOf(DecommissionPlanner.Result.Refused.class);
    }

    @Test
    void skipsPartitionsNotHostingTheLeavingBroker() {
        var plan = planOf(DecommissionPlanner.plan(
                3, Set.of(1, 2, 3, 4), List.of(assign("t", 0, 1, List.of(1, 2)), assign("t", 1, 3, List.of(3, 2)))));

        assertThat(plan.moves()).hasSize(1);
        assertThat(plan.moves().get(0).partition()).isEqualTo(1);
    }

    @Test
    void emptyPlanWhenTheBrokerHostsNothing() {
        var plan = planOf(DecommissionPlanner.plan(4, Set.of(1, 2, 3, 4), List.of(assign("t", 0, 1, List.of(1, 2)))));

        assertThat(plan.moves()).isEmpty();
    }

    @Test
    void leaderLedPartitionIsStillJustAMove() {
        // The leaving broker leads t-0; the planner emits the same shaped
        // move — leadership transfer is the reassignment contract step's job.
        var plan = planOf(DecommissionPlanner.plan(1, Set.of(1, 2, 3), List.of(assign("t", 0, 1, List.of(1, 2)))));

        assertThat(plan.moves()).hasSize(1);
        assertThat(plan.moves().get(0).target()).containsExactlyInAnyOrder(2, 3);
    }
}
