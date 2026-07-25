package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class ReplicaPlacerTest {

    private static java.util.Set<String> racksOf(List<Integer> replicas, Map<Integer, String> rackOf) {
        return replicas.stream().map(b -> rackOf.getOrDefault(b, "")).collect(Collectors.toSet());
    }

    @Test
    void noRacksKeepsTheOriginalCandidateOrderExactly() {
        // Regression-critical: a rack-blind cluster must place exactly as
        // before — the first rf candidates, order untouched.
        var replicas = ReplicaPlacer.place(List.of(7, 3, 9), Map.of(), Map.of(), 2);

        assertThat(replicas).containsExactly(7, 3);
    }

    @Test
    void uniformRackKeepsTheOriginalCandidateOrderExactly() {
        var rackOf = Map.of(7, "zone-a", 3, "zone-a", 9, "zone-a");

        var replicas = ReplicaPlacer.place(List.of(7, 3, 9), rackOf, Map.of(), 2);

        assertThat(replicas).containsExactly(7, 3);
    }

    @Test
    void twoRacksRfTwoSpansBoth() {
        // Anchor 1 shares zone-a with 2; the second replica must come from
        // zone-b even though 2 precedes 3 in candidate order.
        var rackOf = Map.of(1, "zone-a", 2, "zone-a", 3, "zone-b");

        var replicas = ReplicaPlacer.place(List.of(1, 2, 3), rackOf, Map.of(), 2);

        assertThat(replicas).containsExactly(1, 3);
        assertThat(racksOf(replicas, rackOf)).containsExactlyInAnyOrder("zone-a", "zone-b");
    }

    @Test
    void twoRacksRfThreeCoversBothThenFillsBack() {
        var rackOf = Map.of(1, "zone-a", 2, "zone-a", 3, "zone-b", 4, "zone-b");

        var replicas = ReplicaPlacer.place(List.of(1, 2, 3, 4), rackOf, Map.of(), 3);

        assertThat(replicas).containsExactly(1, 3, 2);
        assertThat(racksOf(replicas, rackOf)).containsExactlyInAnyOrder("zone-a", "zone-b");
    }

    @Test
    void threeRacksRfThreeCoversAllThree() {
        var rackOf = Map.of(1, "zone-a", 2, "zone-b", 3, "zone-c", 4, "zone-a");

        var replicas = ReplicaPlacer.place(List.of(1, 2, 3, 4), rackOf, Map.of(), 3);

        assertThat(replicas).startsWith(1);
        assertThat(racksOf(replicas, rackOf)).containsExactlyInAnyOrder("zone-a", "zone-b", "zone-c");
    }

    @Test
    void anchorStaysFirstWhateverItsRack() {
        // The proposing controller must stay replicas[0] — it names itself
        // partition leader and the preferred-leader balancer trusts index 0.
        var rackOf = Map.of(5, "zone-b", 1, "zone-a", 2, "zone-a");

        var replicas = ReplicaPlacer.place(List.of(5, 1, 2), rackOf, Map.of(), 2);

        assertThat(replicas).startsWith(5);
        assertThat(racksOf(replicas, rackOf)).containsExactlyInAnyOrder("zone-a", "zone-b");
    }

    @Test
    void leastLoadedBrokerWinsWithinARack() {
        var rackOf = Map.of(1, "zone-a", 2, "zone-b", 3, "zone-b");

        var replicas = ReplicaPlacer.place(List.of(1, 2, 3), rackOf, Map.of(2, 5, 3, 1), 2);

        assertThat(replicas).containsExactly(1, 3);
    }

    @Test
    void loadTiesBreakToTheLowestId() {
        var rackOf = Map.of(1, "zone-a", 9, "zone-b", 4, "zone-b");

        var replicas = ReplicaPlacer.place(List.of(1, 9, 4), rackOf, Map.of(), 2);

        assertThat(replicas).containsExactly(1, 4);
    }

    @Test
    void racklessBrokersAreASpreadGroupOfLastResort() {
        // 3 has no rack; racked candidates win while they last, but the
        // rackless broker still fills the set when rf outruns them.
        var rackOf = Map.of(1, "zone-a", 2, "zone-b");

        assertThat(ReplicaPlacer.place(List.of(1, 3, 2), rackOf, Map.of(), 2)).containsExactly(1, 2);
        assertThat(ReplicaPlacer.place(List.of(1, 3, 2), rackOf, Map.of(), 3)).containsExactly(1, 2, 3);
    }

    @Test
    void rackAwareChoiceIgnoresCandidateTailOrder() {
        // Determinism: with racks in play the result depends only on the
        // anchor, racks, and load — not on the tail's iteration order
        // (which comes from an unordered Set at the call site).
        var rackOf = Map.of(1, "zone-a", 2, "zone-a", 3, "zone-b", 4, "zone-b");

        var a = ReplicaPlacer.place(List.of(1, 2, 3, 4), rackOf, Map.of(), 3);
        var b = ReplicaPlacer.place(List.of(1, 4, 3, 2), rackOf, Map.of(), 3);

        assertThat(a).isEqualTo(b);
    }

    @Test
    void refusesAnRfLargerThanTheCandidateSet() {
        assertThatThrownBy(() -> ReplicaPlacer.place(List.of(1), Map.of(), Map.of(), 2))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
