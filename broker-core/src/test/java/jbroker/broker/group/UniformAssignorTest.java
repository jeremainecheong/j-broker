package jbroker.broker.group;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;
import jbroker.proto.common.TopicPartition;
import org.junit.jupiter.api.Test;

class UniformAssignorTest {

    private static final UniformAssignor UNIFORM = new UniformAssignor();

    @Test
    void singleTopic_evenSplitMatchesRange() {
        var assignment = UNIFORM.assign(
                List.of("m1", "m2", "m3"),
                Map.of("m1", Set.of("orders"), "m2", Set.of("orders"), "m3", Set.of("orders")),
                Map.of("orders", 6));

        assertThat(assignment.get("m1")).hasSize(2);
        assertThat(assignment.get("m2")).hasSize(2);
        assertThat(assignment.get("m3")).hasSize(2);
        // Round-robin: m1=[0,3] m2=[1,4] m3=[2,5].
        assertThat(assignment.get("m1"))
                .extracting(TopicPartition::getPartition)
                .containsExactly(0, 3);
        assertThat(assignment.get("m2"))
                .extracting(TopicPartition::getPartition)
                .containsExactly(1, 4);
        assertThat(assignment.get("m3"))
                .extracting(TopicPartition::getPartition)
                .containsExactly(2, 5);
    }

    @Test
    void multipleTopics_evenLoadAcrossMembers() {
        var assignment = UNIFORM.assign(
                List.of("m1", "m2", "m3"),
                Map.of(
                        "m1", Set.of("a", "b"),
                        "m2", Set.of("a", "b"),
                        "m3", Set.of("a", "b")),
                Map.of("a", 3, "b", 3));

        // 6 total pairs across 3 members → each gets 2.
        assertThat(assignment.get("m1")).hasSize(2);
        assertThat(assignment.get("m2")).hasSize(2);
        assertThat(assignment.get("m3")).hasSize(2);
    }

    @Test
    void heterogeneousSubscriptions_skipsIneligibleMembersForEachPair() {
        var assignment = UNIFORM.assign(
                List.of("a", "b", "c"),
                Map.of(
                        "a", Set.of("topic-x"),
                        "b", Set.of("topic-y"),
                        "c", Set.of("topic-x", "topic-y")),
                Map.of("topic-x", 2, "topic-y", 2));

        // topic-x partitions only go to {a, c}; topic-y only to {b, c}.
        assertThat(assignment.get("a"))
                .allSatisfy(tp -> assertThat(tp.getTopic()).isEqualTo("topic-x"));
        assertThat(assignment.get("b"))
                .allSatisfy(tp -> assertThat(tp.getTopic()).isEqualTo("topic-y"));
        // Every partition is assigned exactly once.
        long total = assignment.values().stream().mapToLong(List::size).sum();
        assertThat(total).isEqualTo(4);
    }

    @Test
    void emptyMembers_returnsEmptyAssignment() {
        var assignment = UNIFORM.assign(List.of(), Map.of(), Map.of("t", 4));
        assertThat(assignment).isEmpty();
    }

    @Test
    void deterministic_sameInputProducesSameOutputAcrossRuns() {
        var first = UNIFORM.assign(
                List.of("m1", "m2"), Map.of("m1", Set.of("a", "b"), "m2", Set.of("a", "b")), Map.of("a", 4, "b", 3));
        var second = UNIFORM.assign(
                List.of("m1", "m2"), Map.of("m1", Set.of("a", "b"), "m2", Set.of("a", "b")), Map.of("a", 4, "b", 3));
        assertThat(first).isEqualTo(second);
    }
}
