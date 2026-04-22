package jbroker.broker.group;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;
import jbroker.proto.common.TopicPartition;
import org.junit.jupiter.api.Test;

class RangeAssignorTest {

    private static final RangeAssignor RANGE = new RangeAssignor();

    @Test
    void evenSplit_sixPartitionsThreeMembers_eachGetsTwo() {
        var assignment = RANGE.assign(
                List.of("m1", "m2", "m3"),
                Map.of("m1", Set.of("orders"), "m2", Set.of("orders"), "m3", Set.of("orders")),
                Map.of("orders", 6));

        assertThat(assignment.get("m1"))
                .extracting(TopicPartition::getPartition)
                .containsExactly(0, 1);
        assertThat(assignment.get("m2"))
                .extracting(TopicPartition::getPartition)
                .containsExactly(2, 3);
        assertThat(assignment.get("m3"))
                .extracting(TopicPartition::getPartition)
                .containsExactly(4, 5);
    }

    @Test
    void unevenSplit_nineOverFour_firstMemberGetsExtra() {
        var assignment = RANGE.assign(
                List.of("a", "b", "c", "d"),
                Map.of(
                        "a", Set.of("t"),
                        "b", Set.of("t"),
                        "c", Set.of("t"),
                        "d", Set.of("t")),
                Map.of("t", 9));

        // 9 / 4 = 2 base, 1 remainder. First member gets 3, rest get 2.
        assertThat(assignment.get("a")).hasSize(3);
        assertThat(assignment.get("b")).hasSize(2);
        assertThat(assignment.get("c")).hasSize(2);
        assertThat(assignment.get("d")).hasSize(2);
        // Contiguous runs: a=[0,1,2], b=[3,4], c=[5,6], d=[7,8].
        assertThat(assignment.get("a")).extracting(TopicPartition::getPartition).containsExactly(0, 1, 2);
        assertThat(assignment.get("d")).extracting(TopicPartition::getPartition).containsExactly(7, 8);
    }

    @Test
    void singleConsumer_getsAllPartitions() {
        var assignment = RANGE.assign(List.of("solo"), Map.of("solo", Set.of("t")), Map.of("t", 4));
        assertThat(assignment.get("solo"))
                .extracting(TopicPartition::getPartition)
                .containsExactly(0, 1, 2, 3);
    }

    @Test
    void moreMembersThanPartitions_extrasGetEmpty() {
        var assignment = RANGE.assign(
                List.of("a", "b", "c", "d", "e"),
                Map.of(
                        "a", Set.of("t"),
                        "b", Set.of("t"),
                        "c", Set.of("t"),
                        "d", Set.of("t"),
                        "e", Set.of("t")),
                Map.of("t", 2));

        assertThat(assignment.get("a")).extracting(TopicPartition::getPartition).containsExactly(0);
        assertThat(assignment.get("b")).extracting(TopicPartition::getPartition).containsExactly(1);
        assertThat(assignment.get("c")).isEmpty();
        assertThat(assignment.get("d")).isEmpty();
        assertThat(assignment.get("e")).isEmpty();
    }

    @Test
    void multipleTopics_assignedIndependentlyThenConcatenated() {
        var assignment = RANGE.assign(
                List.of("m1", "m2"), Map.of("m1", Set.of("a", "b"), "m2", Set.of("a", "b")), Map.of("a", 4, "b", 2));

        // Topic a: 4/2 = [0,1] [2,3]; topic b: 2/2 = [0] [1].
        assertThat(assignment.get("m1")).hasSize(3);
        assertThat(assignment.get("m2")).hasSize(3);
        assertThat(assignment.get("m1"))
                .extracting(tp -> tp.getTopic() + "-" + tp.getPartition())
                .containsExactly("a-0", "a-1", "b-0");
        assertThat(assignment.get("m2"))
                .extracting(tp -> tp.getTopic() + "-" + tp.getPartition())
                .containsExactly("a-2", "a-3", "b-1");
    }

    @Test
    void differentSubscriptions_topicsRoutedOnlyToInterestedMembers() {
        var assignment =
                RANGE.assign(List.of("m1", "m2"), Map.of("m1", Set.of("a"), "m2", Set.of("b")), Map.of("a", 2, "b", 2));

        // Each topic is consumed by only one member, so each gets all
        // partitions of their own subscribed topic.
        assertThat(assignment.get("m1")).hasSize(2).allSatisfy(tp -> assertThat(tp.getTopic())
                .isEqualTo("a"));
        assertThat(assignment.get("m2")).hasSize(2).allSatisfy(tp -> assertThat(tp.getTopic())
                .isEqualTo("b"));
    }

    @Test
    void deterministic_sameInputProducesSameOutputAcrossRuns() {
        var first = RANGE.assign(
                List.of("m1", "m2", "m3"),
                Map.of("m1", Set.of("t"), "m2", Set.of("t"), "m3", Set.of("t")),
                Map.of("t", 7));
        var second = RANGE.assign(
                List.of("m1", "m2", "m3"),
                Map.of("m1", Set.of("t"), "m2", Set.of("t"), "m3", Set.of("t")),
                Map.of("t", 7));
        assertThat(first).isEqualTo(second);
    }
}
