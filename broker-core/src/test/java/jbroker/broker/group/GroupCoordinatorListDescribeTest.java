package jbroker.broker.group;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import org.junit.jupiter.api.Test;

class GroupCoordinatorListDescribeTest {

    @Test
    void emptyCoordinatorListsNoGroups() {
        var c = new GroupCoordinator(t -> 1, new RangeAssignor());
        assertThat(c.listGroups()).isEmpty();
        assertThat(c.describeGroup("never-seen")).isEmpty();
    }

    @Test
    void joinOneMemberGroupSurfacesInList() {
        var c = new GroupCoordinator(t -> 3, new RangeAssignor(), instance -> "m-1");
        c.join("g1", "", Set.of("orders"), 30_000L, 60_000, 0L);

        var summaries = c.listGroups();
        assertThat(summaries).hasSize(1);
        var s = summaries.get(0);
        assertThat(s.groupId()).isEqualTo("g1");
        assertThat(s.state()).isEqualTo("Stable");
        assertThat(s.memberCount()).isEqualTo(1);
        assertThat(s.generation()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void describeGroupReturnsMembersWithSubscriptionsAndOwnedPartitions() {
        var c = new GroupCoordinator(t -> 3, new RangeAssignor(), instance -> "m-a");
        c.join("g1", "", Set.of("orders"), 30_000L, 60_000, 0L);

        var detail = c.describeGroup("g1");
        assertThat(detail).isPresent();
        var d = detail.get();
        assertThat(d.groupId()).isEqualTo("g1");
        assertThat(d.state()).isEqualTo("Stable");
        assertThat(d.members()).hasSize(1);
        var m = d.members().get(0);
        assertThat(m.memberId()).isEqualTo("m-a");
        assertThat(m.subscribedTopics()).containsExactly("orders");
        assertThat(m.ownedPartitions()).hasSize(3);
    }

    @Test
    void emptyGroupAfterEveryoneLeftReportsEmptyState() {
        var c = new GroupCoordinator(t -> 2, new RangeAssignor(), instance -> "m-leaver");
        c.join("g1", "", Set.of("t"), 30_000L, 60_000, 0L);
        c.leave("g1", "m-leaver");

        var detail = c.describeGroup("g1");
        assertThat(detail).isPresent();
        assertThat(detail.get().state()).isEqualTo("Empty");
        assertThat(detail.get().members()).isEmpty();
    }
}
