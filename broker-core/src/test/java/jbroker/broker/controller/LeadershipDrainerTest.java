package jbroker.broker.controller;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import jbroker.broker.TopicManager;
import org.junit.jupiter.api.Test;

class LeadershipDrainerTest {

    private static TopicManager topics() {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 4, 3, 0L);
        return tm;
    }

    @Test
    void handsOffEveryLedPartitionPreferringThePreferredReplica() {
        var tm = topics();
        // p0: self(1) leads, preferred replica is 2 → hand to 2.
        tm.onPartitionChange("orders", 0, 1, List.of(1, 2, 3), List.of(2, 1, 3), 5, 0);
        // p1: self leads AND is preferred → first other ISR member (3).
        tm.onPartitionChange("orders", 1, 1, List.of(1, 3), List.of(1, 2, 3), 2, 0);
        // p2: led by another broker → untouched.
        tm.onPartitionChange("orders", 2, 2, List.of(1, 2, 3), List.of(2, 1, 3), 1, 0);

        var proposals = LeadershipDrainer.proposeDrain(tm, 1);

        assertThat(proposals)
                .containsExactlyInAnyOrder(
                        new LeadershipDrainer.Proposal("orders", 0, 2, 6),
                        new LeadershipDrainer.Proposal("orders", 1, 3, 3));
    }

    @Test
    void soleIsrMemberIsNeverHandedOff() {
        var tm = topics();
        // Followers fell out of sync — promoting one would be exactly the
        // shorter-log promotion ISR-only election exists to prevent.
        tm.onPartitionChange("orders", 0, 1, List.of(1), List.of(1, 2, 3), 4, 0);

        assertThat(LeadershipDrainer.proposeDrain(tm, 1)).isEmpty();
    }

    @Test
    void preferredReplicaOutOfIsrFallsBackToAnInSyncMember() {
        var tm = topics();
        // Preferred (2) is lagging; 3 is in sync.
        tm.onPartitionChange("orders", 0, 1, List.of(1, 3), List.of(2, 1, 3), 7, 0);

        assertThat(LeadershipDrainer.proposeDrain(tm, 1))
                .containsExactly(new LeadershipDrainer.Proposal("orders", 0, 3, 8));
    }
}
