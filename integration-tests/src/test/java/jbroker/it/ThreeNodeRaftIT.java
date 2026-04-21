package jbroker.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ThreeNodeRaftIT {

    @Test
    void threeNodeClusterElectsLeaderWithinOneSecond(@TempDir Path dir) throws Exception {
        try (var cluster = ClusterHarness.start(dir, 3)) {
            var leader = cluster.waitForLeader(1_000);
            assertThat(leader.id().value()).isBetween(1, 3);
        }
    }

    @Test
    void killingNonLeaderDoesNotDisruptCluster(@TempDir Path dir) throws Exception {
        try (var cluster = ClusterHarness.start(dir, 3)) {
            var leader = cluster.waitForLeader(1_000);
            var victim = cluster.nodes().stream()
                    .filter(n -> !n.id().equals(leader.id()))
                    .findFirst()
                    .orElseThrow();
            cluster.killNode(victim.id());

            Thread.sleep(1_500);

            assertThat(leader.driver().role()).isEqualTo(jbroker.raft.core.Role.LEADER);
        }
    }

    @Test
    void killingLeaderPromotesNewLeaderWithinFiveSeconds(@TempDir Path dir) throws Exception {
        try (var cluster = ClusterHarness.start(dir, 3)) {
            var firstLeader = cluster.waitForLeader(1_000);
            cluster.killNode(firstLeader.id());

            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline) {
                var newLeader = cluster.nodes().stream()
                        .filter(n -> !n.id().equals(firstLeader.id()))
                        .filter(n -> n.driver().role() == jbroker.raft.core.Role.LEADER)
                        .findFirst();
                if (newLeader.isPresent()) {
                    assertThat(newLeader.get().driver().currentTerm().value())
                            .isGreaterThan(firstLeader.driver().currentTerm().value());
                    return;
                }
                Thread.sleep(50);
            }
            throw new AssertionError("no new leader emerged within 5s");
        }
    }
}
