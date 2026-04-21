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

    @Test
    void replicatesThousandEntriesAndAllNodesConverge(@TempDir Path dir) throws Exception {
        try (var cluster = ClusterHarness.start(dir, 3)) {
            var leader = cluster.waitForLeader(1_000);
            for (int i = 0; i < 1000; i++) {
                leader.driver().propose(new byte[] {(byte) i, (byte) (i >>> 8)});
            }

            // 30s budget: local hardware converges in ~2–3s, but GitHub Actions
            // runners under load need significantly more; 10s was too tight and
            // caused spurious CI flakes.
            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline) {
                boolean allThere =
                        cluster.nodes().stream().allMatch(n -> n.sm().applied.size() >= 1000);
                if (allThere) break;
                Thread.sleep(100);
            }

            for (var n : cluster.nodes()) {
                assertThat(n.sm().applied.size()).isGreaterThanOrEqualTo(1000);
            }
        }
    }
}
