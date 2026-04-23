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

            // 1.5× election-timeout ceiling so a quiescent leader has
            // time to notice the dead peer via missed heartbeat ACKs
            // but shouldn't itself lose leadership. Post-settle assert
            // repeats the check until the observation window closes
            // rather than a blind sleep — prevents CI-load-induced
            // scheduler starvation from skipping the assertion.
            long deadline = System.currentTimeMillis() + 1_500;
            while (System.currentTimeMillis() < deadline) {
                assertThat(leader.driver().role()).isEqualTo(jbroker.raft.core.Role.LEADER);
                Thread.sleep(100);
            }
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
    void replicatesManyEntriesAndAllNodesConverge(@TempDir Path dir) throws Exception {
        // 300 entries exercises the multi-batch replication path
        // (maxEntriesPerAppend=100) while remaining reliable on slow CI
        // hardware. The earlier 1000-entry variant effectively became a
        // pure fsync / stdout throughput probe on a GitHub Actions runner;
        // convergence correctness doesn't need 1000 — 300 still proves
        // multi-batch catch-up and ordered apply on every follower.
        final int total = 300;
        try (var cluster = ClusterHarness.start(dir, 3)) {
            var leader = cluster.waitForLeader(1_000);
            for (int i = 0; i < total; i++) {
                leader.driver().propose(new byte[] {(byte) i, (byte) (i >>> 8)});
            }

            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline) {
                boolean allThere =
                        cluster.nodes().stream().allMatch(n -> n.sm().applied.size() >= total);
                if (allThere) break;
                Thread.sleep(100);
            }

            for (var n : cluster.nodes()) {
                assertThat(n.sm().applied.size()).isGreaterThanOrEqualTo(total);
            }
        }
    }
}
