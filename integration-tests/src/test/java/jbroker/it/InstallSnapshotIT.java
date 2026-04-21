package jbroker.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end InstallSnapshot: a fresh node that joins a cluster whose leader
 * has already compacted its log must be brought up to date via an
 * {@code InstallSnapshot} RPC rather than by replaying entries that no
 * longer exist on the leader.
 */
class InstallSnapshotIT {

    @Test
    void freshNodeBootstrapsViaInstallSnapshot(@TempDir Path dir) throws Exception {
        final int initialEntries = 20;

        try (var cluster = ClusterHarness.start(dir, 3)) {
            var leader = cluster.waitForLeader(1_000);

            for (int i = 0; i < initialEntries; i++) {
                leader.driver().propose(new byte[] {(byte) i});
            }
            cluster.waitForAllApplied(initialEntries, 5_000);

            // Snapshot on the leader — this stores the SM bytes and
            // truncates the leader's log prefix up to commitIndex. Any
            // follower whose nextIndex lands below lastIncludedIndex must
            // now be caught up via InstallSnapshot.
            leader.driver().snapshotNow();
            Thread.sleep(200); // let the TakeSnapshot event process

            // Pick a non-leader node and restart it with a fresh data dir:
            // empty log, empty state. It binds on the same port so the
            // leader's existing peer client reconnects.
            var victim = cluster.nodes().stream()
                    .filter(n -> !n.id().equals(leader.id()))
                    .findFirst()
                    .orElseThrow();
            var fresh = cluster.restartAsFresh(victim.id());

            // With an empty log, the fresh node will respond to AE with
            // conflictIndex=1, which is < leader's lastIncludedIndex. The
            // leader falls back to InstallSnapshot, restoring the fresh
            // node's state machine.
            cluster.waitForNodeApplied(fresh.id(), initialEntries, 10_000);

            // Propose one more entry; the fresh node should pick it up via
            // regular AE replication now that its nextIndex has advanced
            // past the compaction point.
            leader.driver().propose(new byte[] {99});
            cluster.waitForNodeApplied(fresh.id(), initialEntries + 1, 5_000);

            assertThat(fresh.sm().applied).hasSizeGreaterThanOrEqualTo(initialEntries + 1);
        }
    }
}
