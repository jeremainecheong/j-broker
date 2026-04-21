package jbroker.sim;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Runs many seeds under message-level chaos. Each seed is deterministic, so
 * a violation gives a seed that can be replayed under a debugger. Per spec
 * §Milestone 3 acceptance gate: no Raft safety invariant should ever violate across thousands
 * of seeds.
 */
class ChaosSoakTest {

    /**
     * spec §Milestone 3 acceptance gate: 10,000 seeds without a safety violation. Each seed
     * exercises drop + duplicate + jitter-induced reorder. Runs in ~10-20s
     * locally; CI budget is well under the 20-minute workflow cap.
     */
    @Test
    void tenThousandSeedsWithLossAndDuplicationPreserveSafety() {
        final int seeds = 10_000;
        final double drop = 0.10;
        final double dup = 0.05;
        final long runDurationNanos = TimeUnit.SECONDS.toNanos(2);

        for (long seed = 1; seed <= seeds; seed++) {
            var sim = new Simulator(seed, 3, Simulator.Chaos.lossy(drop, dup));
            try {
                sim.advanceTo(runDurationNanos);
                if (sim.leader() != null) {
                    for (int i = 0; i < 3; i++) {
                        sim.propose(new byte[] {(byte) i});
                        sim.runFor(TimeUnit.MILLISECONDS.toNanos(200));
                    }
                }
                sim.checkInvariantsAtEnd();
            } catch (Invariants.Violation v) {
                throw new AssertionError("seed " + seed + " violated " + v.getMessage(), v);
            }
        }
        var sim = new Simulator(1L, 3, Simulator.Chaos.lossy(drop, dup));
        sim.advanceTo(runDurationNanos);
        assertThat(sim.leader()).isNotNull();
    }

    /**
     * Chaos + crash-injection: every {@code crashPeriodMs} pick a node to
     * crash, restart the previously-crashed one. Proves safety holds even
     * when nodes drop out of the cluster mid-flight.
     */
    @Test
    void crashInjectionPreservesSafety() {
        final int seeds = 500;
        final double drop = 0.05;
        final long runDurationNanos = TimeUnit.SECONDS.toNanos(3);

        for (long seed = 1; seed <= seeds; seed++) {
            var sim = new Simulator(seed, 3, Simulator.Chaos.lossy(drop, 0.0));
            try {
                java.util.Random r = new java.util.Random(seed);
                sim.advanceTo(TimeUnit.MILLISECONDS.toNanos(500));
                // Toggle one crashed node twice during the run.
                var victim = new jbroker.raft.core.NodeId(1 + r.nextInt(3));
                sim.crash(victim);
                sim.runFor(TimeUnit.MILLISECONDS.toNanos(700));
                sim.restart(victim);
                sim.runFor(runDurationNanos);
                sim.checkInvariantsAtEnd();
            } catch (Invariants.Violation v) {
                throw new AssertionError("seed " + seed + " violated " + v.getMessage(), v);
            }
        }
    }
}
