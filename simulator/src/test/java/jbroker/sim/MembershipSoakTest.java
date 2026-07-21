package jbroker.sim;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import jbroker.raft.core.Membership;
import jbroker.raft.core.NodeId;
import org.junit.jupiter.api.Test;

/**
 * Membership-change safety (Raft dissertation §4.2), proven in the
 * deterministic simulator before any of this is wired into the broker
 * cluster (R4.2). Each seed drives single-server changes — add a spare as a
 * learner, promote it to a voter, remove another voter — interleaved with
 * the election timeouts, message loss, and node crashes that make the
 * changes race real Raft activity. The four safety invariants (election
 * safety, state-machine safety, log matching, commit monotonicity) must
 * never violate; a violation prints the seed, which replays bit-for-bit.
 */
class MembershipSoakTest {

    /**
     * Acceptance gate: 10,000 seeds of add-learner → promote → remove under
     * message loss, with no safety violation. Deterministic, so any failure
     * yields a replayable seed.
     */
    @Test
    void tenThousandSeedsOfMembershipChurnPreserveSafety() {
        final int seeds = 10_000;
        final double drop = 0.05;

        for (long seed = 1; seed <= seeds; seed++) {
            // 3 voters {1,2,3} + 2 spares {4,5} available to add.
            var sim = Simulator.withSpares(seed, 3, 2, Simulator.Chaos.lossy(drop, 0.0));
            try {
                driveMembershipCycle(sim, seed);
                sim.checkInvariantsAtEnd();
            } catch (Invariants.Violation v) {
                throw new AssertionError("seed " + seed + " violated " + v.getMessage(), v);
            }
        }
    }

    /**
     * Membership changes racing crashes: a voter drops out mid-change, so the
     * add/promote/remove sequence must contend with an unavailable node and a
     * possible leader election on top of the config churn.
     */
    @Test
    void membershipChurnUnderCrashesPreservesSafety() {
        final int seeds = 500;
        final double drop = 0.05;

        for (long seed = 1; seed <= seeds; seed++) {
            var sim = Simulator.withSpares(seed, 3, 2, Simulator.Chaos.lossy(drop, 0.0));
            var r = new Random(seed ^ 0x9E3779B97F4A7C15L);
            try {
                sim.advanceTo(TimeUnit.MILLISECONDS.toNanos(600));
                // Begin adding spare 4 as a learner.
                sim.proposeMembership(new Membership(List.of(N(1), N(2), N(3)), List.of(N(4))));
                sim.runFor(TimeUnit.MILLISECONDS.toNanos(300));
                // Crash a random voter mid-change; keep running so an election
                // can happen while the learner is still catching up.
                var victim = N(1 + r.nextInt(3));
                sim.crash(victim);
                sim.runFor(TimeUnit.SECONDS.toNanos(1));
                sim.restart(victim);
                sim.runFor(TimeUnit.SECONDS.toNanos(1));
                sim.checkInvariantsAtEnd();
            } catch (Invariants.Violation v) {
                throw new AssertionError("seed " + seed + " violated " + v.getMessage(), v);
            }
        }
    }

    /**
     * With no chaos, the happy-path membership cycle actually converges:
     * the learner is promoted into the voter set, so the change is not just
     * safe but makes progress.
     */
    @Test
    void addingAndPromotingALearnerConverges() {
        var sim = Simulator.withSpares(7L, 3, 1, Simulator.Chaos.noChaos());
        sim.advanceTo(TimeUnit.MILLISECONDS.toNanos(1_000));
        assertThat(sim.leader()).isNotNull();

        // Add 4 as a learner, let it catch up, then promote it to a voter.
        assertThat(sim.proposeMembership(new Membership(List.of(N(1), N(2), N(3)), List.of(N(4)))))
                .isTrue();
        sim.runFor(TimeUnit.MILLISECONDS.toNanos(800));
        sim.proposeMembership(Membership.ofVoters(List.of(N(1), N(2), N(3), N(4))));
        sim.runFor(TimeUnit.SECONDS.toNanos(1));

        assertThat(sim.leaderVoters()).containsExactlyInAnyOrder(N(1), N(2), N(3), N(4));
        sim.checkInvariantsAtEnd();
    }

    private static void driveMembershipCycle(Simulator sim, long seed) {
        var r = new Random(seed ^ 0x2545F4914F6CDD1DL);
        sim.advanceTo(TimeUnit.MILLISECONDS.toNanos(400 + r.nextInt(400)));

        // Add spare 4 as a learner.
        sim.proposeMembership(new Membership(List.of(N(1), N(2), N(3)), List.of(N(4))));
        sim.runFor(TimeUnit.MILLISECONDS.toNanos(300));
        // A little client load through the change.
        sim.propose(new byte[] {(byte) r.nextInt()});
        sim.runFor(TimeUnit.MILLISECONDS.toNanos(300));

        // Promote 4 to a voter (only applies once the add committed; a
        // still-in-flight change is rejected and the set stays put — safe).
        sim.proposeMembership(Membership.ofVoters(List.of(N(1), N(2), N(3), N(4))));
        sim.runFor(TimeUnit.MILLISECONDS.toNanos(400));

        // Remove voter 1 (which may be the leader — self-removal steps down
        // after commit, exercising that path too).
        sim.proposeMembership(Membership.ofVoters(List.of(N(2), N(3), N(4))));
        sim.runFor(TimeUnit.MILLISECONDS.toNanos(600));
    }

    private static NodeId N(int id) {
        return new NodeId(id);
    }
}
