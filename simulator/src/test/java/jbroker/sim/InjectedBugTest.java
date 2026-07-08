package jbroker.sim;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.PersistentState;
import jbroker.raft.core.Term;
import org.junit.jupiter.api.Test;

/**
 * Validates that the simulator's invariant checkers can catch a deliberately
 * broken Raft implementation within a bounded number of seeds (acceptance
 * gate: "deliberately injected bug caught by simulator within 1000 seeds").
 *
 * <p>The bug: persist {@code currentTerm} but drop {@code votedFor}. A node
 * can then vote for multiple candidates at the same term — classic Raft
 * safety break — which should manifest as an election-safety or
 * log-matching violation.
 */
class InjectedBugTest {

    /** Persistent state that silently drops votedFor writes. */
    static final class VotedForLosingState implements PersistentState {
        private Term term = Term.ZERO;

        @Override
        public Term currentTerm() {
            return term;
        }

        @Override
        public Optional<NodeId> votedFor() {
            return Optional.empty();
        }

        @Override
        public void update(Term term, Optional<NodeId> vote) {
            this.term = term;
            // votedFor silently dropped — the bug.
        }
    }

    @Test
    void buggyVotedForLossIsCaughtByInvariants() {
        final int maxSeeds = 1_000;
        final long runDurationNanos = TimeUnit.SECONDS.toNanos(2);

        for (long seed = 1; seed <= maxSeeds; seed++) {
            var sim = new Simulator(seed, 3, Simulator.Chaos.lossy(0.15, 0.0), VotedForLosingState::new);
            try {
                sim.advanceTo(runDurationNanos);
                sim.checkInvariantsAtEnd();
            } catch (Invariants.Violation v) {
                assertThat(v.getMessage()).isNotNull();
                System.out.println("injected bug caught at seed " + seed + ": " + v.getMessage());
                return;
            }
        }
        throw new AssertionError(
                "injected votedFor-losing bug NOT caught within " + maxSeeds + " seeds — simulator is too weak");
    }
}
