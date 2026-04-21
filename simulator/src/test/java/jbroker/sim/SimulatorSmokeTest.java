package jbroker.sim;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class SimulatorSmokeTest {

    @Test
    void threeNodeHappyPathElectsLeaderAndReplicatesOneEntry() {
        var sim = new Simulator(42L, 3);

        // A couple election timeouts' worth of simulated time should be plenty
        // to get a leader elected in the happy-path (no chaos yet).
        sim.advanceTo(TimeUnit.MILLISECONDS.toNanos(2_000));
        var leader = sim.leader();
        assertThat(leader).isNotNull();

        // Propose and give time for replication + commit + apply.
        assertThat(sim.propose(new byte[] {1, 2, 3})).isTrue();
        sim.runFor(TimeUnit.MILLISECONDS.toNanos(500));

        for (var n : sim.nodes().values()) {
            // Leader emits a NO_OP on election win (Raft §8), so the applied
            // list contains NO_OP + the client's NORMAL entry.
            var normal = n.sm.applied.stream()
                    .filter(e -> e.type() == jbroker.raft.core.LogEntry.Type.NORMAL)
                    .toList();
            assertThat(normal).hasSize(1);
            assertThat(normal.get(0).payload()).containsExactly(1, 2, 3);
        }
    }

    @Test
    void sameSeedProducesSameLeader() {
        var simA = new Simulator(7L, 3);
        var simB = new Simulator(7L, 3);
        simA.advanceTo(TimeUnit.MILLISECONDS.toNanos(2_000));
        simB.advanceTo(TimeUnit.MILLISECONDS.toNanos(2_000));
        assertThat(simA.leader().id).isEqualTo(simB.leader().id);
        assertThat(simA.leader().core.currentTerm())
                .isEqualTo(simB.leader().core.currentTerm());
    }
}
