package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.raft.core.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Sanity check: 3 brokers send point-to-point BrokerHeartbeat RPCs and
 * every broker's BrokerLiveness map converges with fresh entries for all
 * three broker IDs. The fencer builds on this.
 */
class MultiBrokerHeartbeatIT {

    @Test
    void threeBrokerLivenessMapConverges(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3) throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        try (var cluster = TestBrokerCluster.start(
                3,
                2,
                (i, voters, ports) ->
                        new Broker.Config(new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters))) {
            var br1 = cluster.broker(0);
            var br2 = cluster.broker(1);
            var br3 = cluster.broker(2);

            // Each broker hears from the other two (not from self — sender
            // skips self). 1 Hz interval × 2 peers → expect each broker to
            // see the other two within ~3s.
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                boolean br1SeesBoth = br1.brokerLiveness().lastSignal(2).isPresent()
                        && br1.brokerLiveness().lastSignal(3).isPresent();
                boolean br2SeesBoth = br2.brokerLiveness().lastSignal(1).isPresent()
                        && br2.brokerLiveness().lastSignal(3).isPresent();
                boolean br3SeesBoth = br3.brokerLiveness().lastSignal(1).isPresent()
                        && br3.brokerLiveness().lastSignal(2).isPresent();
                if (br1SeesBoth && br2SeesBoth && br3SeesBoth) break;
                Thread.sleep(100);
            }

            assertThat(br1.brokerLiveness().lastSignal(2)).isPresent();
            assertThat(br1.brokerLiveness().lastSignal(3)).isPresent();
            assertThat(br2.brokerLiveness().lastSignal(1)).isPresent();
            assertThat(br2.brokerLiveness().lastSignal(3)).isPresent();
            assertThat(br3.brokerLiveness().lastSignal(1)).isPresent();
            assertThat(br3.brokerLiveness().lastSignal(2)).isPresent();
        }
    }
}
