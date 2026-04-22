package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import jbroker.raft.core.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * P6.5.b sanity: 3 brokers send point-to-point BrokerHeartbeat RPCs and
 * every broker's BrokerLiveness map converges with fresh entries for all
 * three broker IDs. P6.5.c's fencer builds on this.
 */
class MultiBrokerHeartbeatIT {

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void threeBrokerLivenessMapConverges(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3) throws Exception {
        int r1 = freePort(), r2 = freePort(), r3 = freePort();
        int b1 = freePort(), b2 = freePort(), b3 = freePort();
        var voters = List.of(
                new VoterAddress(new NodeId(1), "127.0.0.1", r1, b1),
                new VoterAddress(new NodeId(2), "127.0.0.1", r2, b2),
                new VoterAddress(new NodeId(3), "127.0.0.1", r3, b3));

        try (var br1 = Broker.start(new Broker.Config(new NodeId(1), d1, r1, b1, voters));
                var br2 = Broker.start(new Broker.Config(new NodeId(2), d2, r2, b2, voters));
                var br3 = Broker.start(new Broker.Config(new NodeId(3), d3, r3, b3, voters))) {

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
