package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * P6.5.a sanity: 3 in-process brokers with a shared static voter list boot,
 * elect a single Raft leader, and run far enough that every broker's state
 * machine settles at the same term. The replication DoD gate is in
 * {@link MultiBrokerReplicationIT}; this IT only covers metadata-layer
 * wiring.
 */
class MultiBrokerStartupIT {

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void threeBrokerClusterElectsSingleLeader(@TempDir Path dir1, @TempDir Path dir2, @TempDir Path dir3)
            throws Exception {
        int r1 = freePort(), r2 = freePort(), r3 = freePort();
        int b1 = freePort(), b2 = freePort(), b3 = freePort();
        var voters = List.of(
                new VoterAddress(new NodeId(1), "127.0.0.1", r1, b1),
                new VoterAddress(new NodeId(2), "127.0.0.1", r2, b2),
                new VoterAddress(new NodeId(3), "127.0.0.1", r3, b3));

        try (var br1 = Broker.start(new Broker.Config(new NodeId(1), dir1, r1, b1, voters));
                var br2 = Broker.start(new Broker.Config(new NodeId(2), dir2, r2, b2, voters));
                var br3 = Broker.start(new Broker.Config(new NodeId(3), dir3, r3, b3, voters))) {

            long deadline = System.currentTimeMillis() + 10_000;
            int leaders = 0;
            while (System.currentTimeMillis() < deadline) {
                leaders = (br1.role() == Role.LEADER ? 1 : 0)
                        + (br2.role() == Role.LEADER ? 1 : 0)
                        + (br3.role() == Role.LEADER ? 1 : 0);
                if (leaders == 1) break;
                Thread.sleep(50);
            }
            assertThat(leaders).isEqualTo(1);

            long regDeadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < regDeadline) {
                boolean allKnowAll = br1.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3))
                        && br2.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3))
                        && br3.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3));
                if (allKnowAll) break;
                Thread.sleep(50);
            }
            assertThat(br1.brokerRegistry().addressFor(2)).isPresent();
            assertThat(br2.brokerRegistry().addressFor(3)).isPresent();
            assertThat(br3.brokerRegistry().addressFor(1)).isPresent();
        }
    }
}
