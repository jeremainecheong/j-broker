package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * sanity: 3 in-process brokers with a shared static voter list boot,
 * elect a single Raft leader, and run far enough that every broker's state
 * machine settles at the same term. The replication acceptance gate gate is in
 * {@link MultiBrokerReplicationIT}; this IT only covers metadata-layer
 * wiring.
 */
class MultiBrokerStartupIT {

    @Test
    void threeBrokerClusterElectsSingleLeader(@TempDir Path dir1, @TempDir Path dir2, @TempDir Path dir3)
            throws Exception {
        var dirs = new Path[] {dir1, dir2, dir3};
        try (var cluster = TestBrokerCluster.start(
                3,
                2,
                (i, voters, ports) ->
                        new Broker.Config(new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters))) {
            var br1 = cluster.broker(0);
            var br2 = cluster.broker(1);
            var br3 = cluster.broker(2);

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
