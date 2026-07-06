package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * P6.6 DoD gate: acks=all produces block until every ISR member has
 * replicated the record. Verified end-to-end in a 3-broker cluster by
 * issuing {@code produceAcksAll} calls and reading the same offsets
 * back on every replica.
 */
class MultiBrokerAcksAllIT {

    @Test
    void acksAllOnlyReturnsWhenEveryReplicaHasTheRecord(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        try (var cluster = TestBrokerCluster.start(
                3,
                2,
                (i, voters, ports) ->
                        new Broker.Config(new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters))) {
            var br1 = cluster.broker(0);
            var br2 = cluster.broker(1);
            var br3 = cluster.broker(2);

            awaitSingleLeader(List.of(br1, br2, br3));
            awaitRegistryConvergence(List.of(br1, br2, br3));

            var raftLeader = leaderOf(List.of(br1, br2, br3));
            try (var client = new BrokerClient("127.0.0.1", raftLeader.brokerPort())) {
                client.createTopic("durable", 1, 3);

                // Each acks=all produce returns only after the record lands
                // on all three replicas. Check invariants at each step:
                // by the time produceAcksAll returns, every broker's log
                // contains the record.
                for (int i = 0; i < 5; i++) {
                    long offset = client.produceAcksAll("durable", 0, ("strong-" + i).getBytes(StandardCharsets.UTF_8));
                    assertThat(offset).isEqualTo(i);

                    // Immediate read-back on every replica (no waiting).
                    assertThat(br1.logManager().logFor("durable", 0).nextOffset())
                            .as("br1 LEO after acks=all produce %d", i)
                            .isGreaterThanOrEqualTo(i + 1);
                    assertThat(br2.logManager().logFor("durable", 0).nextOffset())
                            .as("br2 LEO after acks=all produce %d", i)
                            .isGreaterThanOrEqualTo(i + 1);
                    assertThat(br3.logManager().logFor("durable", 0).nextOffset())
                            .as("br3 LEO after acks=all produce %d", i)
                            .isGreaterThanOrEqualTo(i + 1);
                }
            }
        }
    }

    private static void awaitSingleLeader(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            long leaders = brokers.stream().filter(b -> b.role() == Role.LEADER).count();
            if (leaders == 1) return;
            Thread.sleep(50);
        }
        throw new AssertionError("no single leader within 10s");
    }

    private static void awaitRegistryConvergence(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            boolean allKnow = brokers.stream()
                    .allMatch(b -> b.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3)));
            if (allKnow) return;
            Thread.sleep(50);
        }
        throw new AssertionError("broker registry did not converge within 5s");
    }

    private static Broker leaderOf(List<Broker> brokers) {
        return brokers.stream().filter(b -> b.role() == Role.LEADER).findFirst().orElseThrow();
    }
}
