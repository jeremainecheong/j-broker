package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * P6.5.a DoD gate. Three in-process brokers; one replicated topic;
 * 200 records produced on the leader; assert all 3 replicas' logs are
 * byte-identical once replication catches up.
 */
class MultiBrokerReplicationIT {

    @Test
    void threeBrokerReplicatedTopicConvergesByteIdentically(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
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
            // Broker registrations commit ~1s after election (ticker delay);
            // wait until the leader knows every peer so AdminHandler picks
            // all 3 brokers as replicas.
            awaitRegistryConvergence(List.of(br1, br2, br3));

            // Send CreateTopic + produce through the Raft leader's broker
            // port (AdminHandler doesn't forward proposals to the leader;
            // clients talk to whoever is currently leader).
            var leader = leaderOf(List.of(br1, br2, br3));
            int leaderPort = leader.brokerPort();

            try (var client = new BrokerClient("127.0.0.1", leaderPort)) {
                client.createTopic("replicated", 1, /*rf*/ 3);
                int n = 200;
                for (int i = 0; i < n; i++) {
                    client.produce("replicated", 0, ("msg-" + i).getBytes(StandardCharsets.UTF_8));
                }
            }

            // Poll until all 3 logs report nextOffset == 200.
            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline) {
                long o1 = br1.logManager().logFor("replicated", 0).nextOffset();
                long o2 = br2.logManager().logFor("replicated", 0).nextOffset();
                long o3 = br3.logManager().logFor("replicated", 0).nextOffset();
                if (o1 == 200 && o2 == 200 && o3 == 200) break;
                Thread.sleep(100);
            }
            assertThat(br1.logManager().logFor("replicated", 0).nextOffset()).isEqualTo(200);
            assertThat(br2.logManager().logFor("replicated", 0).nextOffset()).isEqualTo(200);
            assertThat(br3.logManager().logFor("replicated", 0).nextOffset()).isEqualTo(200);

            var p1 = readAll(br1);
            var p2 = readAll(br2);
            var p3 = readAll(br3);
            assertThat(p1).hasSize(200);
            assertThat(p2).isEqualTo(p1);
            assertThat(p3).isEqualTo(p1);
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
            boolean allKnowAll = brokers.stream()
                    .allMatch(b -> b.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3)));
            if (allKnowAll) return;
            Thread.sleep(50);
        }
        throw new AssertionError("broker registry did not converge within 5s");
    }

    private static Broker leaderOf(List<Broker> brokers) {
        return brokers.stream().filter(b -> b.role() == Role.LEADER).findFirst().orElseThrow();
    }

    private static List<String> readAll(Broker b) throws Exception {
        var log = b.logManager().logFor("replicated", 0);
        var out = new ArrayList<String>();
        long offset = 0;
        while (offset < log.nextOffset()) {
            var batches = log.read(offset, 1 << 20);
            if (batches.isEmpty()) break;
            for (var batch : batches) {
                for (var rec : batch.records()) {
                    out.add(new String(rec.value(), StandardCharsets.UTF_8));
                }
                offset = batch.baseOffset() + batch.lastOffsetDelta() + 1;
            }
        }
        return out;
    }
}
