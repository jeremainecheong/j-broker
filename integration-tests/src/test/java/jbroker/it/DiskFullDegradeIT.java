package jbroker.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.List;
import jbroker.app.Broker;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Disk-headroom degrade (R2.3), end to end. The watermark is set far above
 * the volume's actual free space, so every broker boots straight into the
 * degraded mode — the honest way to exercise a "full" disk without loop
 * devices. Client produces must fail fast with retriable STORAGE_FULL
 * while the control plane (topic creation through Raft), fetch, and the
 * metrics surface keep working. Recovery is probe-driven and covered at
 * the unit level, where the space supplier is injectable.
 */
class DiskFullDegradeIT {

    @Test
    void degradedBrokerRefusesProduceButKeepsServingEverythingElse(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        try (var cluster = TestBrokerCluster.start(3, 3, (i, voters, ports) -> new Broker.Config(
                        new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters)
                .withChaosPort(ports[i][2])
                .withStorageHeadroomBytes(Long.MAX_VALUE / 4))) {
            var brokers = List.of(cluster.broker(0), cluster.broker(1), cluster.broker(2));
            int[] brokerPorts = {cluster.brokerPort(0), cluster.brokerPort(1), cluster.brokerPort(2)};
            waitForClusterReady(brokers);

            // Admin path is unaffected: the topic commits through Raft
            // (metadata appends are not gated) and replicates everywhere.
            try (var client = new BrokerClient("127.0.0.1", pickLeaderPort(brokers, brokerPorts))) {
                client.createTopic("degraded", 1, 3);
            }
            int leaderId = partitionLeaderId(brokers, "degraded", 0);

            try (var client = new BrokerClient("127.0.0.1", brokerPorts[leaderId - 1])) {
                // Client produces are refused pre-append, acks=1 and
                // acks=all alike, well inside the replication timeout.
                long start = System.nanoTime();
                assertThatThrownBy(() -> client.produce("degraded", 0, new byte[64]))
                        .isInstanceOf(RuntimeException.class)
                        .hasMessageContaining("headroom");
                assertThatThrownBy(() -> client.produceAcksAll("degraded", 0, new byte[64]))
                        .isInstanceOf(RuntimeException.class)
                        .hasMessageContaining("headroom");
                long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                assertThat(elapsedMs)
                        .as("STORAGE_FULL is a fast pre-append rejection")
                        .isLessThan(3_000L);

                // Fetch keeps serving (nothing was appended).
                assertThat(client.fetchAll("degraded", 0, 1 << 20)).isEmpty();

                // The degraded state is visible on the metrics surface.
                var metrics = client.describeMetrics();
                assertThat(metrics.getDiskHeadroomLow()).isTrue();
                assertThat(metrics.getDiskUsableBytes()).isGreaterThan(0L);
            }
        }
    }

    // ---------- helpers ----------

    private static int pickLeaderPort(List<Broker> brokers, int[] ports) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            for (int i = 0; i < brokers.size(); i++) {
                if (brokers.get(i).role() == Role.LEADER) return ports[i];
            }
            Thread.sleep(50);
        }
        throw new AssertionError("no Raft leader within 10s");
    }

    private static int partitionLeaderId(List<Broker> brokers, String topic, int partition)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            for (var b : brokers) {
                var state = b.topics().partitionState(topic, partition).orElse(null);
                if (state != null && state.leader() > 0) return state.leader();
            }
            Thread.sleep(50);
        }
        throw new AssertionError("no partition leader for " + topic + "-" + partition + " within 10s");
    }

    private static void waitForClusterReady(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            long leaders = brokers.stream().filter(b -> b.role() == Role.LEADER).count();
            boolean full = brokers.stream()
                    .allMatch(b -> b.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3)));
            if (leaders == 1 && full) return;
            Thread.sleep(50);
        }
        throw new IllegalStateException("cluster did not converge in 15s");
    }
}
