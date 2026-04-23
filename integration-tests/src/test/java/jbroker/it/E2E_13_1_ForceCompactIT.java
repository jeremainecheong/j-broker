package jbroker.it;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import jbroker.app.Broker;
import jbroker.app.VoterAddress;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import jbroker.storage.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * P13.1 — prove post-compaction correctness end-to-end via the gRPC fetch
 * path, not just the Log-layer contract that {@code LogCompactionTest}
 * already covers.
 *
 * <p>Scenario: create a compact-policy topic, append 100 records across 5
 * keys (round-robin), force-compact via the new admin RPC, then fetch via
 * {@link BrokerClient#fetchRecords} and assert the survivors arrive at
 * their original absolute offsets {95, 96, 97, 98, 99}. Sparse-offset
 * preservation is the subtle property — pre-P12.4 compaction reset
 * offsetDeltas to contiguous values, which would have broken consumers
 * holding stale pre-compaction offsets.
 */
class E2E_13_1_ForceCompactIT {

    private static final int TOTAL_RECORDS = 100;
    private static final int DISTINCT_KEYS = 5;

    @Test
    void forceCompactPreservesSparseOffsetsAcrossGrpcFetch(@TempDir Path dir) throws Exception {
        int brokerPort = freePort();
        int raftPort = freePort();
        var voters = List.of(new VoterAddress(new NodeId(1), "127.0.0.1", raftPort, brokerPort));

        try (var broker = Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort, voters))) {
            awaitLeadership(broker);

            try (var client = new BrokerClient("127.0.0.1", brokerPort)) {
                client.createTopicWithConfig("prices", 1, 1, Map.of("cleanup.policy", "compact"));

                // Seed 100 keyed records directly at the Log layer — the
                // producer gRPC path doesn't expose keys today, and the
                // slice cares about compaction + fetch correctness, not
                // how records arrive. Each append is a single-record batch
                // so absolute offsets are 0..99 and the last write per key
                // lands at 95/96/97/98/99.
                var log = broker.logManager().logFor("prices", 0);
                for (int i = 0; i < TOTAL_RECORDS; i++) {
                    String key = "k" + (i % DISTINCT_KEYS);
                    String value = "v" + i;
                    log.append(
                            List.of(new Record(0, 0L, key.getBytes(UTF_8), value.getBytes(UTF_8))),
                            System.currentTimeMillis());
                }

                int kept = client.forceCompactPartition("prices", 0);
                assertThat(kept)
                        .as("survivor count equals the distinct-key count")
                        .isEqualTo(DISTINCT_KEYS);

                var fetched = client.fetchRecords("prices", 0, 0, 4 * 1024 * 1024);
                assertThat(fetched).hasSize(DISTINCT_KEYS);

                var valuesByKey = new java.util.HashMap<String, String>();
                var offsets = new java.util.ArrayList<Long>();
                for (var rec : fetched) {
                    offsets.add(rec.offset());
                    valuesByKey.put(new String(rec.key(), UTF_8), new String(rec.value(), UTF_8));
                }

                assertThat(offsets)
                        .as("sparse absolute offsets survive the compact-then-fetch round trip")
                        .containsExactlyInAnyOrder(95L, 96L, 97L, 98L, 99L);

                assertThat(valuesByKey)
                        .as("each key retains its latest value")
                        .containsEntry("k0", "v95")
                        .containsEntry("k1", "v96")
                        .containsEntry("k2", "v97")
                        .containsEntry("k3", "v98")
                        .containsEntry("k4", "v99");
            }
        }
    }

    @Test
    void forceCompactReturnsUnknownTopicForMissingTopic(@TempDir Path dir) throws Exception {
        int brokerPort = freePort();
        int raftPort = freePort();
        var voters = List.of(new VoterAddress(new NodeId(1), "127.0.0.1", raftPort, brokerPort));

        try (var broker = Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort, voters))) {
            awaitLeadership(broker);

            try (var client = new BrokerClient("127.0.0.1", brokerPort)) {
                assertThatThrownByForceCompact(client, "does-not-exist", 0);
            }
        }
    }

    @Test
    void forceCompactOnEmptyLogReturnsZeroKept(@TempDir Path dir) throws Exception {
        int brokerPort = freePort();
        int raftPort = freePort();
        var voters = List.of(new VoterAddress(new NodeId(1), "127.0.0.1", raftPort, brokerPort));

        try (var broker = Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort, voters))) {
            awaitLeadership(broker);

            try (var client = new BrokerClient("127.0.0.1", brokerPort)) {
                client.createTopicWithConfig("never-written", 1, 1, Map.of("cleanup.policy", "compact"));
                // Single broker hosts the partition so the log IS opened on
                // topic-create; compacting an empty log returns 0 survivors
                // (no error). The -1 sentinel is reserved for multi-broker
                // deployments where a non-hosting broker answers the
                // fan-out — this IT can't exercise that leg on its own.
                int kept = client.forceCompactPartition("never-written", 0);
                assertThat(kept).isEqualTo(0);
            }
        }
    }

    @Test
    void forceCompactReturnsInvalidPartitionForOutOfRange(@TempDir Path dir) throws Exception {
        int brokerPort = freePort();
        int raftPort = freePort();
        var voters = List.of(new VoterAddress(new NodeId(1), "127.0.0.1", raftPort, brokerPort));

        try (var broker = Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort, voters))) {
            awaitLeadership(broker);

            try (var client = new BrokerClient("127.0.0.1", brokerPort)) {
                client.createTopicWithConfig("one-part", 1, 1, Map.of("cleanup.policy", "compact"));
                try {
                    client.forceCompactPartition("one-part", 7);
                    throw new AssertionError("expected out-of-range partition to fail");
                } catch (RuntimeException expected) {
                    assertThat(expected.getMessage()).contains("partition out of range");
                }
            }
        }
    }

    private static void assertThatThrownByForceCompact(BrokerClient client, String topic, int partition) {
        try {
            client.forceCompactPartition(topic, partition);
            throw new AssertionError("expected forceCompactPartition to fail for unknown topic");
        } catch (RuntimeException expected) {
            assertThat(expected.getMessage()).contains("unknown topic");
        }
    }

    private static void awaitLeadership(Broker broker) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (broker.role() != Role.LEADER && System.currentTimeMillis() < deadline) {
            Thread.sleep(25);
        }
        assertThat(broker.role()).isEqualTo(Role.LEADER);
    }

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
