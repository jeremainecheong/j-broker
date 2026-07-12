package jbroker.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import jbroker.app.Broker;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Retention end to end (R2.1/R2.2): per-topic retention.bytes /
 * retention.ms / segment.bytes govern the cleaner, config updates reach
 * logs that are already open, and — the replication-facing half — a
 * follower whose resume offset was retention-deleted on the leader adopts
 * the leader's earliest batch instead of wedging.
 */
class LogRetentionIT {

    private static final long CLEANER_INTERVAL_MS = 500;
    private static final long RETENTION_TIMEOUT_MS = 20_000L;
    private static final long ISR_SHRINK_TIMEOUT_MS = 20_000L;
    private static final long ISR_EXPAND_TIMEOUT_MS = 25_000L;

    @Test
    void sizeRetentionTrimsTheLogAndReadsResumeFromTheNewStart(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        try (var cluster = TestBrokerCluster.start(3, 3, (i, voters, ports) -> new Broker.Config(
                        new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters)
                .withChaosPort(ports[i][2])
                .withLogCleanerIntervalMillis(CLEANER_INTERVAL_MS))) {
            var brokers = List.of(cluster.broker(0), cluster.broker(1), cluster.broker(2));
            int[] brokerPorts = {cluster.brokerPort(0), cluster.brokerPort(1), cluster.brokerPort(2)};
            waitForClusterReady(brokers);

            try (var client = new BrokerClient("127.0.0.1", pickLeaderPort(brokers, brokerPorts))) {
                client.createTopicWithConfig(
                        "trimmed", 1, 3, Map.of("segment.bytes", "1024", "retention.bytes", "4096"));
            }
            waitForFullIsr(brokers, "trimmed", 0);

            int leaderId = partitionLeaderId(brokers, "trimmed", 0);
            var leaderPartitionDir = dirs[leaderId - 1].resolve("topics").resolve("trimmed-0");
            try (var client = new BrokerClient("127.0.0.1", brokerPorts[leaderId - 1])) {
                for (int i = 0; i < 40; i++) {
                    client.produceAcksAll("trimmed", 0, new byte[512]);
                }

                // ~20 KiB across ~1 KiB segments against a 4 KiB budget:
                // the cleaner must delete head segments.
                awaitLogFileCountAtMost(leaderPartitionDir, 6, RETENTION_TIMEOUT_MS);

                // Reads below the trimmed range resolve to the earliest
                // survivor rather than returning empty or looping.
                var records = client.fetchRecords("trimmed", 0, 0L, 1 << 20);
                assertThat(records).isNotEmpty();
                assertThat(records.get(0).offset())
                        .as("offset 0 was retention-deleted; reads start at the new log start")
                        .isGreaterThan(0L);

                // The partition stays writable and readable after trimming.
                client.produceAcksAll("trimmed", 0, new byte[512]);
                var all = client.fetchAll("trimmed", 0, 1 << 20);
                assertThat(all).isNotEmpty();
                assertThat(all.size()).as("head records are gone").isLessThan(41);
            }
        }
    }

    @Test
    void retentionUpdateReachesAnAlreadyOpenLog(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3) throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        try (var cluster = TestBrokerCluster.start(3, 3, (i, voters, ports) -> new Broker.Config(
                        new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters)
                .withChaosPort(ports[i][2])
                .withLogCleanerIntervalMillis(CLEANER_INTERVAL_MS))) {
            var brokers = List.of(cluster.broker(0), cluster.broker(1), cluster.broker(2));
            int[] brokerPorts = {cluster.brokerPort(0), cluster.brokerPort(1), cluster.brokerPort(2)};
            waitForClusterReady(brokers);

            try (var client = new BrokerClient("127.0.0.1", pickLeaderPort(brokers, brokerPorts))) {
                client.createTopicWithConfig("aging", 1, 3, Map.of("segment.bytes", "1024"));
            }
            waitForFullIsr(brokers, "aging", 0);

            int leaderId = partitionLeaderId(brokers, "aging", 0);
            var leaderPartitionDir = dirs[leaderId - 1].resolve("topics").resolve("aging-0");
            try (var client = new BrokerClient("127.0.0.1", brokerPorts[leaderId - 1])) {
                for (int i = 0; i < 40; i++) {
                    client.produceAcksAll("aging", 0, new byte[512]);
                }
            }
            assertThat(logFileCount(leaderPartitionDir))
                    .as("several closed segments exist before the update")
                    .isGreaterThan(3);

            // No retention configured at create time — the log is open and
            // idle when the override commits through the metadata log.
            try (var client = new BrokerClient("127.0.0.1", pickLeaderPort(brokers, brokerPorts))) {
                client.updateTopicConfig("aging", Map.of("retention.ms", "1"));
            }

            // Every closed segment is now past the 1ms window; only the
            // active segment may survive.
            awaitLogFileCountAtMost(leaderPartitionDir, 1, RETENTION_TIMEOUT_MS);
        }
    }

    @Test
    void followerBehindTheRetainedLogStartCatchesUp(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        try (var cluster = TestBrokerCluster.start(3, 3, (i, voters, ports) -> new Broker.Config(
                        new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters)
                .withChaosPort(ports[i][2])
                .withLogCleanerIntervalMillis(CLEANER_INTERVAL_MS))) {
            var brokers = List.of(cluster.broker(0), cluster.broker(1), cluster.broker(2));
            int[] brokerPorts = {cluster.brokerPort(0), cluster.brokerPort(1), cluster.brokerPort(2)};
            int[] chaosPorts = {cluster.port(0, 2), cluster.port(1, 2), cluster.port(2, 2)};
            waitForClusterReady(brokers);

            try (var client = new BrokerClient("127.0.0.1", pickLeaderPort(brokers, brokerPorts))) {
                client.createTopicWithConfig(
                        "catchup", 1, 3, Map.of("segment.bytes", "1024", "retention.bytes", "2048"));
            }
            waitForFullIsr(brokers, "catchup", 0);

            int leaderId = partitionLeaderId(brokers, "catchup", 0);
            int leaderPort = brokerPorts[leaderId - 1];
            // Pause a broker that is neither the partition leader nor the
            // Raft leader, so metadata proposals keep committing while the
            // follower sleeps.
            int pausedId = pickPausableFollower(brokers, leaderId);

            // Baseline the follower at LEO 2.
            try (var client = new BrokerClient("127.0.0.1", leaderPort)) {
                client.produceAcksAll("catchup", 0, new byte[512]);
                client.produceAcksAll("catchup", 0, new byte[512]);
            }

            postChaos(chaosPorts[pausedId - 1], "/debug/chaos/pause");
            waitForIsrWithout(brokers, "catchup", 0, pausedId, ISR_SHRINK_TIMEOUT_MS);

            // Write far past the follower and let the 2 KiB budget delete
            // the range it never replicated (offsets 0..~35).
            try (var client = new BrokerClient("127.0.0.1", leaderPort)) {
                for (int i = 0; i < 40; i++) {
                    client.produceAcksAll("catchup", 0, new byte[512]);
                }
                awaitEarliestOffsetAbove(client, "catchup", 2L, RETENTION_TIMEOUT_MS);
            }

            // On resume the follower fetches from LEO 2, which the leader no
            // longer has. It must adopt the leader's earliest batch and
            // rejoin the ISR — before forward-gap appends, that fetch threw
            // on every poll and the follower stayed expelled forever.
            postChaos(chaosPorts[pausedId - 1], "/debug/chaos/resume");
            waitForIsrContaining(brokers, "catchup", 0, List.of(1, 2, 3), ISR_EXPAND_TIMEOUT_MS);

            try (var client = new BrokerClient("127.0.0.1", leaderPort)) {
                client.produceAcksAll("catchup", 0, new byte[512]);
                assertThat(client.fetchAll("catchup", 0, 1 << 20)).isNotEmpty();
            }
        }
    }

    // ---------- helpers ----------

    private static long logFileCount(Path partitionDir) throws Exception {
        if (!Files.isDirectory(partitionDir)) return 0;
        try (var stream = Files.list(partitionDir)) {
            return stream.filter(p -> p.getFileName().toString().endsWith(".log"))
                    .count();
        }
    }

    private static void awaitLogFileCountAtMost(Path partitionDir, int max, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long last = -1;
        while (System.currentTimeMillis() < deadline) {
            last = logFileCount(partitionDir);
            if (last > 0 && last <= max) return;
            Thread.sleep(200);
        }
        throw new AssertionError("cleaner left " + last + " segment files (wanted ≤ " + max + ") in " + partitionDir);
    }

    private static void awaitEarliestOffsetAbove(BrokerClient client, String topic, long floor, long timeoutMs)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long earliest = -1;
        while (System.currentTimeMillis() < deadline) {
            var records = client.fetchRecords(topic, 0, 0L, 1 << 20);
            if (!records.isEmpty()) {
                earliest = records.get(0).offset();
                if (earliest > floor) return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("earliest offset stayed at " + earliest + " (wanted > " + floor + ")");
    }

    private static int pickPausableFollower(List<Broker> brokers, int partitionLeaderId) {
        for (int id = 1; id <= 3; id++) {
            if (id != partitionLeaderId && brokers.get(id - 1).role() != Role.LEADER) return id;
        }
        throw new AssertionError("no pausable follower in a 3-broker cluster");
    }

    private static void postChaos(int chaosPort, String path) throws Exception {
        var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        HttpResponse<String> resp = http.send(
                HttpRequest.newBuilder()
                        .uri(URI.create("http://127.0.0.1:" + chaosPort + path))
                        .POST(HttpRequest.BodyPublishers.noBody())
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new AssertionError(path + " → HTTP " + resp.statusCode() + ": " + resp.body());
        }
    }

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

    private static void waitForFullIsr(List<Broker> brokers, String topic, int partition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            boolean allSee3 = brokers.stream().allMatch(b -> {
                var state = b.topics().partitionState(topic, partition).orElse(null);
                return state != null && state.isr().size() == 3;
            });
            if (allSee3) return;
            Thread.sleep(100);
        }
        throw new AssertionError("ISR did not reach {1,2,3} within 15s for " + topic + "-" + partition);
    }

    private static void waitForIsrWithout(
            List<Broker> brokers, String topic, int partition, int expelledBrokerId, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean shrunkEverywhere = brokers.stream().allMatch(b -> {
                var state = b.topics().partitionState(topic, partition).orElse(null);
                return state != null && !state.isr().contains(expelledBrokerId);
            });
            if (shrunkEverywhere) return;
            Thread.sleep(100);
        }
        throw new AssertionError(
                "ISR did not shrink to exclude broker " + expelledBrokerId + " within " + timeoutMs + "ms");
    }

    private static void waitForIsrContaining(
            List<Broker> brokers, String topic, int partition, List<Integer> expectedIsr, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean everyoneAgrees = brokers.stream().allMatch(b -> {
                var state = b.topics().partitionState(topic, partition).orElse(null);
                return state != null && state.isr().containsAll(expectedIsr);
            });
            if (everyoneAgrees) return;
            Thread.sleep(100);
        }
        throw new AssertionError("ISR did not re-expand to contain " + expectedIsr + " within " + timeoutMs + "ms");
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
