package jbroker.app;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import jbroker.app.testkit.TestBrokers;
import jbroker.broker.client.BatchingProducer;
import jbroker.broker.client.BrokerClient;
import jbroker.broker.replication.ReplicaFetcher;
import jbroker.broker.replication.ReplicaPeerClient;
import jbroker.raft.core.NodeId;
import jbroker.storage.Compression;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end contract of zstd batch compression: a producer with the
 * codec on writes through a live broker and a plain fetch reads every
 * value back exactly; the batches are stored compressed on the leader's
 * disk; a follower replicates them byte-identically; and a compacted
 * topic full of compressed batches keeps the right survivors — still
 * compressed — at their original sparse offsets.
 */
class CompressionRoundTripIT {

    /** Repetitive tail makes every payload reliably compressible. */
    private static final String FILLER = "lorem-ipsum-dolor-sit-".repeat(12);

    @Test
    void zstdProducerRoundTripsAndStoresCompressed(@TempDir Path dir) throws Exception {
        var broker = TestBrokers.startSingleNode(dir);
        try (var client = new BrokerClient("127.0.0.1", broker.brokerPort())) {
            client.createTopic("stream", 1, 1);

            int n = 400;
            var expected = new ArrayList<String>(n);
            var config = new BatchingProducer.Config(4096, 50, 120_000, 100, Compression.ZSTD);
            try (var producer = BatchingProducer.create(client, config)) {
                var futures = new ArrayList<java.util.concurrent.CompletableFuture<Long>>(n);
                for (int i = 0; i < n; i++) {
                    String payload = "payload-" + i + "-" + FILLER;
                    expected.add(payload);
                    futures.add(producer.send("stream", 0, payload.getBytes(UTF_8)));
                }
                producer.flush();
                for (int i = 0; i < n; i++) {
                    assertThat(futures.get(i).get(10, TimeUnit.SECONDS)).isEqualTo((long) i);
                }
            }

            // Consumer leg: the fetch path decodes (and decompresses)
            // transparently — values come back exactly, in order.
            var fetched = client.fetchAll("stream", 0, 1 << 22);
            assertThat(fetched).hasSize(n);
            for (int i = 0; i < n; i++) {
                assertThat(new String(fetched.get(i), UTF_8)).isEqualTo(expected.get(i));
            }

            // Storage leg: every batch on the leader's disk carries the
            // zstd codec bits and is smaller than its uncompressed
            // encoding. estimatedSize is exact for the uncompressed
            // layout, so the ratio below is the true savings.
            var batches = broker.logManager().logFor("stream", 0).read(0, 1 << 22);
            assertThat(batches).isNotEmpty();
            long stored = 0;
            long uncompressed = 0;
            for (var b : batches) {
                assertThat(b.codec())
                        .as("batch at %d stored compressed", b.baseOffset())
                        .isEqualTo(Compression.ZSTD);
                stored += b.totalBytes();
                uncompressed += RecordBatch.estimatedSize(b.records());
            }
            assertThat(stored).isLessThan(uncompressed);
            System.out.printf(
                    "compression: stored=%d uncompressed=%d ratio=%.2fx%n",
                    stored, uncompressed, (double) uncompressed / stored);
        } finally {
            broker.close();
        }
    }

    @Test
    void followerReplicatesCompressedBatchesByteIdentically(@TempDir Path leaderDir, @TempDir Path followerDir)
            throws Exception {
        var node = TestBrokers.start((rp, bp) -> new Broker.Config(new NodeId(1), leaderDir, rp, bp));
        var leader = node.broker();
        int brokerPort = node.brokerPort();
        try (var client = new BrokerClient("127.0.0.1", brokerPort)) {
            client.createTopic("replicated", 1, 3);

            int n = 300;
            var config = new BatchingProducer.Config(4096, 50, 120_000, 100, Compression.ZSTD);
            try (var producer = BatchingProducer.create(client, config)) {
                for (int i = 0; i < n; i++) {
                    producer.send("replicated", 0, ("msg-" + i + "-" + FILLER).getBytes(UTF_8));
                }
                producer.flush();
            }

            try (var followerLm = new LogManager(
                            followerDir,
                            new LogManager.Config(
                                    128L * 1024 * 1024,
                                    Long.MAX_VALUE,
                                    LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                                    TimeUnit.MINUTES.toMillis(5)));
                    var peer = new ReplicaPeerClient("127.0.0.1", brokerPort)) {

                // Broker-id 1 so the single-broker ISR check accepts the
                // fetch (same trick as ReplicaFetchEndToEndIT).
                var fetcher = new ReplicaFetcher(
                        followerLm,
                        "replicated",
                        0, /* followerBrokerId */
                        1,
                        new ReplicaFetcher.Peer() {
                            @Override
                            public jbroker.proto.broker.ReplicaFetchResponse fetch(
                                    jbroker.proto.broker.ReplicaFetchRequest req) {
                                return peer.fetch(req, 5_000L);
                            }

                            @Override
                            public long offsetsForLeaderEpoch(String topic, int partition, int leaderEpoch) {
                                return peer.offsetsForLeaderEpoch(topic, partition, leaderEpoch, 5_000L);
                            }
                        });

                long deadline = System.currentTimeMillis() + 30_000;
                while (System.currentTimeMillis() < deadline
                        && followerLm.logFor("replicated", 0).nextOffset() < n) {
                    fetcher.pollOnce(/* expectedEpoch */ 0);
                    Thread.sleep(10);
                }
                assertThat(followerLm.logFor("replicated", 0).nextOffset()).isEqualTo((long) n);

                // The replica path appends raw slices — the follower's
                // segment must be a byte-for-byte copy of the leader's,
                // compressed sections included.
                byte[] leaderBytes = Files.readAllBytes(segmentFile(leaderDir, "replicated-0"));
                byte[] followerBytes = Files.readAllBytes(segmentFile(followerDir, "replicated-0"));
                assertThat(followerBytes).isEqualTo(leaderBytes);

                // And the follower's copy decodes to the produced values.
                var batches = followerLm.logFor("replicated", 0).read(0, 1 << 22);
                var values = new ArrayList<String>();
                for (var b : batches) {
                    assertThat(b.codec()).isEqualTo(Compression.ZSTD);
                    for (var r : b.records()) {
                        values.add(new String(r.value(), UTF_8));
                    }
                }
                assertThat(values).hasSize(n);
                assertThat(values.get(0)).isEqualTo("msg-0-" + FILLER);
                assertThat(values.get(n - 1)).isEqualTo("msg-" + (n - 1) + "-" + FILLER);
            }
        } finally {
            leader.close();
        }
    }

    @Test
    void compactedCompressedTopicKeepsRightSurvivors(@TempDir Path dir) throws Exception {
        try (var broker = TestBrokers.startSingleVoter(dir)) {
            awaitLeadership(broker);
            try (var client = new BrokerClient("127.0.0.1", broker.brokerPort())) {
                client.createTopicWithConfig("prices", 1, 1, Map.of("cleanup.policy", "compact"));

                // Seed keyed compressed batches at the Log layer — the
                // producer gRPC path doesn't expose keys (same seeding as
                // ForceCompactIT). Offsets 0..99, 5 keys round-robin, so
                // the last write per key lands at 95..99.
                var log = broker.logManager().logFor("prices", 0);
                for (int i = 0; i < 100; i++) {
                    String key = "k" + (i % 5);
                    String value = "v" + i + "-" + FILLER;
                    log.append(
                            List.of(new Record(0, 0L, key.getBytes(UTF_8), value.getBytes(UTF_8))),
                            System.currentTimeMillis(),
                            -1L,
                            (short) -1,
                            -1,
                            0,
                            Compression.ZSTD);
                }

                int kept = client.forceCompactPartition("prices", 0);
                assertThat(kept).isEqualTo(5);

                var fetched = client.fetchRecords("prices", 0, 0, 1 << 22);
                assertThat(fetched).hasSize(5);
                var offsets = new ArrayList<Long>();
                var valuesByKey = new java.util.HashMap<String, String>();
                for (var rec : fetched) {
                    offsets.add(rec.offset());
                    valuesByKey.put(new String(rec.key(), UTF_8), new String(rec.value(), UTF_8));
                }
                assertThat(offsets).containsExactlyInAnyOrder(95L, 96L, 97L, 98L, 99L);
                for (int k = 0; k < 5; k++) {
                    assertThat(valuesByKey).containsEntry("k" + k, "v" + (95 + k) + "-" + FILLER);
                }

                // The rewritten survivor batch stays compressed.
                var batches = log.read(0, 1 << 22);
                assertThat(batches).hasSize(1);
                assertThat(batches.get(0).codec()).isEqualTo(Compression.ZSTD);
            }
        }
    }

    /** The single .log segment file of {@code topicPartition} under {@code root}. */
    private static Path segmentFile(Path root, String topicPartition) throws Exception {
        try (var walk = Files.walk(root)) {
            return walk.filter(p -> p.getParent() != null
                            && p.getParent().getFileName().toString().equals(topicPartition)
                            && p.getFileName().toString().endsWith(".log"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("no .log segment for " + topicPartition + " under " + root));
        }
    }

    private static void awaitLeadership(Broker broker) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (broker.role() != jbroker.raft.core.Role.LEADER && System.currentTimeMillis() < deadline) {
            Thread.sleep(25);
        }
        assertThat(broker.role()).isEqualTo(jbroker.raft.core.Role.LEADER);
    }
}
