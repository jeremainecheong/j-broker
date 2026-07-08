package jbroker.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import jbroker.app.Broker;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.broker.ConsumerOffsetsTopic;
import jbroker.broker.client.BrokerClient;
import jbroker.broker.client.consumer.Consumer;
import jbroker.broker.client.consumer.ConsumerConfig;
import jbroker.broker.client.consumer.DeadLetterPolicy;
import jbroker.broker.client.consumer.RebalanceListener;
import jbroker.broker.client.consumer.RecordHandler;
import jbroker.broker.client.consumer.RetryableException;
import jbroker.broker.client.consumer.StringDeserializer;
import jbroker.proto.common.TopicPartition;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Consumer-group end-to-end regression net across a 3-broker cluster.
 *
 * <p>Two tests:
 * <ol>
 *   <li>{@link #coordBrokerDeathPreservesGroupCommittedOffsets} —
 *       the genuinely-multi-broker scenario. Kill the broker hosting the
 *       {@code __consumer_offsets} partition leader; assert a new
 *       consumer in the same group can still read back the previously
 *       committed offsets (group state recovered on new coordinator).
 *       This closes the coordinator-failover mechanic that earlier
 *       testing could only exercise on single-broker restart.</li>
 *   <li>{@link #endToEndRegressionOn3BrokerCluster} — a single-cluster
 *       pass that exercises assignment, commit/fetch, DLT routing, and
 *       incremental fetch sessions in one run as a cross-cutting
 *       regression net. Individual dedicated ITs cover these more
 *       thoroughly on single-broker fixtures.</li>
 * </ol>
 */
class ConsumerGroupsE2EIT {

    @Test
    void coordBrokerDeathPreservesGroupCommittedOffsets(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        var cluster = TestBrokerCluster.start(
                3,
                2,
                (i, voters, ports) -> new Broker.Config(new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters));
        var br1 = cluster.broker(0);
        var br2 = cluster.broker(1);
        var br3 = cluster.broker(2);
        int b1 = cluster.brokerPort(0), b2 = cluster.brokerPort(1), b3 = cluster.brokerPort(2);
        var brokers = new ArrayList<>(List.of(br1, br2, br3));
        try {
            awaitSingleRaftLeader(brokers);
            awaitConsumerOffsetsLeaderOnAll(brokers);

            // Pick the broker hosting the __consumer_offsets partition 0 leader
            // — that's the coordinator for *every* group (1-partition fixture).
            int coordBrokerId = brokers.get(0)
                    .topics()
                    .partitionState(ConsumerOffsetsTopic.NAME, 0)
                    .orElseThrow()
                    .leader();
            var coordBroker = brokerById(brokers, coordBrokerId);
            var survivor = brokers.stream()
                    .filter(b -> b.brokerPort() != coordBroker.brokerPort())
                    .findFirst()
                    .orElseThrow();

            // Produce some records via the Raft leader, then commit an
            // offset on a consumer in group "g".
            createTopicWithRetry(brokers, "orders", 1, 3);
            awaitPartitionLeaderOnAll(brokers, "orders", 0);
            var partitionLeader = brokerById(
                    brokers,
                    brokers.get(0).topics().partitionLeader("orders", 0).orElseThrow());
            try (var producer = new BrokerClient("127.0.0.1", partitionLeader.brokerPort())) {
                for (int i = 0; i < 10; i++) {
                    producer.produce("orders", 0, ("msg-" + i).getBytes(StandardCharsets.UTF_8));
                }
            }

            // Connect the first consumer via a *surviving* broker so the
            // bootstrap channel still works after we kill the coord.
            var cfg = ConsumerConfig.builder("g-failover", "127.0.0.1", survivor.brokerPort())
                    .build();
            try (var c1 = new Consumer<>(cfg, new StringDeserializer(), new StringDeserializer())) {
                c1.subscribe(List.of("orders"), RebalanceListener.NO_OP);
                long deadline = System.currentTimeMillis() + 10_000;
                int total = 0;
                while (total < 10 && System.currentTimeMillis() < deadline) {
                    total += c1.poll(Duration.ofMillis(300)).count();
                }
                assertThat(total).isEqualTo(10);
                var committed = c1.commitSync();
                var tp = TopicPartition.newBuilder()
                        .setTopic("orders")
                        .setPartition(0)
                        .build();
                assertThat(committed.get(tp).offset()).isEqualTo(10L);
            }

            // The commit write to __consumer_offsets is a local appendRaw on
            // the coord's log — followers catch up asynchronously via
            // ReplicaFetcher. Wait for every survivor's replica of
            // __consumer_offsets-0 to observe the commit's byte on disk
            // before we kill the coord, otherwise the commit simply dies
            // with it and the test proves nothing.
            awaitConsumerOffsetsReplicated(brokers, coordBroker);

            // Abrupt kill on the coord broker. The survivors must:
            //  (a) fence the dead broker (BrokerFencer ticker),
            //  (b) elect a new __consumer_offsets partition leader,
            //  (c) run GroupMetadataRecovery on the new leader to rebuild
            //      the coordinator's in-memory state from __consumer_offsets
            //      log records.
            coordBroker.closeAbruptly();
            brokers.remove(coordBroker);
            awaitNewConsumerOffsetsLeader(brokers, coordBrokerId);

            // Wait for the new partition leader to have actually applied
            // its PartitionChangeRecord locally (Raft-leader view is
            // authoritative, local views lag briefly under replication).
            int newLeaderId = brokers.stream()
                    .filter(b -> b.role() == Role.LEADER)
                    .findFirst()
                    .orElseThrow()
                    .topics()
                    .partitionState(ConsumerOffsetsTopic.NAME, 0)
                    .orElseThrow()
                    .leader();
            awaitBrokerSeesSelfAsLeader(brokerById(brokers, newLeaderId), ConsumerOffsetsTopic.NAME, 0);

            // A brand-new consumer in the same group — via the surviving
            // bootstrap — must read back the previously committed offset.
            // This proves group commit state survived coord failover:
            // the new coordinator's registration-ticker recovery walk
            // replayed __consumer_offsets into its local OffsetCache.
            try (var c2 = new Consumer<>(cfg, new StringDeserializer(), new StringDeserializer())) {
                c2.subscribe(List.of("orders"), RebalanceListener.NO_OP);
                var tp = TopicPartition.newBuilder()
                        .setTopic("orders")
                        .setPartition(0)
                        .build();
                long deadline = System.currentTimeMillis() + 15_000;
                long seen = -1L;
                while (seen != 10L && System.currentTimeMillis() < deadline) {
                    try {
                        seen = c2.committed(tp).offset();
                    } catch (RuntimeException ignored) {
                        // Coord may still be rebuilding; retry.
                    }
                    if (seen != 10L) Thread.sleep(200);
                }
                assertThat(seen)
                        .as("group commit state must survive coord broker death")
                        .isEqualTo(10L);
            }
        } finally {
            for (var b : brokers) {
                try {
                    b.close();
                } catch (Exception ignored) {
                    // best-effort
                }
            }
        }
    }

    @Test
    void endToEndRegressionOn3BrokerCluster(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3) throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        var cluster = TestBrokerCluster.start(
                3,
                2,
                (i, voters, ports) -> new Broker.Config(new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters));
        var br1 = cluster.broker(0);
        var br2 = cluster.broker(1);
        var br3 = cluster.broker(2);
        int b1 = cluster.brokerPort(0), b2 = cluster.brokerPort(1), b3 = cluster.brokerPort(2);
        var brokers = new ArrayList<>(List.of(br1, br2, br3));
        try {
            awaitSingleRaftLeader(brokers);
            awaitConsumerOffsetsLeaderOnAll(brokers);
            createTopicWithRetry(brokers, "orders", 3, 3);
            createTopicWithRetry(brokers, "orders.dlt", 3, 3);
            awaitPartitionLeaderOnAll(brokers, "orders", 0);

            // Produce records round-robin across partitions via the Raft
            // leader (acks=1 would route by partition leader; we simplify
            // by spreading across partitions explicitly).
            var raftLeader = brokers.stream()
                    .filter(b -> b.role() == Role.LEADER)
                    .findFirst()
                    .orElseThrow();
            try (var producer = new BrokerClient("127.0.0.1", raftLeader.brokerPort())) {
                for (int i = 0; i < 30; i++) {
                    producer.produce("orders", i % 3, ("msg-" + i).getBytes(StandardCharsets.UTF_8));
                }
                // One "bad" record routed to a known partition for the DLT
                // assertion — offset 10 on partition 0.
                producer.produce("orders", 0, "bad-msg".getBytes(StandardCharsets.UTF_8));
            }

            long baselineFetchHits = br1.metrics().incrementalFetchHits()
                    + br2.metrics().incrementalFetchHits()
                    + br3.metrics().incrementalFetchHits();

            // Single consumer (claims every partition) + incremental
            // fetch sessions + commit round-trip, all in one pass.
            var cfg = ConsumerConfig.builder("g-regression", "127.0.0.1", raftLeader.brokerPort())
                    .deadLetterPolicy(new DeadLetterPolicy("orders.dlt", 2, Duration.ofMillis(10)))
                    .build();
            try (var consumer = new Consumer<>(cfg, new StringDeserializer(), new StringDeserializer())) {
                consumer.subscribe(List.of("orders"), RebalanceListener.NO_OP);

                // Drive via a handler that DLTs the "bad-msg" and processes
                // everything else.
                var processed = new AtomicInteger();
                RecordHandler<String, String> handler = rec -> {
                    if ("bad-msg".equals(rec.value())) {
                        throw new RetryableException("always-bad");
                    }
                    processed.incrementAndGet();
                };

                long deadline = System.currentTimeMillis() + 15_000;
                while (processed.get() < 30 && System.currentTimeMillis() < deadline) {
                    consumer.poll(Duration.ofMillis(300), handler);
                }
                assertThat(processed.get())
                        .as("all non-DLT records should be handled")
                        .isEqualTo(30);
                // Single consumer → owns all 3 partitions.
                assertThat(consumer.assignment()).hasSize(3);
            }

            long afterFetchHits = br1.metrics().incrementalFetchHits()
                    + br2.metrics().incrementalFetchHits()
                    + br3.metrics().incrementalFetchHits();
            assertThat(afterFetchHits - baselineFetchHits)
                    .as("consumer's follow-up Fetches should register as incremental hits")
                    .isPositive();

            // Verify the bad record landed on orders.dlt.
            boolean dltHasBadMsg = false;
            for (int p = 0; p < 3 && !dltHasBadMsg; p++) {
                try (var verifier = new BrokerClient("127.0.0.1", raftLeader.brokerPort())) {
                    for (byte[] v : verifier.fetchAll("orders.dlt", p, 1 << 20)) {
                        if ("bad-msg".equals(new String(v, StandardCharsets.UTF_8))) {
                            dltHasBadMsg = true;
                            break;
                        }
                    }
                }
            }
            assertThat(dltHasBadMsg).as("DLT topic must contain the bad record").isTrue();
        } finally {
            for (var b : brokers) {
                try {
                    b.close();
                } catch (Exception ignored) {
                    // best-effort
                }
            }
        }
    }

    // ---------------- cluster plumbing ----------------

    private static Broker brokerById(List<Broker> brokers, int brokerId) throws Exception {
        for (var b : brokers) {
            if (brokerIdOf(b) == brokerId) return b;
        }
        throw new AssertionError("no broker with id=" + brokerId);
    }

    private static int brokerIdOf(Broker b) throws Exception {
        // Mirror MultiBrokerFailoverIT's reflection trick — adding a
        // Broker.selfId() accessor for one-off tests isn't worth the
        // production-API surface.
        var f = Broker.class.getDeclaredField("raftDriver");
        f.setAccessible(true);
        var rd = f.get(b);
        var sidField = rd.getClass().getDeclaredField("selfId");
        sidField.setAccessible(true);
        return ((NodeId) sidField.get(rd)).value();
    }

    private static void createTopicWithRetry(List<Broker> brokers, String topic, int partitions, int rf)
            throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        Exception last = null;
        while (System.currentTimeMillis() < deadline) {
            var raftLeader = brokers.stream()
                    .filter(b -> b.role() == Role.LEADER)
                    .findFirst()
                    .orElse(null);
            if (raftLeader == null) {
                Thread.sleep(100);
                continue;
            }
            try (var client = new BrokerClient("127.0.0.1", raftLeader.brokerPort())) {
                client.createTopic(topic, partitions, rf);
                return;
            } catch (Exception e) {
                last = e;
                Thread.sleep(100);
            }
        }
        throw new AssertionError("createTopic did not succeed within 10s", last);
    }

    private static void awaitSingleRaftLeader(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            long leaders = brokers.stream().filter(b -> b.role() == Role.LEADER).count();
            if (leaders == 1) return;
            Thread.sleep(50);
        }
        throw new AssertionError("no single Raft leader within 10s");
    }

    private static void awaitConsumerOffsetsLeaderOnAll(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            boolean allSee = brokers.stream().allMatch(b -> b.topics()
                    .partitionState(ConsumerOffsetsTopic.NAME, 0)
                    .map(s -> s.leader() > 0)
                    .orElse(false));
            if (allSee) return;
            Thread.sleep(50);
        }
        throw new AssertionError("__consumer_offsets partition 0 did not converge within 15s");
    }

    private static void awaitPartitionLeaderOnAll(List<Broker> brokers, String topic, int partition)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            boolean all = brokers.stream()
                    .allMatch(b -> b.topics().partitionState(topic, partition).isPresent());
            if (all) return;
            Thread.sleep(50);
        }
        throw new AssertionError("partition " + topic + "-" + partition + " did not converge within 5s");
    }

    private static void awaitBrokerSeesSelfAsLeader(Broker broker, String topic, int partition) throws Exception {
        int selfId = brokerIdOf(broker);
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            var state = broker.topics().partitionState(topic, partition).orElse(null);
            if (state != null && state.leader() == selfId) return;
            Thread.sleep(50);
        }
        throw new AssertionError(
                "broker " + selfId + " did not see itself as leader of " + topic + "-" + partition + " within 10s");
    }

    private static void awaitConsumerOffsetsReplicated(List<Broker> brokers, Broker source) throws Exception {
        long sourceLeo =
                source.logManager().logFor(ConsumerOffsetsTopic.NAME, 0).nextOffset();
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            boolean allCaughtUp = true;
            for (var b : brokers) {
                long leo = b.logManager().logFor(ConsumerOffsetsTopic.NAME, 0).nextOffset();
                if (leo < sourceLeo) {
                    allCaughtUp = false;
                    break;
                }
            }
            if (allCaughtUp) return;
            Thread.sleep(50);
        }
        throw new AssertionError("__consumer_offsets-0 replicas did not catch up to LEO " + sourceLeo + " within 10s");
    }

    private static void awaitNewConsumerOffsetsLeader(List<Broker> brokers, int deadBrokerId)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            var survivorRaftLeader =
                    brokers.stream().filter(b -> b.role() == Role.LEADER).findFirst();
            if (survivorRaftLeader.isPresent()) {
                var state = survivorRaftLeader
                        .get()
                        .topics()
                        .partitionState(ConsumerOffsetsTopic.NAME, 0)
                        .orElse(null);
                if (state != null && state.leader() != deadBrokerId && state.leader() > 0) {
                    return;
                }
            }
            Thread.sleep(100);
        }
        throw new AssertionError("new __consumer_offsets leader did not emerge within 15s");
    }
}
