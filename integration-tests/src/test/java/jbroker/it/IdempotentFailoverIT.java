package jbroker.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import jbroker.app.Broker;
import jbroker.app.VoterAddress;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Audit-finding #1 — idempotent producer dedup survives leader failover.
 *
 * <p>Setup: 3-broker cluster, 1-partition 3-replica topic. An idempotent
 * producer (producer_id from {@code InitProducerId}) writes 10 records with
 * sequences 0..9 to the partition leader. After all three replicas have the
 * records, the leader is killed abruptly. A new leader takes over. The same
 * producer retries sequences 0..9 against the new leader.
 *
 * <p>Expectation (broken as of audit): the new leader should de-duplicate
 * the retry — either by rejecting as already-committed or by returning the
 * cached offsets. End state must be exactly 10 records in the log, not 20.
 *
 * <p>Current behavior (the bug): the new leader's dedup state is a per-
 * broker in-memory map in {@code ProduceHandler}; failover hands over an
 * empty map, so the retry is accepted as fresh and the log ends up with 20
 * records.
 */
class IdempotentFailoverIT {

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void idempotentRetryAcrossFailoverDoesNotDuplicate(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
        int r1 = freePort(), r2 = freePort(), r3 = freePort();
        int b1 = freePort(), b2 = freePort(), b3 = freePort();
        var voters = List.of(
                new VoterAddress(new NodeId(1), "127.0.0.1", r1, b1),
                new VoterAddress(new NodeId(2), "127.0.0.1", r2, b2),
                new VoterAddress(new NodeId(3), "127.0.0.1", r3, b3));

        var br1 = Broker.start(new Broker.Config(new NodeId(1), d1, r1, b1, voters));
        var br2 = Broker.start(new Broker.Config(new NodeId(2), d2, r2, b2, voters));
        var br3 = Broker.start(new Broker.Config(new NodeId(3), d3, r3, b3, voters));
        var brokers = new ArrayList<>(List.of(br1, br2, br3));

        try {
            awaitSingleRaftLeader(brokers);
            // BrokerRegistry convergence is what enables inter-broker
            // replication — ReplicaFetcher dials addresses out of the
            // registry, and the registry populates ~1s after election via
            // the broker-registration ticker.
            awaitRegistryConvergence(brokers);

            // Topic with RF=3 — every broker is a replica so a failover
            // produces a survivor that already has the records.
            createTopicWithRetry(brokers, "orders", 1, 3);
            awaitPartitionLeaderOnAll(brokers, "orders", 0);
            // Wait for ISR to include all three brokers — the ReplicaFetcher
            // on each follower has to connect + fetch before they're added,
            // and that isn't synchronous with createTopic returning.
            awaitIsrSize(brokers, "orders", 0, /*size*/ 3);

            int leaderId = brokers.get(0).topics().partitionLeader("orders", 0).orElseThrow();
            var partitionLeader = brokerById(brokers, leaderId);

            // Allocate a producer id via the current Raft leader (server-side
            // dispatch through Raft is idempotent; which broker we dial
            // doesn't affect the id we get).
            long pid;
            try (var client = new BrokerClient("127.0.0.1", partitionLeader.brokerPort())) {
                pid = client.initProducerId();
            }
            assertThat(pid).as("InitProducerId must allocate a real pid (> 0)").isGreaterThan(0L);

            // Write sequences 0..9 via the partition leader with acks=all so
            // every ISR replica has the records on disk before we kill the
            // leader. Without acks=all, records could be lost entirely and
            // the test would prove something other than dedup-on-failover.
            try (var client = new BrokerClient("127.0.0.1", partitionLeader.brokerPort())) {
                for (int seq = 0; seq < 10; seq++) {
                    client.idempotentProduceAcksAll(
                            "orders", 0, ("msg-" + seq).getBytes(StandardCharsets.UTF_8), pid, /*epoch*/ 0, seq);
                }
            }

            // Paranoia check — every replica's LEO must be at least 10
            // before we kill the leader. acks=all already guaranteed this
            // but a local await costs us nothing and keeps the test
            // deterministic under CI jitter.
            awaitPartitionReplicated(brokers, "orders", 0, /*expectedLeo*/ 10L);

            // Abrupt kill on the partition leader — survivors must fence
            // + elect a new partition leader from the in-sync followers.
            partitionLeader.closeAbruptly();
            brokers.remove(partitionLeader);

            int newLeaderId = awaitNewPartitionLeader(brokers, "orders", 0, /*deadLeaderId*/ leaderId);
            var newLeader = brokerById(brokers, newLeaderId);
            // Raft-replicated view convergence: the new leader may not have
            // applied its own PartitionChangeRecord yet when brokerById
            // returns. Produce RPCs will get NOT_LEADER until it does.
            awaitBrokerSeesSelfAsLeader(newLeader, "orders", 0);

            // Retry every sequence 0..9 at the new leader. With correct
            // dedup-across-failover, the broker either returns the cached
            // response (when the retry exactly matches the window's latest
            // batch) or rejects as OUT_OF_ORDER_SEQUENCE (when the retry
            // is an older-than-latest sequence). Both outcomes are valid —
            // the invariant is "no duplicate records appended".
            try (var client = new BrokerClient("127.0.0.1", newLeader.brokerPort())) {
                for (int seq = 0; seq < 10; seq++) {
                    try {
                        client.idempotentProduce(
                                "orders", 0, ("msg-" + seq).getBytes(StandardCharsets.UTF_8), pid, /*epoch*/ 0, seq);
                    } catch (RuntimeException e) {
                        // OUT_OF_ORDER_SEQUENCE is an acceptable outcome for
                        // a retry of an already-committed sequence — it tells
                        // the client its window has advanced and it should
                        // reset. The log-size assertion below is what
                        // actually validates idempotency.
                        assertThat(e).hasMessageContaining("base_sequence");
                    }
                }
            }

            // Consume everything from offset 0 and assert the log is still
            // length 10. The failing build produces length 20.
            try (var client = new BrokerClient("127.0.0.1", newLeader.brokerPort())) {
                var all = client.fetchAll("orders", 0, 1 << 20);
                assertThat(all)
                        .as("idempotent producer retry after leader failover must NOT duplicate records "
                                + "(audit-finding #1: dedup state evaporates on failover)")
                        .hasSize(10);
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

    // ---------------- cluster plumbing (mirrors ConsumerGroupsE2EIT) ----------------

    private static Broker brokerById(List<Broker> brokers, int brokerId) throws Exception {
        for (var b : brokers) {
            if (brokerIdOf(b) == brokerId) return b;
        }
        throw new AssertionError("no broker with id=" + brokerId);
    }

    private static int brokerIdOf(Broker b) throws Exception {
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

    private static void awaitRegistryConvergence(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            boolean allKnowAll = brokers.stream()
                    .allMatch(b -> b.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3)));
            if (allKnowAll) return;
            Thread.sleep(50);
        }
        throw new AssertionError("broker registry did not converge within 10s");
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

    private static void awaitPartitionLeaderOnAll(List<Broker> brokers, String topic, int partition)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            boolean all = brokers.stream()
                    .allMatch(b -> b.topics().partitionState(topic, partition).isPresent()
                            && b.topics()
                                            .partitionState(topic, partition)
                                            .orElseThrow()
                                            .leader()
                                    > 0);
            if (all) return;
            Thread.sleep(50);
        }
        throw new AssertionError("partition " + topic + "-" + partition + " did not converge within 10s");
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

    private static void awaitIsrSize(List<Broker> brokers, String topic, int partition, int size) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            // Any broker's view is fine — partition metadata is Raft-replicated.
            for (var b : brokers) {
                var state = b.topics().partitionState(topic, partition).orElse(null);
                if (state != null && state.isr().size() >= size) return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("ISR for " + topic + "-" + partition + " did not reach size " + size + " in 20s");
    }

    private static void awaitPartitionReplicated(List<Broker> brokers, String topic, int partition, long expectedLeo)
            throws Exception {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            boolean all = true;
            for (var b : brokers) {
                long leo = b.logManager().logFor(topic, partition).nextOffset();
                if (leo < expectedLeo) {
                    all = false;
                    break;
                }
            }
            if (all) return;
            Thread.sleep(50);
        }
        throw new AssertionError(
                "partition " + topic + "-" + partition + " replicas did not catch up to LEO " + expectedLeo);
    }

    private static int awaitNewPartitionLeader(List<Broker> brokers, String topic, int partition, int deadLeaderId)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            for (var b : brokers) {
                var state = b.topics().partitionState(topic, partition).orElse(null);
                if (state != null && state.leader() != deadLeaderId && state.leader() > 0) {
                    return state.leader();
                }
            }
            Thread.sleep(100);
        }
        throw new AssertionError(
                "new leader for " + topic + "-" + partition + " did not emerge after broker " + deadLeaderId + " died");
    }
}
