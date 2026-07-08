package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Headline zero-loss acceptance gate (in-process shrinkage of
 * {@code scenario-chaos-with-load.sh}): sustained acks=all produce load
 * against a 3-broker cluster while we periodically {@link
 * Broker#closeAbruptly()} random brokers and restart them, then assert
 * every acked record is consumable from some replica — zero loss.
 *
 * <p>Runtime is bounded at 30 s so this fits CI. The formal 10-minute
 * soak is a local-only exercise and not part of CI gating.
 */
class BrokerChaosSoakIT {

    private static final long SOAK_MILLIS = 30_000L;
    private static final long RESTART_INTERVAL_MILLIS = 6_000L;

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void sustainedAcksAllProduceUnderBrokerChurnLosesNoRecords(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
        int r1 = freePort(), r2 = freePort(), r3 = freePort();
        int b1 = freePort(), b2 = freePort(), b3 = freePort();
        var voters = List.of(
                new VoterAddress(new NodeId(1), "127.0.0.1", r1, b1),
                new VoterAddress(new NodeId(2), "127.0.0.1", r2, b2),
                new VoterAddress(new NodeId(3), "127.0.0.1", r3, b3));

        // Map broker id to its config so we can restart a closed broker
        // with the same voter set + data dir.
        var configs = new ConcurrentHashMap<Integer, Broker.Config>();
        configs.put(1, new Broker.Config(new NodeId(1), d1, r1, b1, voters));
        configs.put(2, new Broker.Config(new NodeId(2), d2, r2, b2, voters));
        configs.put(3, new Broker.Config(new NodeId(3), d3, r3, b3, voters));

        var running = new ConcurrentHashMap<Integer, Broker>();
        running.put(1, Broker.start(configs.get(1)));
        running.put(2, Broker.start(configs.get(2)));
        running.put(3, Broker.start(configs.get(3)));

        try {
            awaitSingleLeader(running.values());
            awaitRegistryConvergence(running.values());

            // Create the topic via the Raft leader.
            var raftLeader = leaderOf(running.values());
            try (var client = new BrokerClient("127.0.0.1", raftLeader.brokerPort())) {
                client.createTopic("chaos", 1, 3);
            }
            awaitPartitionStateOnAll(running.values(), "chaos", 0);

            var ackedPayloads = ConcurrentHashMap.<String>newKeySet();
            var stop = new AtomicBoolean(false);
            var produced = new AtomicInteger();
            var failedProduces = new AtomicInteger();

            var producerThread = new Thread(
                    () -> {
                        int i = 0;
                        while (!stop.get()) {
                            // Find the current partition leader by polling any
                            // running broker's TopicManager (all converge).
                            var leaderBroker = findPartitionLeader(running);
                            if (leaderBroker == null) {
                                try {
                                    Thread.sleep(50);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                                continue;
                            }
                            String value = "r-" + i++;
                            try (var client = new BrokerClient("127.0.0.1", leaderBroker.brokerPort())) {
                                client.produceAcksAll("chaos", 0, value.getBytes(StandardCharsets.UTF_8));
                                ackedPayloads.add(value);
                                produced.incrementAndGet();
                            } catch (Exception e) {
                                // Expected during failover or leader-churn; retry
                                // will be attempted on the next loop iteration.
                                failedProduces.incrementAndGet();
                            }
                        }
                    },
                    "chaos-producer");
            producerThread.start();

            // Chaos loop: every RESTART_INTERVAL_MILLIS, pick a running
            // non-critical broker and abruptly close + restart it.
            var rng = new Random(42);
            long chaosDeadline = System.currentTimeMillis() + SOAK_MILLIS;
            while (System.currentTimeMillis() < chaosDeadline) {
                Thread.sleep(RESTART_INTERVAL_MILLIS);
                if (System.currentTimeMillis() >= chaosDeadline) break;
                // Pick a running broker — deliberately includes whoever
                // happens to be partition leader, so we exercise real
                // failover churn.
                var runningIds = new ArrayList<>(running.keySet());
                int victim = runningIds.get(rng.nextInt(runningIds.size()));
                var br = running.remove(victim);
                br.closeAbruptly();
                // Wait a beat for failover to converge before restarting.
                Thread.sleep(2_000);
                running.put(victim, Broker.start(configs.get(victim)));
            }

            stop.set(true);
            producerThread.join(10_000);

            // Before consuming, make sure every broker's log is consistent
            // by letting replication catch up a bit.
            Thread.sleep(2_000);
            awaitPartitionStateOnAll(running.values(), "chaos", 0);
            var finalLeader = findPartitionLeader(running);
            assertThat(finalLeader)
                    .as("expected a partition leader at end of soak")
                    .isNotNull();

            // Consume all records from the final partition leader.
            Set<String> consumed = new HashSet<>();
            try (var client = new BrokerClient("127.0.0.1", finalLeader.brokerPort())) {
                long offset = 0;
                while (true) {
                    var batch = client.fetch("chaos", 0, offset, 1 << 20);
                    if (batch.isEmpty()) break;
                    for (var b : batch) {
                        consumed.add(new String(b, StandardCharsets.UTF_8));
                    }
                    offset += batch.size();
                }
            }

            // Zero-loss assertion: every payload we saw acked is in the
            // consumed set. Duplicates / extra records are allowed (the
            // chaos soak's producer doesn't use idempotent-producer ids,
            // so retries produced on failover may land as dupes — the
            // idempotent-producer dedup is a separate path).
            var lost = new ArrayList<String>();
            for (var v : ackedPayloads) {
                if (!consumed.contains(v)) lost.add(v);
            }
            System.out.printf(
                    "CHAOS SOAK: produced=%d acked=%d failed=%d consumed=%d lost=%d%n",
                    produced.get(), ackedPayloads.size(), failedProduces.get(), consumed.size(), lost.size());
            assertThat(lost)
                    .as("acks=all records must never be lost under broker churn")
                    .isEmpty();
        } finally {
            for (var br : running.values()) {
                try {
                    br.close();
                } catch (Exception ignored) {
                    // best-effort
                }
            }
        }
    }

    private static Broker findPartitionLeader(ConcurrentHashMap<Integer, Broker> running) {
        for (var br : running.values()) {
            try {
                int leader = br.topics()
                        .partitionState("chaos", 0)
                        .map(s -> s.leader())
                        .orElse(-1);
                if (leader > 0 && running.containsKey(leader)) return running.get(leader);
            } catch (Exception ignored) {
                // broker likely shutting down; try next.
            }
        }
        return null;
    }

    private static void awaitSingleLeader(Iterable<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            long leaders = 0;
            for (var b : brokers) if (b.role() == Role.LEADER) leaders++;
            if (leaders == 1) return;
            Thread.sleep(50);
        }
        throw new AssertionError("no single leader within 10s");
    }

    private static void awaitRegistryConvergence(Iterable<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            boolean allKnow = true;
            for (var b : brokers) {
                if (!b.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3))) {
                    allKnow = false;
                    break;
                }
            }
            if (allKnow) return;
            Thread.sleep(50);
        }
        throw new AssertionError("broker registry did not converge within 5s");
    }

    private static void awaitPartitionStateOnAll(Iterable<Broker> brokers, String topic, int partition)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            boolean all = true;
            for (var b : brokers) {
                if (b.topics().partitionState(topic, partition).isEmpty()) {
                    all = false;
                    break;
                }
            }
            if (all) return;
            Thread.sleep(50);
        }
        throw new AssertionError("partition state did not converge");
    }

    private static Broker leaderOf(Iterable<Broker> brokers) {
        for (var b : brokers) if (b.role() == Role.LEADER) return b;
        throw new AssertionError("no broker is Raft leader");
    }
}
