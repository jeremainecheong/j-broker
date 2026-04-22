package jbroker.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import jbroker.app.Broker;
import jbroker.app.VoterAddress;
import jbroker.broker.ConsumerOffsetsTopic;
import jbroker.broker.client.BrokerClient;
import jbroker.broker.client.consumer.Consumer;
import jbroker.broker.client.consumer.ConsumerConfig;
import jbroker.broker.client.consumer.RebalanceListener;
import jbroker.broker.client.consumer.StringDeserializer;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * P7.12 — consumer-group churn under sustained produce load.
 *
 * <p>Spawns a 3-broker cluster, 3 consumers in a single group churning
 * join/leave on a rotating schedule, and a steady producer. Asserts:
 * <ol>
 *   <li>Brokers stay healthy across the churn window.</li>
 *   <li>Every produced record gets consumed by at least one member
 *       (at-least-once delivery — duplicates during rebalance are
 *       expected and not asserted against).</li>
 * </ol>
 *
 * <p>Default duration is 20 seconds for a CI-friendly run; override via
 * the {@code JBROKER_CHURN_DURATION_SECONDS} environment variable (the
 * spec mandates a 10-minute local sign-off — invoke
 * {@code scripts/chaos/group-churn.sh} which sets the env to 600).
 * Excluded from the default {@code test} task via the {@code chaos}
 * JUnit tag; run with {@code :integration-tests:chaosTest}.
 */
@Tag("chaos")
class GroupChurnIT {

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static int durationSeconds() {
        String env = System.getenv("JBROKER_CHURN_DURATION_SECONDS");
        if (env == null || env.isBlank()) return 20;
        try {
            return Integer.parseInt(env);
        } catch (NumberFormatException e) {
            throw new IllegalStateException("invalid JBROKER_CHURN_DURATION_SECONDS: " + env, e);
        }
    }

    @Test
    void clusterSurvivesJoinLeaveChurnUnderSustainedProduceLoad(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
        int durationSec = durationSeconds();

        int r1 = freePort(), r2 = freePort(), r3 = freePort();
        int b1 = freePort(), b2 = freePort(), b3 = freePort();
        var voters = List.of(
                new VoterAddress(new NodeId(1), "127.0.0.1", r1, b1),
                new VoterAddress(new NodeId(2), "127.0.0.1", r2, b2),
                new VoterAddress(new NodeId(3), "127.0.0.1", r3, b3));

        var br1 = Broker.start(new Broker.Config(new NodeId(1), d1, r1, b1, voters));
        var br2 = Broker.start(new Broker.Config(new NodeId(2), d2, r2, b2, voters));
        var br3 = Broker.start(new Broker.Config(new NodeId(3), d3, r3, b3, voters));
        var brokers = List.of(br1, br2, br3);
        var pool = Executors.newFixedThreadPool(5);
        try {
            awaitSingleRaftLeader(brokers);
            awaitConsumerOffsetsLeader(brokers);
            var raftLeader = brokers.stream()
                    .filter(b -> b.role() == Role.LEADER)
                    .findFirst()
                    .orElseThrow();
            try (var admin = new BrokerClient("127.0.0.1", raftLeader.brokerPort())) {
                admin.createTopic("churn", 6, 3);
            }

            var produced = ConcurrentHashMap.<String>newKeySet();
            var consumed = ConcurrentHashMap.<String>newKeySet();
            var stop = new AtomicBoolean();
            var producerFailures = new AtomicInteger();
            var consumerFailures = new AtomicInteger();

            // Steady producer: ~50 r/s spread across 6 partitions.
            pool.submit(runSafely("producer", producerFailures, () -> {
                try (var p = new BrokerClient("127.0.0.1", raftLeader.brokerPort())) {
                    int i = 0;
                    while (!stop.get()) {
                        String msg = "m-" + (i++);
                        try {
                            p.produce("churn", i % 6, msg.getBytes(StandardCharsets.UTF_8));
                            produced.add(msg);
                        } catch (RuntimeException e) {
                            producerFailures.incrementAndGet();
                        }
                        Thread.sleep(20);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));

            // Three consumers cycling in a staggered pattern: each takes a
            // turn being offline for ~3s out of every ~12s, creating rolling
            // rebalances.
            int consumerCount = 3;
            for (int idx = 0; idx < consumerCount; idx++) {
                final int myIdx = idx;
                pool.submit(runSafely("consumer-" + idx, consumerFailures, () -> {
                    long startNanos = System.nanoTime();
                    long cycleNanos = TimeUnit.SECONDS.toNanos(12);
                    while (!stop.get()) {
                        long elapsed = System.nanoTime() - startNanos;
                        long phase = (elapsed / TimeUnit.SECONDS.toNanos(1)) % (12);
                        // This consumer is "offline" for its 3-second slot.
                        if (phase >= myIdx * 3 && phase < (myIdx + 1) * 3) {
                            sleepMs(200);
                            continue;
                        }
                        // Run a consumer for one cycle's worth before restarting.
                        long sessionDeadline = System.nanoTime() + cycleNanos - (elapsed % cycleNanos);
                        try (var c = newConsumer("g-churn", raftLeader.brokerPort(), "c" + myIdx)) {
                            c.subscribe(List.of("churn"), RebalanceListener.NO_OP);
                            while (!stop.get() && System.nanoTime() < sessionDeadline) {
                                var records = c.poll(Duration.ofMillis(200));
                                for (var rec : records) {
                                    consumed.add(rec.value());
                                }
                                if (records.isEmpty()) sleepMs(50);
                            }
                            c.commitSync();
                        } catch (RuntimeException e) {
                            consumerFailures.incrementAndGet();
                        }
                    }
                }));
            }

            // Let the churn run.
            Thread.sleep(TimeUnit.SECONDS.toMillis(durationSec));
            stop.set(true);
            pool.shutdown();
            pool.awaitTermination(30, TimeUnit.SECONDS);

            // All brokers alive.
            for (var b : brokers) {
                assertThat(b.role()).isIn(Role.LEADER, Role.FOLLOWER, Role.CANDIDATE);
            }

            // Drain any stragglers via a final clean consumer (no churn).
            try (var drain = newConsumer("g-churn", raftLeader.brokerPort(), "drain")) {
                drain.subscribe(List.of("churn"), RebalanceListener.NO_OP);
                long drainDeadline = System.currentTimeMillis() + 10_000;
                int quietPolls = 0;
                while (System.currentTimeMillis() < drainDeadline && quietPolls < 5) {
                    var records = drain.poll(Duration.ofMillis(300));
                    if (records.isEmpty()) {
                        quietPolls++;
                        sleepMs(100);
                    } else {
                        quietPolls = 0;
                        for (var rec : records) consumed.add(rec.value());
                    }
                }
            }

            Set<String> missing = new HashSet<>(produced);
            missing.removeAll(consumed);
            assertThat(producerFailures.get())
                    .as("producer should not fail under chaos load")
                    .isZero();
            // At-least-once: every produced record must have been seen by
            // at least one consumer (including the drain pass).
            assertThat(missing)
                    .as("every produced record must be consumed by some member (at-least-once)")
                    .isEmpty();
            assertThat(produced.size())
                    .as("produce path should have generated meaningful traffic")
                    .isGreaterThan(100);
        } finally {
            pool.shutdownNow();
            for (var b : brokers) {
                try {
                    b.close();
                } catch (Exception ignored) {
                    // best-effort
                }
            }
        }
    }

    private static Consumer<String, String> newConsumer(String groupId, int bootstrapPort, String instanceId) {
        var cfg = ConsumerConfig.builder(groupId, "127.0.0.1", bootstrapPort)
                .instanceId(instanceId)
                .build();
        return new Consumer<>(cfg, new StringDeserializer(), new StringDeserializer());
    }

    private static Runnable runSafely(String label, AtomicInteger failureCounter, Runnable body) {
        return () -> {
            try {
                body.run();
            } catch (Throwable t) {
                failureCounter.incrementAndGet();
                System.err.println("[chaos-" + label + "] died: " + t);
            }
        };
    }

    private static void sleepMs(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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

    private static void awaitConsumerOffsetsLeader(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            boolean all = brokers.stream().allMatch(b -> b.topics()
                    .partitionState(ConsumerOffsetsTopic.NAME, 0)
                    .map(s -> s.leader() > 0)
                    .orElse(false));
            if (all) return;
            Thread.sleep(50);
        }
        throw new AssertionError("__consumer_offsets-0 did not converge within 15s");
    }
}
