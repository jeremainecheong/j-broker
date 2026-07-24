package jbroker.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import jbroker.app.Broker;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Acceptance: a rolling restart of every broker under continuous
 * acks=all load produces zero non-retriable client errors and loses
 * nothing that was acked. Each broker shuts down via
 * {@link Broker#closeGracefully} — leadership hands off to another ISR
 * member before the process exits, so clients see retriable NOT_LEADER /
 * transport errors during the roll, never data-shaped failures.
 *
 * <p>Error classification mirrors what a real producer would treat as
 * fatal: invalid partition, size violations, corruption, sequence
 * violations. Everything else (leadership moves, connection drops to the
 * restarting broker, replication-floor waits, metadata lag on a freshly
 * restarted broker) is retriable by contract.
 */
class GracefulRollingRestartIT {

    private static final int PARTITIONS = 3;
    private static final long DRAIN_TIMEOUT_MS = 10_000L;
    private static final long CONVERGE_TIMEOUT_MS = 30_000L;

    // "unknown topic" is deliberately absent: a freshly restarted broker
    // answers UNKNOWN_TOPIC until its metadata replay catches up, which is
    // exactly why Kafka classifies UNKNOWN_TOPIC_OR_PARTITION as retriable.
    private static final String[] NON_RETRIABLE_MARKERS = {
        "invalid partition", "max.message.bytes", "corrupt", "out-of-order", "out of order"
    };

    @Test
    void rollingRestartUnderLoadLosesNothingAndThrowsNothingFatal(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        var configs = new Broker.Config[3];
        try (var cluster = TestBrokerCluster.start(3, 3, (i, voters, ports) -> {
            configs[i] = new Broker.Config(new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters)
                    .withChaosPort(ports[i][2]);
            return configs[i];
        })) {
            var current =
                    new AtomicReferenceArray<>(new Broker[] {cluster.broker(0), cluster.broker(1), cluster.broker(2)});
            int[] brokerPorts = {cluster.brokerPort(0), cluster.brokerPort(1), cluster.brokerPort(2)};
            waitForClusterReady(current);

            try (var client = new BrokerClient("127.0.0.1", pickLeaderPort(current, brokerPorts))) {
                client.createTopic("rolling", PARTITIONS, 3);
            }
            for (int p = 0; p < PARTITIONS; p++) {
                waitForIsrSize(current, "rolling", p, 3, CONVERGE_TIMEOUT_MS);
            }

            var running = new AtomicBoolean(true);
            var ackedPerPartition = new AtomicLongArray(PARTITIONS);
            var nonRetriable = new CopyOnWriteArrayList<String>();
            var producer = new Thread(
                    () -> produceLoop(current, brokerPorts, running, ackedPerPartition, nonRetriable),
                    "rolling-producer");
            producer.start();

            try {
                // Let load establish a baseline before the first restart,
                // and breathe between restarts so every phase of the roll
                // sees traffic.
                Thread.sleep(1_500);
                for (int i = 0; i < 3; i++) {
                    current.get(i).closeGracefully(DRAIN_TIMEOUT_MS);
                    current.set(i, Broker.start(configs[i]));
                    for (int p = 0; p < PARTITIONS; p++) {
                        waitForIsrSize(current, "rolling", p, 3, CONVERGE_TIMEOUT_MS);
                    }
                    Thread.sleep(1_500);
                }
            } finally {
                running.set(false);
                producer.join(10_000);
            }

            assertThat(nonRetriable)
                    .as("rolling restart must not surface non-retriable client errors")
                    .isEmpty();

            // Nothing acked was lost: every partition serves at least as
            // many records as were acked to the producer. (≥, not ==ᅳ an
            // ambiguous failure after append may legitimately land without
            // an ack.)
            long ackedTotal = 0;
            try (var client = new BrokerClient("127.0.0.1", pickLeaderPort(current, brokerPorts))) {
                for (int p = 0; p < PARTITIONS; p++) {
                    long acked = ackedPerPartition.get(p);
                    ackedTotal += acked;
                    assertThat((long) client.fetchAll("rolling", p, 1 << 22).size())
                            .as("partition %d serves every acked record", p)
                            .isGreaterThanOrEqualTo(acked);
                }
            }
            assertThat(ackedTotal)
                    .as("the load actually produced through the roll")
                    .isGreaterThan(50L);
        } finally {
            // Restarted brokers are not the instances the cluster tracks.
            // (Cluster teardown tolerates its already-closed originals.)
        }
    }

    private static void produceLoop(
            AtomicReferenceArray<Broker> brokers,
            int[] brokerPorts,
            AtomicBoolean running,
            AtomicLongArray ackedPerPartition,
            List<String> nonRetriable) {
        // Metadata-driven routing, like a real client: resolve the
        // partition's current leader from broker state and produce to it
        // directly, re-resolving on any failure. Blind rotation would burn
        // an acks=all deadline per miss and starve the load.
        var clients = new BrokerClient[brokerPorts.length];
        for (int i = 0; i < brokerPorts.length; i++) {
            clients[i] = new BrokerClient("127.0.0.1", brokerPorts[i]);
        }
        try {
            long seq = 0;
            while (running.get()) {
                int partition = (int) (seq % PARTITIONS);
                int leader = resolveLeader(brokers, partition);
                if (leader < 1 || leader > clients.length) {
                    pause();
                    continue;
                }
                try {
                    clients[leader - 1].produceAcksAll("rolling", partition, ("m" + seq).getBytes());
                    ackedPerPartition.incrementAndGet(partition);
                    seq++;
                } catch (RuntimeException e) {
                    String msg = String.valueOf(e.getMessage()).toLowerCase(java.util.Locale.ROOT);
                    for (var marker : NON_RETRIABLE_MARKERS) {
                        if (msg.contains(marker)) {
                            nonRetriable.add(e.getMessage());
                            break;
                        }
                    }
                    pause();
                }
            }
        } finally {
            for (var c : clients) {
                try {
                    c.close();
                } catch (RuntimeException ignored) {
                    /* teardown */
                }
            }
        }
    }

    /** Latest leader id any broker's metadata reports for the partition, or -1. */
    private static int resolveLeader(AtomicReferenceArray<Broker> brokers, int partition) {
        for (int i = 0; i < brokers.length(); i++) {
            try {
                var state = brokers.get(i)
                        .topics()
                        .partitionState("rolling", partition)
                        .orElse(null);
                if (state != null && state.leader() > 0) return state.leader();
            } catch (RuntimeException ignored) {
                // broker mid-restart; ask the next one
            }
        }
        return -1;
    }

    private static void pause() {
        try {
            Thread.sleep(50);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    // ---------- helpers ----------

    private static int pickLeaderPort(AtomicReferenceArray<Broker> brokers, int[] ports) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            for (int i = 0; i < brokers.length(); i++) {
                if (brokers.get(i).role() == Role.LEADER) return ports[i];
            }
            Thread.sleep(50);
        }
        throw new AssertionError("no Raft leader within 10s");
    }

    private static void waitForIsrSize(
            AtomicReferenceArray<Broker> brokers, String topic, int partition, int size, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean everyoneAgrees = true;
            for (int i = 0; i < brokers.length(); i++) {
                var state =
                        brokers.get(i).topics().partitionState(topic, partition).orElse(null);
                if (state == null || state.isr().size() < size) {
                    everyoneAgrees = false;
                    break;
                }
            }
            if (everyoneAgrees) return;
            Thread.sleep(100);
        }
        throw new AssertionError(
                "ISR for " + topic + "-" + partition + " did not reach " + size + " within " + timeoutMs + "ms");
    }

    private static void waitForClusterReady(AtomicReferenceArray<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            int leaders = 0;
            boolean full = true;
            for (int i = 0; i < brokers.length(); i++) {
                var b = brokers.get(i);
                if (b.role() == Role.LEADER) leaders++;
                if (!b.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3))) full = false;
            }
            if (leaders == 1 && full) return;
            Thread.sleep(50);
        }
        throw new IllegalStateException("cluster did not converge in 15s");
    }
}
