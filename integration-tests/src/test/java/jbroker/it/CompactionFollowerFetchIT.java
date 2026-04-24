package jbroker.it;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import jbroker.app.Broker;
import jbroker.app.VoterAddress;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import jbroker.storage.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * compaction on the leader while followers (and clients)
 * concurrently fetch from the same partition.
 *
 * <p>{@link ForceCompactIT} proves the gRPC fetch path returns
 * sparse-offset survivors after compaction, but it runs single-broker so
 * it doesn't exercise the "compaction and replica-fetch overlap" path.
 * This IT does:
 *
 * <ol>
 *   <li>Spin up a 3-broker RF=3 compact-policy topic. Seed distinct-key
 *       records directly on the leader's log (the gRPC produce path
 *       doesn't expose keys today) and wait for follower LEOs to
 *       converge — ReplicaFetchers copy raw batches off the leader so
 *       followers end up with the full keyed history.</li>
 *   <li>Launch continuous {@code fetch()} threads against every broker
 *       (leader + both followers) so the leader's acceptor + both
 *       followers' local log readers stay busy.</li>
 *   <li>Trigger {@code forceCompactPartition} on the leader — its log
 *       gets rewritten in-place during the fetch storm.</li>
 *   <li>Continue fetching for a short drain window, then stop.</li>
 *   <li>Assert: no fetch threw, leader post-compaction returns exactly
 *       the distinct-key survivors, and each follower's LEO has NOT
 *       regressed (the leader's compaction is local and must not
 *       propagate truncation to followers).</li>
 * </ol>
 */
class CompactionFollowerFetchIT {

    private static final int TOTAL_RECORDS = 500;
    private static final int DISTINCT_KEYS = 50;
    private static final int FETCH_BYTES = 4 * 1024 * 1024;
    private static final long DRAIN_AFTER_COMPACT_MS = 500L;
    private static final int FETCHER_THREADS_PER_BROKER = 2;

    @Test
    void leaderCompactionDoesNotCorruptConcurrentFollowerFetches(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
        int r1 = freePort(), r2 = freePort(), r3 = freePort();
        int b1 = freePort(), b2 = freePort(), b3 = freePort();
        var voters = List.of(
                new VoterAddress(new NodeId(1), "127.0.0.1", r1, b1),
                new VoterAddress(new NodeId(2), "127.0.0.1", r2, b2),
                new VoterAddress(new NodeId(3), "127.0.0.1", r3, b3));

        try (var br1 = Broker.start(new Broker.Config(new NodeId(1), d1, r1, b1, voters));
                var br2 = Broker.start(new Broker.Config(new NodeId(2), d2, r2, b2, voters));
                var br3 = Broker.start(new Broker.Config(new NodeId(3), d3, r3, b3, voters))) {
            waitForClusterReady(br1, br2, br3);

            var brokers = List.of(br1, br2, br3);
            int[] brokerPorts = {b1, b2, b3};

            int raftLeaderPort = pickLeaderPort(brokers, brokerPorts);
            try (var client = new BrokerClient("127.0.0.1", raftLeaderPort)) {
                client.createTopicWithConfig("audit06", 1, 3, Map.of("cleanup.policy", "compact"));
            }

            int partitionLeaderId = partitionLeaderId(brokers, "audit06", 0);
            int partitionLeaderPort = brokerPorts[partitionLeaderId - 1];
            Broker partitionLeader = brokers.get(partitionLeaderId - 1);

            // Seed TOTAL_RECORDS records across DISTINCT_KEYS keys on the
            // leader. ReplicaFetchers on followers mirror the raw batches
            // so after convergence every broker holds the same history.
            var leaderLog = partitionLeader.logManager().logFor("audit06", 0);
            for (int i = 0; i < TOTAL_RECORDS; i++) {
                String key = "k" + (i % DISTINCT_KEYS);
                String value = "v" + i;
                leaderLog.append(
                        List.of(new Record(0, 0L, key.getBytes(UTF_8), value.getBytes(UTF_8))),
                        System.currentTimeMillis());
            }
            waitForLeoAtLeast(brokers, "audit06", 0, TOTAL_RECORDS);

            // Fetchers run until `running` flips to false. Each fetcher
            // holds its own BrokerClient so the leader is under real TCP
            // fan-in, not just a single session.
            var running = new AtomicBoolean(true);
            var exceptions = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();
            var fetchLoops = new AtomicInteger(0);

            try (var exec = Executors.newVirtualThreadPerTaskExecutor()) {
                var tasks = new java.util.ArrayList<Callable<Void>>();
                for (int port : brokerPorts) {
                    for (int t = 0; t < FETCHER_THREADS_PER_BROKER; t++) {
                        final int p = port;
                        tasks.add(() -> {
                            try (var c = new BrokerClient("127.0.0.1", p)) {
                                while (running.get()) {
                                    try {
                                        c.fetch("audit06", 0, 0L, FETCH_BYTES);
                                        fetchLoops.incrementAndGet();
                                    } catch (Exception e) {
                                        exceptions.add(e);
                                    }
                                }
                            }
                            return null;
                        });
                    }
                }
                var futures = new java.util.ArrayList<java.util.concurrent.Future<Void>>();
                for (var tk : tasks) futures.add(exec.submit(tk));

                // Give fetchers a head start so the compaction overlap is
                // real, not a race between thread startup + compaction.
                Thread.sleep(100);
                int kept;
                try (var client = new BrokerClient("127.0.0.1", partitionLeaderPort)) {
                    kept = client.forceCompactPartition("audit06", 0);
                }
                assertThat(kept)
                        .as("compaction must keep exactly one record per distinct key")
                        .isEqualTo(DISTINCT_KEYS);

                Thread.sleep(DRAIN_AFTER_COMPACT_MS);
                running.set(false);
                for (var f : futures) f.get();
            }

            assertThat(exceptions)
                    .as(
                            "no fetch during compaction may throw; saw %d exceptions after %d loops",
                            exceptions.size(), fetchLoops.get())
                    .isEmpty();

            // Post-compact leader read — exactly the distinct-key
            // survivors, at their sparse pre-compaction offsets.
            try (var client = new BrokerClient("127.0.0.1", partitionLeaderPort)) {
                var after = client.fetchRecords("audit06", 0, 0, FETCH_BYTES);
                assertThat(after)
                        .as("leader fetch must return exactly the distinct-key survivors post-compaction")
                        .hasSize(DISTINCT_KEYS);
            }

            // Follower LEOs must not regress. The leader's in-place
            // compaction is local; followers should still hold the full
            // pre-compaction log (LEO ≥ TOTAL_RECORDS).
            for (int i = 0; i < brokers.size(); i++) {
                if ((i + 1) == partitionLeaderId) continue;
                var follower = brokers.get(i);
                long leo = follower.logManager().logFor("audit06", 0).nextOffset();
                assertThat(leo)
                        .as("follower broker %d LEO must not regress after leader compaction", i + 1)
                        .isGreaterThanOrEqualTo(TOTAL_RECORDS);
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

    private static void waitForLeoAtLeast(List<Broker> brokers, String topic, int partition, long minLeo)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            boolean caughtUp = brokers.stream().allMatch(b -> {
                try {
                    return b.logManager().logFor(topic, partition).nextOffset() >= minLeo;
                } catch (IOException e) {
                    return false;
                }
            });
            if (caughtUp) return;
            Thread.sleep(100);
        }
        throw new AssertionError("not all brokers reached LEO≥" + minLeo + " for " + topic + "-" + partition);
    }

    private static void waitForClusterReady(Broker b1, Broker b2, Broker b3) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            int leaders = (b1.role() == Role.LEADER ? 1 : 0)
                    + (b2.role() == Role.LEADER ? 1 : 0)
                    + (b3.role() == Role.LEADER ? 1 : 0);
            boolean full = b1.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3))
                    && b2.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3))
                    && b3.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3));
            if (leaders == 1 && full) return;
            Thread.sleep(50);
        }
        throw new IllegalStateException("cluster did not converge in 15s");
    }

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
