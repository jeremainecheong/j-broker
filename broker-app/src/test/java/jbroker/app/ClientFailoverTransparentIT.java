package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import jbroker.broker.client.BatchingProducer;
import jbroker.broker.client.ClusterClient;
import jbroker.broker.client.consumer.Consumer;
import jbroker.broker.client.consumer.ConsumerConfig;
import jbroker.broker.client.consumer.StringDeserializer;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Failover-transparency acceptance gate: an application produce loop
 * (ClusterClient-backed {@link BatchingProducer}) and a consume loop
 * (cluster-aware {@link Consumer}) run with ZERO try/catch and no
 * app-written retries. Mid-stream the partition leader is killed
 * abruptly, twice, and after EACH kill the loops must recover on the
 * surviving replicas alone: the surviving follower re-points its
 * fetcher to the new leader, the dead broker is shrunk out of the ISR,
 * and acks=all (min.insync.replicas=2) resumes on the two survivors
 * while the victim is still down. The first victim is restarted only
 * between the kills, after survivor-only recovery has completed, and
 * only because the second kill would otherwise leave a single broker —
 * a Raft minority that can commit neither metadata nor produces. At
 * the end, every acked record is consumed exactly once, in order.
 *
 * <p>The application code is confined to {@link #produceLoop} and
 * {@link #consumeLoop}: plain loops calling send/poll. A structural
 * assertion re-reads this source file and verifies neither loop contains
 * a {@code try} or {@code catch} — failover transparency must come from
 * the client, not the app.
 *
 * <p>Kill orchestration is progress-gated, not timed: each segment of
 * records is fed to the produce loop, the kill happens with the last
 * segment still in flight, and the harness waits on acked counts before
 * moving on — so production deterministically spans both failovers on
 * any machine speed.
 */
class ClientFailoverTransparentIT {

    private static final String TOPIC = "orders";
    private static final int SEGMENT = 1_000;
    private static final int TOTAL = 3 * SEGMENT;
    /** Poison pill ending the produce loop's feed. */
    private static final String END = "__end-of-feed__";

    // ---------------------------------------------------------------
    // Application code. Plain loops, no error handling — the structural
    // assertion below enforces it.
    // ---------------------------------------------------------------

    private static void produceLoop(
            BatchingProducer producer, BlockingQueue<String> feed, List<CompletableFuture<Long>> futures)
            throws InterruptedException {
        for (String value = feed.take(); !value.equals(END); value = feed.take()) {
            futures.add(producer.send(TOPIC, 0, value.getBytes(StandardCharsets.UTF_8)));
        }
        producer.flush();
    }

    private static void consumeLoop(Consumer<String, String> consumer, int expected, List<String> consumed) {
        consumer.subscribe(List.of(TOPIC), null);
        while (consumed.size() < expected) {
            for (var record : consumer.poll(Duration.ofMillis(200))) {
                consumed.add(record.value());
            }
        }
    }

    // ---------------------------------------------------------------
    // Harness.
    // ---------------------------------------------------------------

    @Test
    void produceAndConsumeLoopsSurviveTwoLeaderKillsWithNoAppErrorHandling(
            @TempDir Path d1, @TempDir Path d2, @TempDir Path d3) throws Exception {
        int r1 = freePort(), r2 = freePort(), r3 = freePort();
        int b1 = freePort(), b2 = freePort(), b3 = freePort();
        var voters = List.of(
                new VoterAddress(new NodeId(1), "127.0.0.1", r1, b1),
                new VoterAddress(new NodeId(2), "127.0.0.1", r2, b2),
                new VoterAddress(new NodeId(3), "127.0.0.1", r3, b3));
        // Balancer off: a preferred-leader flip between the two kills would
        // move leadership off the broker this test is about to target.
        var configs = new ConcurrentHashMap<Integer, Broker.Config>();
        configs.put(1, new Broker.Config(new NodeId(1), d1, r1, b1, voters).withBalancerTiming(3_600_000, 3_600_000));
        configs.put(2, new Broker.Config(new NodeId(2), d2, r2, b2, voters).withBalancerTiming(3_600_000, 3_600_000));
        configs.put(3, new Broker.Config(new NodeId(3), d3, r3, b3, voters).withBalancerTiming(3_600_000, 3_600_000));
        var running = new ConcurrentHashMap<Integer, Broker>();
        for (int id = 1; id <= 3; id++) running.put(id, Broker.start(configs.get(id)));

        var cluster = new ClusterClient(List.of("127.0.0.1:" + b1, "127.0.0.1:" + b2, "127.0.0.1:" + b3));
        BatchingProducer producer = null;
        Consumer<String, String> consumer = null;
        Thread producerThread = null;
        Thread consumerThread = null;
        var feed = new LinkedBlockingQueue<String>();
        try {
            awaitSingleRaftLeader(running.values());
            awaitRegistryConvergence(running.values());
            cluster.createTopic(TOPIC, 1, 3);
            awaitPartitionStateOnAll(running.values(), TOPIC, 0);

            producer = BatchingProducer.create(cluster);
            consumer = new Consumer<>(
                    ConsumerConfig.builder("orders-app").build(),
                    new StringDeserializer(),
                    new StringDeserializer(),
                    cluster);

            var sent = new ArrayList<String>(TOTAL);
            for (int i = 0; i < TOTAL; i++) sent.add("r-" + i);
            var futures = Collections.synchronizedList(new ArrayList<CompletableFuture<Long>>(TOTAL));
            var consumed = Collections.synchronizedList(new ArrayList<String>(TOTAL));

            var producerFailure = new AtomicReference<Throwable>();
            var consumerFailure = new AtomicReference<Throwable>();
            var fp = producer;
            var fc = consumer;
            producerThread = thread("app-producer", producerFailure, () -> produceLoop(fp, feed, futures));
            consumerThread = thread("app-consumer", consumerFailure, () -> consumeLoop(fc, TOTAL, consumed));

            // Segment 1: baseline — everything acks against the healthy cluster.
            feed.addAll(sent.subList(0, SEGMENT));
            awaitAcked(futures, SEGMENT, producerFailure, "segment 1 on a healthy cluster");

            // Segment 2 goes in first, THEN the partition leader dies
            // abruptly — batches are in flight across the failover. The
            // loops keep running (retrying inside the client) while a
            // survivor takes over.
            feed.addAll(sent.subList(SEGMENT, 2 * SEGMENT));
            int firstVictim = killPartitionLeader(running);
            awaitNewLeader(running, firstVictim);

            // Recovery must come from the survivors alone: the surviving
            // follower re-points its fetcher to the new leader, the dead
            // broker is shrunk out, and the ISR settles on exactly the two
            // survivors — the victim stays down throughout. Only that
            // two-member ISR lets acks=all (min.insync.replicas=2) resume,
            // so segment 2's acks prove the survivor-only pipeline works.
            awaitIsrOfExactlyTheSurvivors(running);
            awaitAcked(futures, 2 * SEGMENT, producerFailure, "segment 2 across the first leader kill");

            // Restart the first victim only now, for Raft majority: the
            // second kill drops the cluster to two brokers, and with a
            // single voter left neither metadata changes nor acks=all
            // produces could commit. ISR recovery above already completed
            // without it. Waiting for its rejoin exercises a cold restart
            // catching up under a leader it never fetched from, and leaves
            // the next kill a fully-caught-up successor.
            running.put(firstVictim, Broker.start(configs.get(firstVictim)));
            awaitBackInIsr(running, firstVictim);

            // Segment 3 in flight, then kill the CURRENT leader (a broker
            // that survived round one). Recovery again comes only from the
            // two remaining brokers: one is promoted, and the other — a
            // follower of the newly dead leader — must re-point its fetcher
            // before acks=all can complete the segment.
            feed.addAll(sent.subList(2 * SEGMENT, TOTAL));
            killPartitionLeader(running);
            awaitAcked(futures, TOTAL, producerFailure, "segment 3 across the second leader kill");

            feed.add(END);
            producerThread.join(60_000);
            assertThat(producerThread.isAlive())
                    .as("produce loop finished (flush drained)")
                    .isFalse();
            assertThat(producerFailure.get()).as("produce loop saw no error").isNull();
            synchronized (futures) {
                assertThat(futures).hasSize(TOTAL).allMatch(f -> f.isDone() && !f.isCompletedExceptionally());
            }

            long consumeBudgetMs = ciBudget(120_000, 60_000);
            consumerThread.join(consumeBudgetMs);
            assertThat(consumerFailure.get()).as("consume loop saw no error").isNull();
            assertThat(consumerThread.isAlive())
                    .as(
                            "consume loop drained all %d records within %dms (got %d)",
                            TOTAL, consumeBudgetMs, consumed.size())
                    .isFalse();

            // The heart of the gate: every acked record consumed exactly
            // once, in order — list equality, not set containment.
            assertThat(List.copyOf(consumed)).isEqualTo(sent);

            assertLoopsContainNoErrorHandling();
        } finally {
            // Unstick the app threads before touching the clients: the
            // produce loop may be parked on the feed, and the consume loop
            // holds the consumer's monitor almost continuously — closing
            // while it still runs starves close() of the lock forever.
            feed.add(END);
            if (producerThread != null) stopThread(producerThread);
            if (consumerThread != null) stopThread(consumerThread);
            if (consumer != null) closeQuietly(consumer);
            if (producer != null) closeQuietly(producer);
            cluster.close();
            for (var b : running.values()) closeQuietly(b);
        }
    }

    private static void stopThread(Thread t) throws InterruptedException {
        t.interrupt();
        t.join(10_000);
    }

    /** Kill the current partition leader abruptly; returns its broker id. */
    private static int killPartitionLeader(ConcurrentHashMap<Integer, Broker> running) throws Exception {
        long deadline = System.currentTimeMillis() + ciBudget(60_000, 20_000);
        while (System.currentTimeMillis() < deadline) {
            for (var broker : running.values()) {
                int leader = broker.topics()
                        .partitionState(TOPIC, 0)
                        .map(s -> s.leader())
                        .orElse(-1);
                if (leader > 0 && running.containsKey(leader)) {
                    var victim = running.remove(leader);
                    victim.closeAbruptly();
                    return leader;
                }
            }
            Thread.sleep(50);
        }
        throw new AssertionError("no running partition leader to kill");
    }

    /** Wait until the partition has a leader other than {@code oldLeader} on some survivor's view. */
    private static void awaitNewLeader(ConcurrentHashMap<Integer, Broker> running, int oldLeader)
            throws InterruptedException {
        long budget = ciBudget(45_000, 20_000);
        long deadline = System.currentTimeMillis() + budget;
        while (System.currentTimeMillis() < deadline) {
            for (var broker : running.values()) {
                int leader = broker.topics()
                        .partitionState(TOPIC, 0)
                        .map(s -> s.leader())
                        .orElse(-1);
                if (leader > 0 && leader != oldLeader) return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("no new leader emerged within " + budget + "ms after killing broker " + oldLeader);
    }

    /**
     * Wait until every running broker's view of the partition ISR is exactly
     * the running brokers. With the victim still down this pins survivor-only
     * recovery: the dead broker has been shrunk out AND the surviving
     * follower is (still or again) in sync via its re-pointed fetcher.
     */
    private static void awaitIsrOfExactlyTheSurvivors(ConcurrentHashMap<Integer, Broker> running)
            throws InterruptedException {
        var survivors = List.copyOf(running.keySet());
        long budget = ciBudget(90_000, 45_000);
        long deadline = System.currentTimeMillis() + budget;
        List<Integer> lastIsr = List.of();
        while (System.currentTimeMillis() < deadline) {
            boolean all = true;
            for (var broker : running.values()) {
                var state = broker.topics().partitionState(TOPIC, 0);
                if (state.isEmpty()) {
                    all = false;
                    break;
                }
                lastIsr = state.get().isr();
                if (lastIsr.size() != survivors.size() || !lastIsr.containsAll(survivors)) {
                    all = false;
                    break;
                }
            }
            if (all) return;
            Thread.sleep(100);
        }
        throw new AssertionError(
                "ISR did not settle on the survivors " + survivors + " within " + budget + "ms; last isr=" + lastIsr);
    }

    /** Wait until {@code brokerId} is back in the partition's ISR on some survivor's view. */
    private static void awaitBackInIsr(ConcurrentHashMap<Integer, Broker> running, int brokerId)
            throws InterruptedException {
        long budget = ciBudget(90_000, 45_000);
        long deadline = System.currentTimeMillis() + budget;
        List<Integer> lastIsr = List.of();
        while (System.currentTimeMillis() < deadline) {
            for (var broker : running.values()) {
                var state = broker.topics().partitionState(TOPIC, 0);
                if (state.isEmpty()) continue;
                lastIsr = state.get().isr();
                if (lastIsr.contains(brokerId)) return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError(
                "broker " + brokerId + " did not rejoin the ISR within " + budget + "ms; last isr=" + lastIsr);
    }

    /** Wait until at least {@code n} sends have acked; fail fast on any produce error. */
    private static void awaitAcked(
            List<CompletableFuture<Long>> futures, int n, AtomicReference<Throwable> failure, String phase)
            throws InterruptedException {
        long budget = ciBudget(120_000, 60_000);
        long deadline = System.currentTimeMillis() + budget;
        int acked = 0;
        while (System.currentTimeMillis() < deadline) {
            List<CompletableFuture<Long>> snapshot;
            synchronized (futures) {
                snapshot = List.copyOf(futures);
            }
            acked = 0;
            for (var f : snapshot) {
                if (f.isCompletedExceptionally()) {
                    throw new AssertionError("produce failed during " + phase, unwrap(f));
                }
                if (f.isDone()) acked++;
            }
            if (failure.get() != null) {
                throw new AssertionError("produce loop died during " + phase, failure.get());
            }
            if (acked >= n) return;
            Thread.sleep(50);
        }
        throw new AssertionError(phase + ": only " + acked + " of " + n + " records acked within " + budget + "ms");
    }

    private static Throwable unwrap(CompletableFuture<Long> f) {
        try {
            f.join();
            return null;
        } catch (Throwable t) {
            return t;
        }
    }

    /**
     * Structural no-error-handling check: the two application loops must
     * be plain loops — no {@code try}, no {@code catch}. Reads this test's
     * own source (Gradle runs tests with the project dir as working dir).
     */
    private static void assertLoopsContainNoErrorHandling() throws IOException {
        var source = Path.of("src", "test", "java", "jbroker", "app", "ClientFailoverTransparentIT.java");
        assertThat(source).as("test source visible from the working directory").exists();
        String text = Files.readString(source);
        for (String method : List.of("produceLoop", "consumeLoop")) {
            String body = methodBody(text, method);
            assertThat(Pattern.compile("\\b(try|catch)\\b").matcher(body).find())
                    .as("%s contains no try/catch — failover handling belongs to the client", method)
                    .isFalse();
        }
    }

    /** The brace-delimited body of {@code private static void <name>(...)}. */
    private static String methodBody(String source, String name) {
        int start = source.indexOf("private static void " + name);
        assertThat(start).as("method %s present in source", name).isGreaterThanOrEqualTo(0);
        int open = source.indexOf('{', start);
        int depth = 0;
        for (int i = open; i < source.length(); i++) {
            char c = source.charAt(i);
            if (c == '{') depth++;
            if (c == '}') depth--;
            if (depth == 0) return source.substring(open, i + 1);
        }
        throw new AssertionError("unbalanced braces after " + name);
    }

    // ---- cluster plumbing (same shape as the other multi-broker ITs) ----

    private static Thread thread(String name, AtomicReference<Throwable> failure, ThrowingRunnable body) {
        var t = new Thread(
                () -> {
                    try {
                        body.run();
                    } catch (Throwable e) {
                        failure.set(e);
                    }
                },
                name);
        t.setDaemon(true);
        t.start();
        return t;
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static long ciBudget(long ciMs, long localMs) {
        boolean ci = "1".equals(System.getenv("JBROKER_CI")) || "true".equalsIgnoreCase(System.getenv("CI"));
        return ci ? ciMs : localMs;
    }

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void closeQuietly(AutoCloseable c) {
        try {
            c.close();
        } catch (Exception ignored) {
            // best-effort teardown
        }
    }

    private static void awaitSingleRaftLeader(Iterable<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            long leaders = 0;
            for (var b : brokers) if (b.role() == Role.LEADER) leaders++;
            if (leaders == 1) return;
            Thread.sleep(50);
        }
        throw new AssertionError("no single Raft leader within 10s");
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
        throw new AssertionError("partition state did not converge within 10s");
    }
}
