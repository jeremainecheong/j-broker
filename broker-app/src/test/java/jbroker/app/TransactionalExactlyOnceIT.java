package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import jbroker.broker.client.ClusterClient;
import jbroker.broker.client.TransactionalProducer;
import jbroker.broker.client.consumer.Consumer;
import jbroker.broker.client.consumer.ConsumerConfig;
import jbroker.broker.client.consumer.ConsumerRecord;
import jbroker.broker.client.consumer.StringDeserializer;
import jbroker.broker.txn.TxnStateTopic;
import jbroker.proto.broker.FetchOffsetsRequest;
import jbroker.proto.broker.FetchRequest;
import jbroker.proto.common.ErrorCode;
import jbroker.proto.common.TopicPartition;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import jbroker.storage.ControlRecord;
import jbroker.storage.RecordBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Exactly-once acceptance gate for transactions: a consume-transform-
 * produce loop — read_committed consumer on the source topic, transform,
 * transactional produce to the sink with the source offsets committed in
 * the same transaction — runs with ZERO application-side error handling
 * beyond the transactional API's own abort-and-retry loop. Mid-run the
 * transaction coordinator's partition leader is killed abruptly once, and
 * the sink partition's leader once, both while transactions are flowing;
 * the first victim is restarted between the kills only so the second kill
 * leaves a Raft majority. At the end, the sink's committed records under
 * read_committed equal the transformed source records exactly once, in
 * order — list equality — with every aborted attempt invisible, and the
 * group's committed offset equals the source size.
 *
 * <p>A deterministic probe phase additionally pins the visibility
 * contract on a separate topic: an open transaction's data is invisible
 * to read_committed and its staged offsets invisible to FetchOffsets
 * until the commit marker lands; an aborted transaction's data stays
 * invisible forever (while read_uncommitted proves the bytes exist).
 *
 * <p>Application code is confined to {@link #transformLoop}: a plain
 * loop calling poll / transact / send / sendOffsetsToTransaction. A
 * structural assertion re-reads this source file and verifies the loop
 * contains no {@code try} or {@code catch} — exactly-once must come from
 * the client contract, not the app.
 */
class TransactionalExactlyOnceIT {

    private static final String SRC = "ctp-src";
    private static final String SINK = "ctp-sink";
    private static final String PROBE = "ctp-probe";
    private static final String GROUP = "ctp-app";
    private static final String TXN_ID = "ctp-app-txn";
    private static final String PROBE_TXN_ID = "ctp-probe-txn";
    private static final String PROBE_GROUP = "ctp-probe-group";
    private static final int TOTAL = 300;

    private static final int CI_MULT =
            "1".equals(System.getenv("JBROKER_CI")) || "true".equalsIgnoreCase(System.getenv("CI")) ? 4 : 1;

    // ---------------------------------------------------------------
    // Application code. One plain loop, no error handling — the
    // structural assertion below enforces it. The transactional API's
    // transact() is the contract's abort-and-retry loop.
    // ---------------------------------------------------------------

    private static void transformLoop(
            Consumer<String, String> source, TransactionalProducer producer, AtomicInteger processed, int expected) {
        source.subscribe(List.of(SRC), null);
        producer.initTransactions();
        while (processed.get() < expected) {
            var records = source.poll(Duration.ofMillis(200));
            if (records.isEmpty()) continue;
            var batch = new ArrayList<ConsumerRecord<String, String>>(records.all());
            long nextOffset = batch.get(batch.size() - 1).offset() + 1;
            producer.transact(() -> {
                for (var record : batch) {
                    producer.send(SINK, 0, ("t-" + record.value()).getBytes(StandardCharsets.UTF_8));
                }
                producer.sendOffsetsToTransaction(GROUP, Map.of(tp(SRC, 0), nextOffset));
            });
            processed.addAndGet(batch.size());
        }
    }

    // ---------------------------------------------------------------
    // Harness.
    // ---------------------------------------------------------------

    @Test
    void consumeTransformProduceSurvivesCoordinatorAndSinkLeaderKillsExactlyOnce(
            @TempDir Path d1, @TempDir Path d2, @TempDir Path d3) throws Exception {
        int r1 = freePort(), r2 = freePort(), r3 = freePort();
        int b1 = freePort(), b2 = freePort(), b3 = freePort();
        var voters = List.of(
                new VoterAddress(new NodeId(1), "127.0.0.1", r1, b1),
                new VoterAddress(new NodeId(2), "127.0.0.1", r2, b2),
                new VoterAddress(new NodeId(3), "127.0.0.1", r3, b3));
        // Balancer off: a preferred-leader flip between the kills would move
        // leadership off the broker the test is about to target.
        var configs = new ConcurrentHashMap<Integer, Broker.Config>();
        configs.put(1, new Broker.Config(new NodeId(1), d1, r1, b1, voters).withBalancerTiming(3_600_000, 3_600_000));
        configs.put(2, new Broker.Config(new NodeId(2), d2, r2, b2, voters).withBalancerTiming(3_600_000, 3_600_000));
        configs.put(3, new Broker.Config(new NodeId(3), d3, r3, b3, voters).withBalancerTiming(3_600_000, 3_600_000));
        var running = new ConcurrentHashMap<Integer, Broker>();
        for (int id = 1; id <= 3; id++) running.put(id, Broker.start(configs.get(id)));

        var cluster = new ClusterClient(List.of("127.0.0.1:" + b1, "127.0.0.1:" + b2, "127.0.0.1:" + b3));
        Consumer<String, String> source = null;
        Consumer<String, String> verifier = null;
        TransactionalProducer producer = null;
        Thread transformThread = null;
        try {
            awaitSingleRaftLeader(running.values());
            awaitRegistryConvergence(running.values());
            for (var topic : List.of(SRC, SINK, PROBE)) {
                cluster.createTopic(topic, 1, 3);
                awaitPartitionStateOnAll(running.values(), topic, 0);
            }
            awaitTxnCoordinator(running);

            // Seed the source with the full feed up front (acks=all).
            var sent = new ArrayList<String>(TOTAL);
            for (int i = 0; i < TOTAL; i++) sent.add("r-" + i);
            long seedPid = cluster.initProducerId();
            cluster.produceIdempotentBatchAcksAll(SRC, 0, toBytes(sent), seedPid, 0, 0);

            // Deterministic visibility probe before the main run.
            probeVisibilityContract(cluster, running);

            source = new Consumer<>(
                    ConsumerConfig.builder(GROUP)
                            .isolationLevel(ConsumerConfig.IsolationLevel.READ_COMMITTED)
                            .maxPollRecords(25)
                            .build(),
                    new StringDeserializer(),
                    new StringDeserializer(),
                    cluster);
            producer = new TransactionalProducer(
                    cluster,
                    TXN_ID,
                    new TransactionalProducer.Config(
                            // The coordinator must never timeout-abort a
                            // transaction that is merely stalled on a
                            // failover; the sweep would fence the producer.
                            /*transactionTimeoutMs*/ 1_800_000,
                            /*retryBackoffMs*/ 50,
                            /*opDeadlineMs*/ 120_000L * CI_MULT,
                            /*transactDeadlineMs*/ 480_000L * CI_MULT));

            var processed = new AtomicInteger();
            var transformFailure = new AtomicReference<Throwable>();
            var fs = source;
            var fp = producer;
            transformThread = thread("app-transform", transformFailure, () -> transformLoop(fs, fp, processed, TOTAL));

            // Phase 1: healthy progress, then kill the transaction
            // coordinator's partition leader with transactions in flight.
            awaitProcessed(processed, TOTAL / 3, transformFailure, "phase 1 on a healthy cluster");
            int coordinatorVictim = killTxnCoordinatorLeader(running);

            // Phase 2: the loop must keep committing on the survivors alone
            // (successor coordinator activates from the replicated
            // __transaction_state log; ISR shrinks to the two survivors).
            awaitProcessed(processed, 2 * TOTAL / 3, transformFailure, "phase 2 across the coordinator kill");

            // Restart the coordinator victim only for Raft majority: the
            // next kill would otherwise leave a single voter that can
            // commit neither metadata nor acks=all appends. Wait until it
            // is back in every partition's ISR so the second kill again
            // leaves two in-sync survivors everywhere.
            running.put(coordinatorVictim, Broker.start(configs.get(coordinatorVictim)));
            for (var topic : List.of(SRC, SINK, PROBE, TxnStateTopic.NAME, "__consumer_offsets")) {
                awaitFullIsr(running, topic, 0);
            }

            // Phase 3: kill the sink partition's leader with transactions
            // in flight, then drain to completion on the survivors.
            killLeaderOf(running, SINK, 0);
            awaitProcessed(processed, TOTAL, transformFailure, "phase 3 across the sink leader kill");

            transformThread.join(60_000L * CI_MULT);
            assertThat(transformFailure.get()).as("transform loop saw no error").isNull();
            assertThat(transformThread.isAlive())
                    .as("transform loop drained all %d records", TOTAL)
                    .isFalse();

            // The heart of the gate: the sink's committed records under
            // read_committed equal the transformed source exactly once, in
            // order — list equality. Aborted attempts are invisible.
            var expected = new ArrayList<String>(TOTAL);
            for (var v : sent) expected.add("t-" + v);
            verifier = new Consumer<>(
                    ConsumerConfig.builder("ctp-verify")
                            .isolationLevel(ConsumerConfig.IsolationLevel.READ_COMMITTED)
                            .build(),
                    new StringDeserializer(),
                    new StringDeserializer(),
                    cluster);
            var committed = drainAll(verifier, SINK, expected.size());
            assertThat(committed).isEqualTo(expected);

            // The group's offsets moved transactionally with the data: the
            // committed offset equals the full source size (the last
            // commit's marker fold may lag the loop by a beat).
            awaitCommittedOffset(cluster, GROUP, SRC, TOTAL);

            assertLoopContainsNoErrorHandling();
        } finally {
            if (transformThread != null) {
                transformThread.interrupt();
                transformThread.join(10_000);
            }
            closeQuietly(source);
            closeQuietly(verifier);
            closeQuietly(producer);
            cluster.close();
            for (var b : running.values()) closeQuietly(b);
        }
    }

    // ---------------------------------------------------------------
    // Probe phase: the visibility contract, deterministically.
    // ---------------------------------------------------------------

    private static void probeVisibilityContract(ClusterClient cluster, ConcurrentHashMap<Integer, Broker> running)
            throws Exception {
        try (var probe = new TransactionalProducer(cluster, PROBE_TXN_ID)) {
            probe.initTransactions();

            // Open transaction: data acked but invisible to read_committed,
            // staged offsets invisible to FetchOffsets.
            probe.beginTransaction();
            probe.send(PROBE, 0, "probe-committed".getBytes(StandardCharsets.UTF_8));
            probe.sendOffsetsToTransaction(PROBE_GROUP, Map.of(tp(SRC, 0), 1L));
            assertThat(awaitIsolatedRead(cluster, PROBE, /*readCommitted*/ true))
                    .as("open transaction invisible to read_committed")
                    .isEmpty();
            assertThat(awaitIsolatedRead(cluster, PROBE, /*readCommitted*/ false))
                    .as("read_uncommitted sees the acked record of the open transaction")
                    .containsExactly("probe-committed");
            assertThat(committedOffsetOf(cluster, PROBE_GROUP, SRC))
                    .as("staged offsets invisible to FetchOffsets before the commit")
                    .isEqualTo(-1L);

            // Commit: both become visible once the marker lands.
            probe.commitTransaction();
            awaitProbeState(cluster, List.of("probe-committed"), 1L, "after the commit marker");

            // Abort: the aborted attempt's data and offsets never surface —
            // while read_uncommitted proves the bytes physically exist.
            probe.beginTransaction();
            probe.send(PROBE, 0, "probe-aborted".getBytes(StandardCharsets.UTF_8));
            probe.sendOffsetsToTransaction(PROBE_GROUP, Map.of(tp(SRC, 0), 2L));
            probe.abortTransaction();
            awaitAbortMarker(running, PROBE, 0, probe.producerId());
            assertThat(awaitIsolatedRead(cluster, PROBE, true))
                    .as("aborted data invisible to read_committed after the abort marker")
                    .containsExactly("probe-committed");
            assertThat(awaitIsolatedRead(cluster, PROBE, false))
                    .as("read_uncommitted sees the aborted record")
                    .containsExactly("probe-committed", "probe-aborted");
            assertThat(committedOffsetOf(cluster, PROBE_GROUP, SRC))
                    .as("aborted offsets discarded")
                    .isEqualTo(1L);
        }
    }

    private static void awaitProbeState(ClusterClient cluster, List<String> records, long offset, String phase)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 20_000L * CI_MULT;
        List<String> lastRead = null;
        long lastOffset = Long.MIN_VALUE;
        while (System.currentTimeMillis() < deadline) {
            lastRead = isolatedRead(cluster, PROBE, true);
            lastOffset = committedOffsetOf(cluster, PROBE_GROUP, SRC);
            if (records.equals(lastRead) && offset == lastOffset) return;
            Thread.sleep(100);
        }
        throw new AssertionError(phase + ": expected " + records + " @ committed offset " + offset + ", saw " + lastRead
                + " @ " + lastOffset);
    }

    private static void awaitAbortMarker(
            ConcurrentHashMap<Integer, Broker> running, String topic, int partition, long pid) throws Exception {
        long deadline = System.currentTimeMillis() + 15_000L * CI_MULT;
        while (System.currentTimeMillis() < deadline) {
            for (var broker : running.values()) {
                try {
                    var log = broker.logManager().logFor(topic, partition);
                    long offset = 0;
                    long end = log.nextOffset();
                    while (offset < end) {
                        var batches = log.read(offset, 1 << 20);
                        if (batches.isEmpty()) break;
                        for (var b : batches) {
                            if (b.control()
                                    && b.producerId() == pid
                                    && b.controlRecord().type() == ControlRecord.Type.ABORT) {
                                return;
                            }
                            offset = b.lastOffset() + 1;
                        }
                    }
                } catch (RuntimeException ignored) {
                    // broker mid-shutdown or not hosting the log yet
                }
            }
            Thread.sleep(100);
        }
        throw new AssertionError("no ABORT marker for pid " + pid + " on " + topic + "-" + partition);
    }

    // ---------------------------------------------------------------
    // Isolated raw reads (probe assertions need LSO-precise fetches).
    // ---------------------------------------------------------------

    /** Retry {@link #isolatedRead} through transient routing errors until it answers. */
    private static List<String> awaitIsolatedRead(ClusterClient cluster, String topic, boolean readCommitted)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000L * CI_MULT;
        while (System.currentTimeMillis() < deadline) {
            var read = isolatedRead(cluster, topic, readCommitted);
            if (read != null) return read;
            Thread.sleep(100);
        }
        throw new AssertionError("no successful " + (readCommitted ? "read_committed" : "read_uncommitted")
                + " fetch of " + topic + " within budget");
    }

    /**
     * Full read of the topic under the given isolation level, decoding the
     * raw batches the way the consumer client does: control batches are
     * skipped, and under read_committed the response's aborted ranges drop
     * transactional batches of aborted producers until their ABORT marker.
     * Returns null on a transient envelope error (caller retries).
     */
    private static List<String> isolatedRead(ClusterClient cluster, String topic, boolean readCommitted) {
        var out = new ArrayList<String>();
        var abortedProducers = new java.util.HashSet<Long>();
        long offset = 0;
        while (true) {
            jbroker.proto.broker.FetchResponse resp;
            try {
                resp = cluster.fetch(
                        FetchRequest.newBuilder()
                                .setTopic(topic)
                                .setPartition(0)
                                .setOffset(offset)
                                .setMaxBytes(1 << 20)
                                .setIsolationLevel(readCommitted ? 1 : 0)
                                .build(),
                        10_000L * CI_MULT);
            } catch (RuntimeException e) {
                return null;
            }
            if (resp.hasError() && resp.getError().getCode() != 0) return null;
            var pendingAborts = new java.util.PriorityQueue<jbroker.proto.broker.AbortedTxn>(
                    java.util.Comparator.comparingLong(jbroker.proto.broker.AbortedTxn::getFirstOffset));
            if (readCommitted) pendingAborts.addAll(resp.getAbortedTxnsList());
            var buf = java.nio.ByteBuffer.wrap(resp.getRecords().toByteArray());
            long nextOffset = -1;
            while (buf.remaining() >= RecordBatch.BATCH_OVERHEAD) {
                int mark = buf.position();
                try {
                    var parsed = RecordBatch.decode(buf);
                    nextOffset = Math.max(nextOffset, parsed.lastOffset() + 1);
                    if (parsed.control()) {
                        if (readCommitted && parsed.controlRecord().type() == ControlRecord.Type.ABORT) {
                            abortedProducers.remove(parsed.producerId());
                        }
                        continue;
                    }
                    if (readCommitted) {
                        while (!pendingAborts.isEmpty()
                                && pendingAborts.peek().getFirstOffset() <= parsed.lastOffset()) {
                            abortedProducers.add(pendingAborts.poll().getProducerId());
                        }
                        if (parsed.transactional() && abortedProducers.contains(parsed.producerId())) {
                            continue;
                        }
                    }
                    for (var rec : parsed.records()) {
                        if (parsed.baseOffset() + rec.offsetDelta() < offset) continue;
                        out.add(new String(rec.value(), StandardCharsets.UTF_8));
                    }
                } catch (IllegalArgumentException e) {
                    buf.position(mark);
                    break;
                }
            }
            // Progress guard: a fetch at the log end may re-serve the
            // window below it (index-floor read); only forward progress
            // continues the walk.
            if (nextOffset <= offset) return out;
            offset = nextOffset;
        }
    }

    private static long committedOffsetOf(ClusterClient cluster, String group, String topic) {
        var resp = cluster.fetchOffsets(
                FetchOffsetsRequest.newBuilder()
                        .setGroupId(group)
                        .addTps(tp(topic, 0))
                        .build(),
                15_000L * CI_MULT);
        var result = resp.getResults(0);
        return result.getError() == ErrorCode.OK ? result.getOffset() : -1L;
    }

    private static void awaitCommittedOffset(ClusterClient cluster, String group, String topic, long expected)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000L * CI_MULT;
        long last = Long.MIN_VALUE;
        while (System.currentTimeMillis() < deadline) {
            last = committedOffsetOf(cluster, group, topic);
            if (last == expected) return;
            Thread.sleep(100);
        }
        throw new AssertionError(
                "committed offset for " + group + " on " + topic + ": expected " + expected + ", saw " + last);
    }

    /** Poll {@code verifier} until {@code expected} records arrive, then prove no extras follow. */
    private static List<String> drainAll(Consumer<String, String> verifier, String topic, int expected)
            throws InterruptedException {
        verifier.subscribe(List.of(topic), null);
        var out = new ArrayList<String>();
        long deadline = System.currentTimeMillis() + 60_000L * CI_MULT;
        while (out.size() < expected && System.currentTimeMillis() < deadline) {
            for (var record : verifier.poll(Duration.ofMillis(200))) {
                out.add(record.value());
            }
        }
        // A few extra polls: duplicates or aborted leakage would surface here.
        for (int i = 0; i < 3; i++) {
            for (var record : verifier.poll(Duration.ofMillis(200))) {
                out.add(record.value());
            }
        }
        return out;
    }

    // ---------------------------------------------------------------
    // Kill orchestration.
    // ---------------------------------------------------------------

    private static int killTxnCoordinatorLeader(ConcurrentHashMap<Integer, Broker> running) throws Exception {
        int partition = txnCoordinatorPartition(running);
        return killLeaderOf(running, TxnStateTopic.NAME, partition);
    }

    private static int txnCoordinatorPartition(ConcurrentHashMap<Integer, Broker> running) {
        for (var broker : running.values()) {
            try {
                var desc = broker.topics().describe(TxnStateTopic.NAME);
                if (desc.isPresent()) {
                    return TxnStateTopic.partitionFor(TXN_ID, desc.get().partitions());
                }
            } catch (RuntimeException ignored) {
                // closed broker — try the next one
            }
        }
        throw new AssertionError("no live broker knows " + TxnStateTopic.NAME);
    }

    private static int killLeaderOf(ConcurrentHashMap<Integer, Broker> running, String topic, int partition)
            throws Exception {
        long deadline = System.currentTimeMillis() + 30_000L * CI_MULT;
        while (System.currentTimeMillis() < deadline) {
            for (var broker : running.values()) {
                int leader = broker.topics()
                        .partitionState(topic, partition)
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
        throw new AssertionError("no running leader of " + topic + "-" + partition + " to kill");
    }

    private static void awaitFullIsr(ConcurrentHashMap<Integer, Broker> running, String topic, int partition)
            throws InterruptedException {
        long budget = 90_000L * CI_MULT;
        long deadline = System.currentTimeMillis() + budget;
        List<Integer> lastIsr = List.of();
        while (System.currentTimeMillis() < deadline) {
            for (var broker : running.values()) {
                var state = broker.topics().partitionState(topic, partition);
                if (state.isEmpty()) continue;
                lastIsr = state.get().isr();
                if (lastIsr.containsAll(running.keySet())) return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError(
                topic + "-" + partition + " ISR did not regain all brokers within " + budget + "ms; last=" + lastIsr);
    }

    private static void awaitProcessed(AtomicInteger processed, int n, AtomicReference<Throwable> failure, String phase)
            throws InterruptedException {
        long budget = 120_000L * CI_MULT;
        long deadline = System.currentTimeMillis() + budget;
        while (System.currentTimeMillis() < deadline) {
            if (failure.get() != null) {
                throw new AssertionError("transform loop died during " + phase, failure.get());
            }
            if (processed.get() >= n) return;
            Thread.sleep(50);
        }
        throw new AssertionError(
                phase + ": only " + processed.get() + " of " + n + " records processed within " + budget + "ms");
    }

    // ---------------------------------------------------------------
    // Structural no-error-handling check.
    // ---------------------------------------------------------------

    private static void assertLoopContainsNoErrorHandling() throws IOException {
        var sourceFile = Path.of("src", "test", "java", "jbroker", "app", "TransactionalExactlyOnceIT.java");
        assertThat(sourceFile)
                .as("test source visible from the working directory")
                .exists();
        String text = Files.readString(sourceFile);
        String body = methodBody(text, "transformLoop");
        assertThat(Pattern.compile("\\b(try|catch)\\b").matcher(body).find())
                .as("transformLoop contains no try/catch — exactly-once belongs to the transactional client")
                .isFalse();
    }

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

    // ---------------------------------------------------------------
    // Cluster plumbing (same shape as the sibling multi-broker gates).
    // ---------------------------------------------------------------

    private static void awaitTxnCoordinator(ConcurrentHashMap<Integer, Broker> running) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000L * CI_MULT;
        while (System.currentTimeMillis() < deadline) {
            var desc = running.values().iterator().next().topics().describe(TxnStateTopic.NAME);
            if (desc.isPresent()) {
                int partition = TxnStateTopic.partitionFor(TXN_ID, desc.get().partitions());
                var leader = running.values().iterator().next().topics().partitionLeader(TxnStateTopic.NAME, partition);
                if (leader.isPresent() && leader.get() > 0) return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError(TxnStateTopic.NAME + " never became coordinatable");
    }

    private static List<byte[]> toBytes(List<String> values) {
        var out = new ArrayList<byte[]>(values.size());
        for (var v : values) out.add(v.getBytes(StandardCharsets.UTF_8));
        return out;
    }

    private static TopicPartition tp(String topic, int partition) {
        return TopicPartition.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .build();
    }

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

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void closeQuietly(AutoCloseable c) {
        if (c == null) return;
        try {
            c.close();
        } catch (Exception ignored) {
            // best-effort teardown
        }
    }

    private static void awaitSingleRaftLeader(Iterable<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000L * CI_MULT;
        while (System.currentTimeMillis() < deadline) {
            long leaders = 0;
            for (var b : brokers) if (b.role() == Role.LEADER) leaders++;
            if (leaders == 1) return;
            Thread.sleep(50);
        }
        throw new AssertionError("no single Raft leader within budget");
    }

    private static void awaitRegistryConvergence(Iterable<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000L * CI_MULT;
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
        throw new AssertionError("broker registry did not converge");
    }

    private static void awaitPartitionStateOnAll(Iterable<Broker> brokers, String topic, int partition)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000L * CI_MULT;
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
        throw new AssertionError("partition state did not converge for " + topic + "-" + partition);
    }
}
