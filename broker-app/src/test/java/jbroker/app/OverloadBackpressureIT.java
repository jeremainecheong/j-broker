package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.broker.ErrorCodes;
import jbroker.broker.client.BrokerClient;
import jbroker.proto.broker.InitProducerIdRequest;
import jbroker.proto.broker.ProduceRequest;
import jbroker.proto.broker.ProduceResponse;
import jbroker.proto.broker.ProducerGrpc;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Graceful degradation under overload: when producers offer far more than
 * the cluster can currently absorb, the broker must shed load with
 * retriable errors and bounded rejection latency — never crash, never lose
 * an acked record — and resume acking the moment conditions clear.
 *
 * <p>Absolute-rate overload (N unpaced threads out-running the disk) is
 * nondeterministic on shared CI runners: a fast machine absorbs any rate
 * the test can offer and nothing degrades. So the overload is provoked
 * through the broker's own backpressure surface instead: replica fetch
 * from both followers is cut (cooperative chaos partition, inbound-only at
 * the leader so Raft and client traffic keep flowing), which is exactly
 * what saturation looks like to the acks=all produce path — replication no
 * longer keeps up with offered load. Producers then observe the two
 * degraded shapes in order: in-flight appends time out their HWM wait
 * ({@code NOT_ENOUGH_REPLICAS} after the 5s acks=all budget), and once the
 * ISR shrinks below {@code min.insync.replicas} every produce is refused
 * pre-append, fast. Healing the partition lets the followers catch up, the
 * ISR re-expands, and acks flow again. The quota and disk-headroom gates
 * were considered and rejected: the broker app wires the quota enforcer as
 * a no-op, and the headroom watermark measures real volume free space,
 * which shared runners change under the test's feet.
 *
 * <p>Invariants asserted — deliberately no absolute throughput numbers:
 * <ul>
 *   <li><b>Only retriable errors:</b> every produce failure carries a code
 *       from the retriable taxonomy ({@code NOT_ENOUGH_REPLICAS},
 *       {@code NOT_LEADER}); anything else — or any unclassified exception
 *       — fails the test.</li>
 *   <li><b>Stays responsive:</b> metadata RPCs against the stressed leader
 *       answer within their deadline for the whole run.</li>
 *   <li><b>Fast rejection:</b> with the ISR below the floor, a fresh
 *       produce is refused pre-append well inside the acks=all timeout and
 *       leaves nothing in the log.</li>
 *   <li><b>No loss, no duplication:</b> every acked payload appears
 *       exactly once in the final read-back, in per-producer sequence
 *       order (idempotent producers retry the same sequence until acked,
 *       so retry storms cannot double-append).</li>
 *   <li><b>Recovers:</b> after heal, every producer acks a further batch
 *       within a bounded window and all three logs converge to the same
 *       tail.</li>
 * </ul>
 */
class OverloadBackpressureIT {

    // CI runners fsync + schedule 2–3× slower; scale deadlines so the gate
    // validates "it happens", not laptop-specific timing.
    private static final int CI_MULT =
            "1".equals(System.getenv("JBROKER_CI")) || "true".equalsIgnoreCase(System.getenv("CI")) ? 4 : 1;

    private static final String TOPIC = "overload";
    private static final int PRODUCERS = 4;
    private static final int PAYLOAD_BYTES = 4 * 1024;
    private static final int BASELINE_ACKS_PER_PRODUCER = 20;
    private static final int RECOVERY_ACKS_PER_PRODUCER = 12;
    /** Sustainable pacing between sends outside the overload window. */
    private static final long SUSTAINABLE_PACE_MS = 10L;
    /** Extra unpaced-storm time after the ISR floor trips, to pile up fast rejections. */
    private static final long STORM_EXTRA_MS = 3_000L;
    /**
     * The broker's acks=all wait is 5s; a pre-append rejection must come
     * back well inside that or "fast fail" is really a timeout. Fixed on
     * purpose — scaling it past 5s would let the slow path pass.
     */
    private static final long FAST_REJECT_MAX_MS = 4_000L;
    /** Client-side stub deadline (7s) plus scheduling grace; accepted work must resolve inside it. */
    private static final long ACKED_LATENCY_MAX_MS = 8_000L;

    /** Produce-path errors the taxonomy marks retriable and this scenario may legitimately surface. */
    private static final Set<Integer> RETRIABLE = Set.of(ErrorCodes.NOT_ENOUGH_REPLICAS, ErrorCodes.NOT_LEADER);

    @Test
    void overloadDegradesWithRetriableErrorsAndRecoversWithoutLoss(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        try (var cluster = TestBrokerCluster.start(3, 3, (i, voters, ports) -> new Broker.Config(
                        new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters)
                .withChaosPort(ports[i][2])
                // Producers pin to the leader they started with, so the
                // balancer must not move partition leadership mid-run.
                .withBalancerTiming(60_000L, 600_000L))) {
            awaitSingleLeader(cluster.brokers());
            awaitRegistryConvergence(cluster.brokers());

            try (var admin = new BrokerClient(
                    "127.0.0.1", raftLeaderOf(cluster.brokers()).brokerPort())) {
                admin.createTopic(TOPIC, 1, /*rf*/ 3);
            }
            int leaderId = awaitPartitionLeader(cluster, 15_000L * CI_MULT);
            var leader = cluster.broker(leaderId - 1);
            int leaderPort = cluster.brokerPort(leaderId - 1);
            int leaderChaosPort = cluster.port(leaderId - 1, 2);
            var followerIds = new ArrayList<Integer>();
            for (int id = 1; id <= 3; id++) if (id != leaderId) followerIds.add(id);

            var stats = new LoadStats(PRODUCERS);
            var pace = new AtomicLong(SUSTAINABLE_PACE_MS);
            var stop = new AtomicBoolean(false);
            var proberStop = new AtomicBoolean(false);
            var proberFailures = new CopyOnWriteArrayList<String>();
            var proberMaxNanos = new LongAccumulator(Long::max, 0L);

            var prober = new Thread(
                    () -> runMetadataProber(leaderPort, proberStop, proberFailures, proberMaxNanos), "overload-prober");
            prober.start();

            var producers = new ArrayList<Thread>();
            for (int t = 0; t < PRODUCERS; t++) {
                int idx = t;
                var thread =
                        new Thread(() -> runProducer(idx, leaderPort, stats, pace, stop), "overload-producer-" + t);
                thread.start();
                producers.add(thread);
            }

            // --- Baseline: paced load against a healthy ISR; every send acks.
            awaitEachAckedAtLeast(stats, new int[PRODUCERS], BASELINE_ACKS_PER_PRODUCER, 60_000L * CI_MULT, "baseline");
            long baselineErrors = totalRetriable(stats);

            // --- Overload: cut replica fetch from both followers at the
            // leader (inbound-only: Raft, client produce/fetch, and metadata
            // all keep flowing), then drop pacing entirely. Offered load now
            // exceeds what the cluster can durably accept — by construction.
            var http = HttpClient.newHttpClient();
            for (int f : followerIds) {
                chaosPost(http, leaderChaosPort, "/debug/chaos/partition?peer=" + f + "&direction=inbound");
            }
            pace.set(0L);

            // First the HWM waits time out; after replica.lag (10s) + tick
            // (2s) the ISR shrinks to just the leader — below the floor of 2.
            awaitIsrSize(leader, 1, 45_000L * CI_MULT, stats);

            // Fast-rejection probe: a fresh non-idempotent batch against the
            // shrunk ISR must be refused pre-append, quickly, and leave no
            // trace in the log (the payload is asserted absent below).
            String rejectedProbePayload = "probe-must-be-rejected-" + System.nanoTime();
            long probeStart = System.nanoTime();
            ProduceResponse probeResp;
            try (var probe = new RawProducer(leaderPort)) {
                probeResp = probe.produceAcksAll(
                        TOPIC, rejectedProbePayload.getBytes(StandardCharsets.UTF_8), /*producerId*/ -1L, /*seq*/ -1);
            }
            long probeMs = (System.nanoTime() - probeStart) / 1_000_000L;
            assertThat(probeResp.hasError() && probeResp.getError().getCode() == ErrorCodes.NOT_ENOUGH_REPLICAS)
                    .as("produce against a below-floor ISR is refused with NOT_ENOUGH_REPLICAS, got %s", probeResp)
                    .isTrue();
            assertThat(probeResp.getError().getMessage())
                    .as("the refusal happens before the append, not after a replication wait")
                    .contains("rejected before append");
            assertThat(probeMs)
                    .as("below-floor rejection must fail fast, not burn the acks=all timeout")
                    .isLessThan(FAST_REJECT_MAX_MS);

            // Let the unpaced retry storm hammer the fast-reject path.
            Thread.sleep(STORM_EXTRA_MS);

            // --- Recovery: heal, return to the sustainable rate, and require
            // acks to flow again for every producer within a bounded window.
            int[] atHeal = ackedSnapshot(stats);
            chaosPost(http, leaderChaosPort, "/debug/chaos/heal-partition");
            pace.set(SUSTAINABLE_PACE_MS);
            awaitIsrSize(leader, 3, 60_000L * CI_MULT, stats);
            awaitEachAckedAtLeast(stats, atHeal, RECOVERY_ACKS_PER_PRODUCER, 60_000L * CI_MULT, "recovery");

            stop.set(true);
            for (var t : producers) {
                t.join(30_000L * CI_MULT);
                assertThat(t.isAlive()).as("%s finished", t.getName()).isFalse();
            }
            proberStop.set(true);
            prober.join(10_000L);

            // (b) Taxonomy: nothing fatal, nothing unclassified, and the
            // overload really produced backpressure errors.
            assertThat(stats.fatal)
                    .as("no fatal or unclassified producer errors")
                    .isEmpty();
            assertThat(stats.retriableByCode.keySet())
                    .as("every broker-reported produce error is retriable")
                    .isSubsetOf(RETRIABLE);
            long backpressureErrors = stats.retriableByCode
                    .getOrDefault(ErrorCodes.NOT_ENOUGH_REPLICAS, new LongAdder())
                    .sum();
            assertThat(backpressureErrors)
                    .as("the overload window surfaced NOT_ENOUGH_REPLICAS backpressure")
                    .isGreaterThan(0L);

            // (a) The stressed leader answered metadata RPCs throughout.
            assertThat(proberFailures)
                    .as("metadata stayed responsive under overload")
                    .isEmpty();

            // Bounded latency for accepted work across all stages.
            assertThat(stats.maxAckedNanos.get() / 1_000_000L)
                    .as("acked produces resolve within the acks=all budget")
                    .isLessThan(ACKED_LATENCY_MAX_MS);

            // (c) Zero loss, zero duplication, per-producer order. Every send
            // was retried to an ack (same sequence, so the broker dedups),
            // which makes the read-back an exact multiset check.
            awaitLogTailConvergence(cluster, 30_000L * CI_MULT);
            List<String> fetched;
            try (var reader = new BrokerClient("127.0.0.1", leaderPort)) {
                fetched = reader.fetchAll(TOPIC, 0, 1 << 20).stream()
                        .map(b -> new String(b, StandardCharsets.UTF_8))
                        .toList();
            }
            var fetchedSet = new HashSet<>(fetched);
            assertThat(fetched.size()).as("no duplicate records in the log").isEqualTo(fetchedSet.size());
            var missing = new ArrayList<String>();
            for (var p : stats.acked) if (!fetchedSet.contains(p)) missing.add(prefixOf(p));
            assertThat(missing).as("every acked payload is in the read-back").isEmpty();
            var aliens = new ArrayList<String>();
            for (var p : fetchedSet) if (!stats.acked.contains(p)) aliens.add(prefixOf(p));
            assertThat(aliens).as("nothing unacked leaked into the read-back").isEmpty();
            assertThat(fetchedSet).doesNotContain(rejectedProbePayload);
            assertPerProducerSequenceOrder(fetched);

            System.out.printf(
                    "overload-backpressure: acked=%d baselineErrs=%d retriable=%s clientDeadlines=%d "
                            + "probeReject=%dms proberMax=%.1fms ackedMax=%.1fms%n",
                    stats.acked.size(),
                    baselineErrors,
                    stats.retriableByCode,
                    stats.clientDeadlines.sum(),
                    probeMs,
                    proberMaxNanos.get() / 1e6,
                    stats.maxAckedNanos.get() / 1e6);
        }
    }

    // ---- load harness ----

    /** Shared counters the producer threads and the main thread communicate through. */
    private static final class LoadStats {
        final Set<String> acked = ConcurrentHashMap.newKeySet();
        final AtomicInteger[] ackedPerProducer;
        final ConcurrentHashMap<Integer, LongAdder> retriableByCode = new ConcurrentHashMap<>();
        final ConcurrentHashMap<Integer, String> sampleMessageByCode = new ConcurrentHashMap<>();
        final LongAdder clientDeadlines = new LongAdder();
        final CopyOnWriteArrayList<String> fatal = new CopyOnWriteArrayList<>();
        final LongAccumulator maxAckedNanos = new LongAccumulator(Long::max, 0L);

        LoadStats(int producers) {
            ackedPerProducer = new AtomicInteger[producers];
            for (int i = 0; i < producers; i++) ackedPerProducer[i] = new AtomicInteger();
        }
    }

    /**
     * One idempotent producer: sends sequence 0,1,2,… and retries the SAME
     * sequence until it is acked, so an append whose ack failed can never
     * be duplicated by its retry — the broker dedups on (pid, epoch, seq).
     * Retriable broker errors are counted; anything else is fatal and ends
     * the thread (the main thread asserts on it).
     */
    private static void runProducer(
            int idx, int brokerPort, LoadStats stats, AtomicLong paceMillis, AtomicBoolean stop) {
        try (var producer = new RawProducer(brokerPort)) {
            long producerId = producer.initProducerId();
            int seq = 0;
            while (true) {
                String payload = payload(idx, seq);
                byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
                boolean acked = false;
                while (!acked) {
                    long t0 = System.nanoTime();
                    try {
                        var resp = producer.produceAcksAll(TOPIC, bytes, producerId, seq);
                        if (!resp.hasError() || resp.getError().getCode() == ErrorCodes.NONE) {
                            stats.maxAckedNanos.accumulate(System.nanoTime() - t0);
                            stats.acked.add(payload);
                            stats.ackedPerProducer[idx].incrementAndGet();
                            acked = true;
                        } else {
                            int code = resp.getError().getCode();
                            if (!RETRIABLE.contains(code)) {
                                stats.fatal.add("producer " + idx + " seq " + seq + ": non-retriable code " + code
                                        + " — " + resp.getError().getMessage());
                                return;
                            }
                            stats.retriableByCode
                                    .computeIfAbsent(code, c -> new LongAdder())
                                    .increment();
                            stats.sampleMessageByCode.putIfAbsent(
                                    code, resp.getError().getMessage());
                        }
                    } catch (StatusRuntimeException e) {
                        // The stub deadline (7s) sits above the broker's 5s
                        // acks wait, so an expiry here is client-side
                        // impatience under scheduling noise, not a broker
                        // verdict. Ambiguous outcome — the same-sequence
                        // retry dedups if the append actually landed.
                        if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                            stats.clientDeadlines.increment();
                        } else {
                            stats.fatal.add("producer " + idx + " seq " + seq + ": unexpected " + e.getStatus());
                            return;
                        }
                    }
                    long pause = paceMillis.get();
                    if (!acked && pause > 0) sleepQuietly(pause);
                }
                seq++;
                if (stop.get()) return; // exit only on an acked boundary — every started sequence ends acked
                long pause = paceMillis.get();
                if (pause > 0) sleepQuietly(pause);
            }
        } catch (Throwable t) {
            stats.fatal.add("producer " + idx + " died: " + t);
        }
    }

    /** Polls the leader's admin surface; any failure or missed deadline is an unresponsiveness finding. */
    private static void runMetadataProber(
            int brokerPort, AtomicBoolean stop, List<String> failures, LongAccumulator maxNanos) {
        try (var client = new BrokerClient("127.0.0.1", brokerPort)) {
            while (!stop.get()) {
                long t0 = System.nanoTime();
                try {
                    client.listTopics();
                    maxNanos.accumulate(System.nanoTime() - t0);
                } catch (Exception e) {
                    failures.add("metadata probe failed after " + (System.nanoTime() - t0) / 1_000_000L + "ms: " + e);
                }
                sleepQuietly(250L);
            }
        } catch (Throwable t) {
            failures.add("metadata prober died: " + t);
        }
    }

    /** Bare produce stub that returns the raw response, so callers read broker error codes, not exception text. */
    private static final class RawProducer implements AutoCloseable {
        private final ManagedChannel channel;
        private final ProducerGrpc.ProducerBlockingStub stub;

        RawProducer(int port) {
            this.channel = NettyChannelBuilder.forAddress("127.0.0.1", port)
                    .usePlaintext()
                    .build();
            this.stub = ProducerGrpc.newBlockingStub(channel);
        }

        long initProducerId() {
            var resp = stub.withDeadlineAfter(10, TimeUnit.SECONDS)
                    .initProducerId(InitProducerIdRequest.getDefaultInstance());
            if (resp.hasError() && resp.getError().getCode() != 0) {
                throw new IllegalStateException(
                        "initProducerId failed: " + resp.getError().getMessage());
            }
            return resp.getProducerId();
        }

        /** acks=all produce; {@code producerId <= 0} sends the legacy non-idempotent shape. */
        ProduceResponse produceAcksAll(String topic, byte[] value, long producerId, int baseSequence) {
            var records = List.of(new Record(0, 0L, null, value));
            var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
            long now = System.currentTimeMillis();
            RecordBatch.encode(buf, 0L, 0, now, now, producerId, (short) 0, baseSequence, records);
            buf.flip();
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            // A hair above the broker's 5s acks=all wait so the server-side
            // NOT_ENOUGH_REPLICAS verdict surfaces instead of a client
            // DEADLINE_EXCEEDED.
            return stub.withDeadlineAfter(7, TimeUnit.SECONDS)
                    .produce(ProduceRequest.newBuilder()
                            .setTopic(topic)
                            .setPartition(0)
                            .setBatch(ByteString.copyFrom(bytes))
                            .setProducerId(producerId)
                            .setProducerEpoch(0)
                            .setBaseSequence(baseSequence)
                            .setAcks(-1)
                            .build());
        }

        @Override
        public void close() {
            channel.shutdownNow();
            try {
                channel.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static String payload(int producerIndex, int seq) {
        var prefix = "t" + producerIndex + "-s" + String.format("%06d", seq) + "-";
        return prefix + "x".repeat(PAYLOAD_BYTES - prefix.length());
    }

    private static String prefixOf(String payload) {
        return payload.length() <= 16 ? payload : payload.substring(0, 16);
    }

    /** Records must appear in strictly increasing sequence order within each producer. */
    private static void assertPerProducerSequenceOrder(List<String> fetched) {
        int[] lastSeq = new int[PRODUCERS];
        Arrays.fill(lastSeq, -1);
        for (var value : fetched) {
            var parts = value.split("-", 3);
            int producer = Integer.parseInt(parts[0].substring(1));
            int seq = Integer.parseInt(parts[1].substring(1));
            assertThat(seq)
                    .as("producer %d records in send order (previous seq %d)", producer, lastSeq[producer])
                    .isGreaterThan(lastSeq[producer]);
            lastSeq[producer] = seq;
        }
    }

    private static int[] ackedSnapshot(LoadStats stats) {
        int[] out = new int[stats.ackedPerProducer.length];
        for (int i = 0; i < out.length; i++) out[i] = stats.ackedPerProducer[i].get();
        return out;
    }

    private static long totalRetriable(LoadStats stats) {
        long sum = 0;
        for (var adder : stats.retriableByCode.values()) sum += adder.sum();
        return sum;
    }

    private static void awaitEachAckedAtLeast(LoadStats stats, int[] base, int delta, long timeoutMs, String stage)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (!stats.fatal.isEmpty()) {
                throw new AssertionError("producer failure during " + stage + ": " + stats.fatal);
            }
            boolean all = true;
            for (int i = 0; i < stats.ackedPerProducer.length; i++) {
                if (stats.ackedPerProducer[i].get() < base[i] + delta) {
                    all = false;
                    break;
                }
            }
            if (all) return;
            Thread.sleep(50);
        }
        throw new AssertionError(stage + ": producers did not reach +" + delta + " acks within " + timeoutMs
                + "ms; counts=" + Arrays.toString(ackedSnapshot(stats)) + " errors=" + stats.retriableByCode
                + " clientDeadlines=" + stats.clientDeadlines.sum());
    }

    // ---- cluster plumbing ----

    private static void chaosPost(HttpClient http, int chaosPort, String pathAndQuery) throws Exception {
        var resp = http.send(
                HttpRequest.newBuilder()
                        .uri(URI.create("http://127.0.0.1:" + chaosPort + pathAndQuery))
                        .POST(HttpRequest.BodyPublishers.noBody())
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).as("chaos %s", pathAndQuery).isEqualTo(200);
    }

    private static void awaitIsrSize(Broker onBroker, int expected, long timeoutMs, LoadStats stats)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (!stats.fatal.isEmpty()) {
                throw new AssertionError("producer failure while waiting on ISR size " + expected + ": " + stats.fatal);
            }
            var size = onBroker.topics()
                    .partitionState(TOPIC, 0)
                    .map(s -> s.isr().size())
                    .orElse(-1);
            if (size == expected) return;
            Thread.sleep(100);
        }
        throw new AssertionError("ISR did not reach size " + expected + " within " + timeoutMs + "ms; state="
                + onBroker.topics().partitionState(TOPIC, 0));
    }

    /** All three brokers agree on a live leader with a full ISR before load starts. */
    private static int awaitPartitionLeader(TestBrokerCluster cluster, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            Integer agreed = null;
            boolean ok = true;
            for (var b : cluster.brokers()) {
                var state = b.topics().partitionState(TOPIC, 0).orElse(null);
                if (state == null || state.leader() <= 0 || state.isr().size() != 3) {
                    ok = false;
                    break;
                }
                if (agreed == null) agreed = state.leader();
                else if (agreed != state.leader()) {
                    ok = false;
                    break;
                }
            }
            if (ok && agreed != null) return agreed;
            Thread.sleep(50);
        }
        throw new AssertionError("no agreed partition leader with a full ISR within " + timeoutMs + "ms");
    }

    private static void awaitLogTailConvergence(TestBrokerCluster cluster, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long[] leos = new long[3];
        while (System.currentTimeMillis() < deadline) {
            for (int i = 0; i < 3; i++) {
                leos[i] = cluster.broker(i).logManager().logFor(TOPIC, 0).nextOffset();
            }
            if (leos[0] == leos[1] && leos[1] == leos[2]) return;
            Thread.sleep(100);
        }
        throw new AssertionError("log tails did not converge after heal: " + Arrays.toString(leos));
    }

    private static void awaitSingleLeader(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (brokers.stream().filter(b -> b.role() == Role.LEADER).count() == 1) return;
            Thread.sleep(50);
        }
        throw new AssertionError("no single leader within 10s");
    }

    private static void awaitRegistryConvergence(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (brokers.stream()
                    .allMatch(b -> b.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3)))) return;
            Thread.sleep(50);
        }
        throw new AssertionError("broker registry did not converge within 5s");
    }

    private static Broker raftLeaderOf(List<Broker> brokers) {
        return brokers.stream().filter(b -> b.role() == Role.LEADER).findFirst().orElseThrow();
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
