package jbroker.broker.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * Accumulation, ordering, and retry logic of {@link BatchingProducer},
 * exercised against a fake {@link BatchingProducer.BatchSender} — no broker.
 * Timing-sensitive paths (linger, delivery deadline) avoid sleeps: they
 * assert on future completion with generous timeouts, and every batching
 * boundary that CAN be deterministic (size trigger, sequence contiguity,
 * order) is asserted exactly.
 */
class BatchingProducerTest {

    /** Batch-size threshold no test's payload volume ever reaches. */
    private static final int HUGE_BYTES = 1 << 20;

    /** Linger no test waits out — only size, flush, or close can trigger. */
    private static final long HUGE_LINGER_MS = 3_600_000;

    @Test
    void sizeTriggeredFlushSealsBatchAtThreshold() throws Exception {
        var fake = new FakeSender();
        // 100-byte values encode to ~106 bytes each on top of the 61-byte
        // batch header, so a 250-byte threshold seals after exactly two.
        var config = new BatchingProducer.Config(250, HUGE_LINGER_MS, 120_000, 1);
        try (var producer = new BatchingProducer(fake, config)) {
            assertThat(fake.initCalls.get())
                    .as("producer id allocation is lazy")
                    .isZero();

            var f0 = producer.send("t", 0, value(100));
            var f1 = producer.send("t", 0, value(100));
            var f2 = producer.send("t", 0, value(100));

            assertThat(f0.get(10, TimeUnit.SECONDS)).isEqualTo(0L);
            assertThat(f1.get(10, TimeUnit.SECONDS)).isEqualTo(1L);
            assertThat(fake.attempts).hasSize(1);
            assertThat(fake.attempts.get(0).values()).hasSize(2);
            assertThat(fake.initCalls.get()).isEqualTo(1);
            assertThat(fake.attempts.get(0).producerId()).isEqualTo(FakeSender.PRODUCER_ID);

            // The third record starts a new batch and nothing can flush it
            // yet: it is under the size threshold and the linger is an hour.
            assertThat(f2.isDone()).isFalse();

            producer.flush();
            assertThat(f2.get(10, TimeUnit.SECONDS)).isEqualTo(2L);
            assertThat(fake.attempts).hasSize(2);
            assertThat(fake.attempts.get(1).baseSequence()).isEqualTo(2);
        }
    }

    @Test
    void lingerTriggeredFlushDeliversWithoutSizePressureOrExplicitFlush() throws Exception {
        var fake = new FakeSender();
        var config = new BatchingProducer.Config(HUGE_BYTES, 25, 120_000, 1);
        try (var producer = new BatchingProducer(fake, config)) {
            var f0 = producer.send("t", 0, value(10));
            var f1 = producer.send("t", 0, value(10));
            var f2 = producer.send("t", 0, value(10));

            // No flush() call: only the linger can push these out. Batch
            // boundaries are timing-dependent (a stall between sends may
            // split them), so assert delivery + order, not batch count.
            assertThat(f0.get(10, TimeUnit.SECONDS)).isEqualTo(0L);
            assertThat(f1.get(10, TimeUnit.SECONDS)).isEqualTo(1L);
            assertThat(f2.get(10, TimeUnit.SECONDS)).isEqualTo(2L);
            assertThat(totalRecords(fake, "t", 0)).isEqualTo(3);
            assertSequencesContiguous(fake, "t", 0);
        }
    }

    @Test
    void partitionsKeepOrderContiguousSequencesAndOneBatchInFlightPerPartition() throws Exception {
        var fake = new FakeSender();
        // Small threshold so the 100 sends split into many batches per
        // partition; huge linger so size is the only trigger until flush.
        var config = new BatchingProducer.Config(200, HUGE_LINGER_MS, 120_000, 1);
        List<List<CompletableFuture<Long>>> futures = List.of(new ArrayList<>(), new ArrayList<>());
        try (var producer = new BatchingProducer(fake, config)) {
            for (int i = 0; i < 100; i++) {
                int partition = i % 2;
                int seq = i / 2;
                byte[] payload = ("p" + partition + "-" + seq).getBytes(StandardCharsets.UTF_8);
                futures.get(partition).add(producer.send("t", partition, payload));
            }
            producer.flush();
        }

        for (int partition = 0; partition < 2; partition++) {
            // Offsets are 0..49 in send order — no gaps, no reordering.
            for (int seq = 0; seq < 50; seq++) {
                assertThat(futures.get(partition).get(seq).get(10, TimeUnit.SECONDS))
                        .as("partition %d record %d", partition, seq)
                        .isEqualTo((long) seq);
            }
            // Replaying the partition's batches in arrival order must
            // reproduce the exact send order of the payloads.
            var replayed = new ArrayList<String>();
            for (var attempt : fake.attemptsFor("t", partition)) {
                for (byte[] v : attempt.values()) {
                    replayed.add(new String(v, StandardCharsets.UTF_8));
                }
            }
            final int p = partition;
            assertThat(replayed)
                    .hasSize(50)
                    .isEqualTo(IntStream.range(0, 50)
                            .mapToObj(seq -> "p" + p + "-" + seq)
                            .toList());
            assertSequencesContiguous(fake, "t", partition);
            // Per-partition, never globally: distinct partitions may overlap.
            assertThat(fake.maxInFlightFor("t", partition))
                    .as("batches for one partition must never pipeline — sequence dedup relies on it")
                    .isEqualTo(1);
        }
    }

    @Test
    void partitionsShipConcurrentlyOnePartitionsRpcNeverBlocksAnothers() throws Exception {
        var p0Entered = new CountDownLatch(1);
        var p0Release = new CountDownLatch(1);
        // Scripted transport: partition 0's RPC parks until released;
        // partition 1 commits immediately. The single producer writes each
        // partition from empty, so offset == baseSequence + count - 1.
        var scripted = new BatchingProducer.BatchSender() {
            @Override
            public long initProducerId() {
                return 1L;
            }

            @Override
            public long send(
                    String topic,
                    int partition,
                    long producerId,
                    int producerEpoch,
                    int baseSequence,
                    List<byte[]> values) {
                if (partition == 0) {
                    p0Entered.countDown();
                    await(p0Release);
                }
                return baseSequence + values.size() - 1;
            }
        };
        // One-byte threshold: every record seals its batch on send().
        var config = new BatchingProducer.Config(1, HUGE_LINGER_MS, 120_000, 1);
        try (var producer = new BatchingProducer(scripted, config)) {
            var parked = producer.send("t", 0, value(10));
            assertThat(p0Entered.await(10, TimeUnit.SECONDS))
                    .as("partition 0's RPC reached the transport")
                    .isTrue();
            try {
                // Partition 0's RPC is parked INSIDE the transport, yet
                // partition 1's batches must still go out and complete.
                var f0 = producer.send("t", 1, value(10));
                var f1 = producer.send("t", 1, value(10));
                assertThat(f0.get(10, TimeUnit.SECONDS)).isEqualTo(0L);
                assertThat(f1.get(10, TimeUnit.SECONDS)).isEqualTo(1L);
                assertThat(parked.isDone())
                        .as("partition 0's RPC is still in flight")
                        .isFalse();
            } finally {
                p0Release.countDown();
            }
            assertThat(parked.get(10, TimeUnit.SECONDS)).isEqualTo(0L);
        }
    }

    @Test
    void deliveryFailureOnOnePartitionLeavesTheOthersStreamHealthy() throws Exception {
        // Scripted transport: partition 0 is down for good, partition 1 is
        // healthy and assigns offset == baseSequence + count - 1.
        var scripted = new BatchingProducer.BatchSender() {
            @Override
            public long initProducerId() {
                return 1L;
            }

            @Override
            public long send(
                    String topic,
                    int partition,
                    long producerId,
                    int producerEpoch,
                    int baseSequence,
                    List<byte[]> values) {
                if (partition == 0) throw new RuntimeException("partition 0 unavailable");
                return baseSequence + values.size() - 1;
            }
        };
        var config = new BatchingProducer.Config(HUGE_BYTES, HUGE_LINGER_MS, /*deliveryTimeoutMs*/ 250, 10);
        try (var producer = new BatchingProducer(scripted, config)) {
            var doomed = producer.send("t", 0, value(10));
            var healthy = producer.send("t", 1, value(10));
            producer.flush();

            assertThat(healthy.get(10, TimeUnit.SECONDS)).isEqualTo(0L);
            assertThat(doomed.isCompletedExceptionally()).isTrue();
            assertThatThrownBy(() -> doomed.get(10, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasMessageContaining("delivery deadline exceeded")
                    .hasRootCauseMessage("partition 0 unavailable");

            // The failure is scoped to partition 0's stream: partition 1
            // keeps delivering, its sequence continuing where it left off.
            var after = producer.send("t", 1, value(10));
            producer.flush();
            assertThat(after.get(10, TimeUnit.SECONDS)).isEqualTo(1L);
        }
    }

    @Test
    void futuresCompleteWithAbsoluteOffsetsFromTheBroker() throws Exception {
        var fake = new FakeSender();
        // The partition already holds 100 records; the broker's assigned
        // offsets must flow through to the futures verbatim.
        fake.nextOffset.put("t-0", 100L);
        var config = new BatchingProducer.Config(HUGE_BYTES, HUGE_LINGER_MS, 120_000, 1);
        try (var producer = new BatchingProducer(fake, config)) {
            var futures = new ArrayList<CompletableFuture<Long>>();
            for (int i = 0; i < 5; i++) {
                futures.add(producer.send("t", 0, value(8)));
            }
            producer.flush();
            for (int i = 0; i < 5; i++) {
                assertThat(futures.get(i).get(10, TimeUnit.SECONDS)).isEqualTo(100L + i);
            }
        }
    }

    @Test
    void closeFlushesPendingRecordsAndRejectsNewOnes() throws Exception {
        var fake = new FakeSender();
        var config = new BatchingProducer.Config(HUGE_BYTES, HUGE_LINGER_MS, 120_000, 1);
        var producer = new BatchingProducer(fake, config);
        var f0 = producer.send("t", 0, value(10));
        var f1 = producer.send("t", 0, value(10));
        producer.close();

        assertThat(f0.get(10, TimeUnit.SECONDS)).isEqualTo(0L);
        assertThat(f1.get(10, TimeUnit.SECONDS)).isEqualTo(1L);
        assertThatThrownBy(() -> producer.send("t", 0, value(10)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
        // Idempotent close.
        producer.close();
    }

    @Test
    void retriableFailureResendsTheSameSequenceAndValues() throws Exception {
        var fake = new FakeSender();
        fake.failuresRemaining = 1;
        var config = new BatchingProducer.Config(HUGE_BYTES, HUGE_LINGER_MS, 120_000, 1);
        try (var producer = new BatchingProducer(fake, config)) {
            var f0 = producer.send("t", 0, value(10));
            var f1 = producer.send("t", 0, value(10));
            producer.flush();

            assertThat(f0.get(10, TimeUnit.SECONDS)).isEqualTo(0L);
            assertThat(f1.get(10, TimeUnit.SECONDS)).isEqualTo(1L);
            // The failed attempt and its retry must be byte-identical: same
            // base sequence, same records — that's what lets the broker
            // dedup a retry whose original actually landed.
            assertThat(fake.attempts).hasSize(2);
            assertThat(fake.attempts.get(1).baseSequence())
                    .isEqualTo(fake.attempts.get(0).baseSequence());
            assertThat(fake.attempts.get(1).values())
                    .isEqualTo(fake.attempts.get(0).values());
            assertThat(fake.initCalls.get()).as("no fresh producer id on retry").isEqualTo(1);

            // The next batch continues the sequence past the delivered one.
            var f2 = producer.send("t", 0, value(10));
            producer.flush();
            assertThat(f2.get(10, TimeUnit.SECONDS)).isEqualTo(2L);
            assertThat(fake.attempts.get(2).baseSequence()).isEqualTo(2);
        }
    }

    @Test
    void deliveryDeadlineExhaustionFailsTheAffectedFutures() {
        var fake = new FakeSender();
        fake.failuresRemaining = Integer.MAX_VALUE;
        var config = new BatchingProducer.Config(HUGE_BYTES, HUGE_LINGER_MS, /*deliveryTimeoutMs*/ 250, 10);
        try (var producer = new BatchingProducer(fake, config)) {
            var future = producer.send("t", 0, value(10));
            producer.flush(); // returns once the batch has failed

            assertThat(future.isCompletedExceptionally()).isTrue();
            assertThatThrownBy(() -> future.get(10, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasMessageContaining("delivery deadline exceeded")
                    .hasRootCauseMessage("transient send failure");
            // It kept retrying up to the deadline, not just once.
            assertThat(fake.attempts.size()).isGreaterThan(1);
        }
    }

    @Test
    void configDefaultsToNoCompressionAndRejectsNull() {
        assertThat(BatchingProducer.Config.defaults().compression()).isEqualTo(jbroker.storage.Compression.NONE);
        // The pre-codec constructor keeps its meaning: no compression.
        assertThat(new BatchingProducer.Config(1, 0, 1, 1).compression()).isEqualTo(jbroker.storage.Compression.NONE);
        assertThatThrownBy(() -> new BatchingProducer.Config(1, 0, 1, 1, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("compression");
    }

    // ---- helpers ----

    private static byte[] value(int size) {
        return new byte[size];
    }

    /** Bounded latch wait: a failed test releases (or times out) rather than hangs. */
    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch timed out");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static int totalRecords(FakeSender fake, String topic, int partition) {
        return fake.attemptsFor(topic, partition).stream()
                .mapToInt(a -> a.values().size())
                .sum();
    }

    /** Each delivered batch must start where the previous one ended. */
    private static void assertSequencesContiguous(FakeSender fake, String topic, int partition) {
        int expected = 0;
        for (var attempt : fake.attemptsFor(topic, partition)) {
            assertThat(attempt.baseSequence())
                    .as("%s-%d batch base sequence", topic, partition)
                    .isEqualTo(expected);
            expected += attempt.values().size();
        }
    }

    /**
     * In-memory {@link BatchingProducer.BatchSender}: records every send
     * attempt (including ones it fails on purpose) and assigns contiguous
     * offsets per partition like a healthy single broker would.
     */
    private static final class FakeSender implements BatchingProducer.BatchSender {

        static final long PRODUCER_ID = 77L;

        record Attempt(String topic, int partition, long producerId, int baseSequence, List<byte[]> values) {}

        final List<Attempt> attempts = Collections.synchronizedList(new ArrayList<>());
        final ConcurrentHashMap<String, Long> nextOffset = new ConcurrentHashMap<>();
        final AtomicInteger initCalls = new AtomicInteger();
        final ConcurrentHashMap<String, AtomicInteger> inFlight = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, AtomicInteger> maxInFlight = new ConcurrentHashMap<>();

        /** Sends to fail (with a RuntimeException) before succeeding. */
        volatile int failuresRemaining;

        @Override
        public long initProducerId() {
            initCalls.incrementAndGet();
            return PRODUCER_ID;
        }

        @Override
        public long send(
                String topic,
                int partition,
                long producerId,
                int producerEpoch,
                int baseSequence,
                List<byte[]> values) {
            String key = topic + "-" + partition;
            int current =
                    inFlight.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
            maxInFlight.computeIfAbsent(key, k -> new AtomicInteger()).updateAndGet(m -> Math.max(m, current));
            try {
                attempts.add(new Attempt(topic, partition, producerId, baseSequence, List.copyOf(values)));
                if (failuresRemaining > 0) {
                    failuresRemaining--;
                    throw new RuntimeException("transient send failure");
                }
                long end = nextOffset.merge(key, (long) values.size(), Long::sum);
                return end - 1;
            } finally {
                inFlight.get(key).decrementAndGet();
            }
        }

        /** Highest number of concurrently in-flight RPCs seen for one partition. */
        int maxInFlightFor(String topic, int partition) {
            var max = maxInFlight.get(topic + "-" + partition);
            return max == null ? 0 : max.get();
        }

        /** Successful and failed attempts for one partition, in arrival order. */
        List<Attempt> attemptsFor(String topic, int partition) {
            synchronized (attempts) {
                return attempts.stream()
                        .filter(a -> a.topic().equals(topic) && a.partition() == partition)
                        .toList();
            }
        }
    }
}
