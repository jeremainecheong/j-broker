package jbroker.broker;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Minimal broker-local counter holder. Each broker owns one instance; tests
 * and, eventually, the observability endpoint in Phase 9, read counters
 * from this bag. Metrics are append-only and thread-safe via
 * {@link LongAdder}.
 *
 * <p>P8.5 adds latency + throughput sampling so the admin {@code /metrics}
 * endpoints have something to show. A 1024-slot reservoir bounds memory; a
 * live counter (bytes/count) tracks all-time totals alongside a rolling
 * window for charts.
 */
public final class BrokerMetrics {

    /**
     * P7.11 — incremented once per {@code Fetch} RPC that arrives carrying a
     * non-zero {@code session_id} (i.e. a client echoing back a previously
     * allocated session). The first call per-session — where
     * {@code session_id == 0} — is <em>not</em> counted.
     */
    private final LongAdder incrementalFetchHits = new LongAdder();

    public void recordIncrementalFetchHit() {
        incrementalFetchHits.increment();
    }

    public long incrementalFetchHits() {
        return incrementalFetchHits.sum();
    }

    // ---- P8.5 — produce / fetch latency + throughput ----

    private final Reservoir produceLatency = new Reservoir(1024);
    private final Reservoir fetchLatency = new Reservoir(1024);
    private final LongAdder produceCount = new LongAdder();
    private final LongAdder produceBytes = new LongAdder();
    private final LongAdder fetchCount = new LongAdder();
    private final LongAdder fetchBytes = new LongAdder();
    /** First-produce timestamp (ms) so throughput = bytes / elapsed-s on snapshot. */
    private final AtomicLong firstSampleMillis = new AtomicLong(0L);

    public void recordProduce(long latencyNanos, long bytes) {
        long now = System.currentTimeMillis();
        firstSampleMillis.compareAndSet(0L, now);
        produceCount.increment();
        produceBytes.add(Math.max(0L, bytes));
        produceLatency.add(latencyNanos);
    }

    public void recordFetch(long latencyNanos, long bytes) {
        long now = System.currentTimeMillis();
        firstSampleMillis.compareAndSet(0L, now);
        fetchCount.increment();
        fetchBytes.add(Math.max(0L, bytes));
        fetchLatency.add(latencyNanos);
    }

    public LatencySnapshot produceLatencySnapshot() {
        return produceLatency.snapshot();
    }

    public LatencySnapshot fetchLatencySnapshot() {
        return fetchLatency.snapshot();
    }

    public ThroughputSnapshot throughputSnapshot() {
        long first = firstSampleMillis.get();
        long now = System.currentTimeMillis();
        double windowSeconds = first == 0L ? 0.0 : Math.max(1e-3, (now - first) / 1000.0);
        long pc = produceCount.sum();
        long pb = produceBytes.sum();
        long fc = fetchCount.sum();
        long fb = fetchBytes.sum();
        return new ThroughputSnapshot(
                windowSeconds,
                pc,
                pb,
                fc,
                fb,
                windowSeconds == 0.0 ? 0.0 : pb / windowSeconds,
                windowSeconds == 0.0 ? 0.0 : fb / windowSeconds);
    }

    /** p50/p99/p999 + count over the reservoir's last-N samples. */
    public record LatencySnapshot(long count, long p50Nanos, long p99Nanos, long p999Nanos) {}

    /** Rolling throughput since {@link #recordProduce} / {@link #recordFetch} first ran. */
    public record ThroughputSnapshot(
            double windowSeconds,
            long produceCount,
            long produceBytes,
            long fetchCount,
            long fetchBytes,
            double produceBytesPerSec,
            double fetchBytesPerSec) {}

    /**
     * Minimal lock-free-ish reservoir: monotonic insert position into a
     * fixed-size ring. Old entries are overwritten. Snapshot copies + sorts.
     * Not exact — for p99/p999 under low sample counts, the reservoir's
     * content bounds the precision — but adequate for admin-UI charts. A
     * proper HdrHistogram can replace this in Phase 9.
     */
    private static final class Reservoir {
        private final long[] samples;
        private final AtomicLong seq = new AtomicLong(0);

        Reservoir(int capacity) {
            this.samples = new long[capacity];
        }

        void add(long value) {
            long idx = seq.getAndIncrement();
            samples[(int) (idx % samples.length)] = value;
        }

        LatencySnapshot snapshot() {
            long total = seq.get();
            int n = (int) Math.min(total, samples.length);
            if (n == 0) return new LatencySnapshot(0, 0, 0, 0);
            long[] copy = Arrays.copyOf(samples, n);
            Arrays.sort(copy);
            return new LatencySnapshot(total, pct(copy, 0.50), pct(copy, 0.99), pct(copy, 0.999));
        }

        private static long pct(long[] sorted, double p) {
            if (sorted.length == 0) return 0L;
            int idx = (int) Math.ceil(p * sorted.length) - 1;
            if (idx < 0) idx = 0;
            if (idx >= sorted.length) idx = sorted.length - 1;
            return sorted[idx];
        }
    }
}
