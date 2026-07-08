package jbroker.broker.quota;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Single-broker in-memory token bucket. One bucket per
 * {@code (principal, op)} pair; bucket refills at {@code bytesPerSec}
 * and caps at one second of capacity.
 *
 * <p>Use this when {@code jbroker.quota.redis.url} is unset. The
 * {@link RedisQuotaEnforcer} variant shares counters across brokers so
 * per-principal caps work cluster-wide; the in-memory variant is
 * still useful for single-node dev or when Redis is unavailable.
 */
public final class InMemoryQuotaEnforcer implements QuotaEnforcer {

    private final long defaultProduceBytesPerSec;
    private final long defaultFetchBytesPerSec;
    private final ConcurrentHashMap<String, Bucket> buckets = new ConcurrentHashMap<>();

    public InMemoryQuotaEnforcer(long defaultProduceBytesPerSec, long defaultFetchBytesPerSec) {
        this.defaultProduceBytesPerSec = Math.max(1L, defaultProduceBytesPerSec);
        this.defaultFetchBytesPerSec = Math.max(1L, defaultFetchBytesPerSec);
    }

    @Override
    public Decision check(String principal, Op op, long bytes) {
        long rate = op == Op.PRODUCE ? defaultProduceBytesPerSec : defaultFetchBytesPerSec;
        String key = op + "::" + principal;
        var bucket = buckets.computeIfAbsent(key, k -> new Bucket(rate));
        long now = System.nanoTime();
        synchronized (bucket) {
            // Refill: add bytes proportional to elapsed time, capped at
            // one-second of quota so a long idle doesn't let a client
            // dump a huge burst.
            long elapsedNanos = now - bucket.lastNanos;
            if (elapsedNanos > 0) {
                double added = (elapsedNanos / 1_000_000_000.0) * rate;
                bucket.available = Math.min(rate, bucket.available + added);
                bucket.lastNanos = now;
            }
            if (bucket.available >= bytes) {
                bucket.available -= bytes;
                return Decision.allowed();
            }
            // Compute throttle: the time needed to refill enough tokens.
            double needed = bytes - bucket.available;
            long throttleMs = (long) Math.ceil((needed / rate) * 1000.0);
            return Decision.denied(rate, Math.max(1L, throttleMs));
        }
    }

    /** Visible for tests — resets all buckets. */
    public void reset() {
        buckets.clear();
    }

    private static final class Bucket {
        double available;
        long lastNanos;

        Bucket(long initial) {
            this.available = initial;
            this.lastNanos = System.nanoTime();
        }
    }

    /** Shared counter side-effect (reset + size) for tests. */
    int bucketCount() {
        return buckets.size();
    }

    /** Cheap helper used by {@link RedisQuotaEnforcer#local} when Redis is unreachable. */
    public static InMemoryQuotaEnforcer withDefaults() {
        return new InMemoryQuotaEnforcer(1L * 1024 * 1024, 10L * 1024 * 1024);
    }

    /** Expose atomics for potential future metrics. */
    public AtomicLong stub() {
        return new AtomicLong();
    }
}
