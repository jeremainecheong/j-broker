package jbroker.broker;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-broker idempotent-producer dedup state. Shared between the leader's
 * {@code ProduceHandler} (which both reads the state to detect retries and
 * writes it after a successful append) and the follower's {@code ReplicaFetcher}
 * (which observes applied batches and updates the state accordingly).
 *
 * <p>Audit-finding #1: before this class, dedup lived in a {@code ProduceHandler}
 * private field, populated only on the leader's produce path. Leader failover
 * handed the new leader an empty map, so a producer retry of
 * {@code (pid, epoch, baseSequence)} after failover re-appended the same
 * records as a fresh batch — violating idempotency. Moving the state here and
 * having every broker track its own applied-batch history means the new leader
 * (an ex-follower) already has the dedup entries it needs on takeover.
 *
 * <p>A hardening pass found that the previous {@code ConcurrentHashMap} had no eviction
 * policy — a long-running broker with many transient producers would leak
 * memory indefinitely (unbounded growth was the original flag). This class now
 * caps at {@link #DEFAULT_MAX_ENTRIES} and evicts least-recently-used keys
 * when the cap is exceeded. The {@link #evictionCount} gauge is exposed so
 * operators can alert on sustained eviction (indicates the cap is too low
 * for the producer churn).
 *
 * <p>State is keyed by {@link DedupKey} and stores the highest-sequence batch
 * observed per key. A produce RPC is a duplicate iff its
 * {@code (baseSequence, recordCount)} matches the cached entry exactly; any
 * other sequence that isn't the contiguous next one is rejected as out-of-order.
 */
public final class ProducerStateManager {

    /**
     * Default cap. 100k entries means up to 100k concurrent idempotent
     * producers per broker before older ones start being evicted —
     * comfortable headroom for any real deployment without leaking unbounded
     * memory. Override via the 1-arg constructor for tests that want
     * deterministic eviction behavior.
     */
    public static final int DEFAULT_MAX_ENTRIES = 100_000;

    public record DedupKey(String topic, int partition, long producerId, int producerEpoch) {}

    public record DedupEntry(int lastBaseSequence, int recordCount, long baseOffset, long lastOffset) {}

    /**
     * Outcome of {@link #dedupOrAppend} — carries either the cached response
     * for a duplicate retry, or the fresh-append result supplied by the caller.
     */
    public record Result(long baseOffset, long lastOffset, boolean cached, String errorMessage) {
        public static Result fromCached(DedupEntry entry) {
            return new Result(entry.baseOffset, entry.lastOffset, true, null);
        }

        public static Result fresh(long baseOffset, long lastOffset) {
            return new Result(baseOffset, lastOffset, false, null);
        }

        public static Result error(String message) {
            return new Result(-1L, -1L, false, message);
        }

        public boolean hasError() {
            return errorMessage != null;
        }
    }

    /**
     * Callback invoked under the dedup lock when a fresh append is needed.
     * Must perform the actual append and return either the new offsets (on
     * success) or null (which becomes a {@link Result#error}).
     */
    @FunctionalInterface
    public interface Appender {
        long[] appendOrFail() throws Exception;
    }

    private final Object lock = new Object();
    private final LinkedHashMap<DedupKey, DedupEntry> state;
    private final int maxEntries;
    private final AtomicLong evictionCount = new AtomicLong();

    public ProducerStateManager() {
        this(DEFAULT_MAX_ENTRIES);
    }

    /** Test-only / capacity-customised constructor. */
    public ProducerStateManager(int maxEntries) {
        if (maxEntries < 1) {
            throw new IllegalArgumentException("maxEntries must be ≥ 1, got " + maxEntries);
        }
        this.maxEntries = maxEntries;
        this.state = new LinkedHashMap<>(Math.min(maxEntries + 16, 1 << 14), 0.75f, /*accessOrder*/ true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<DedupKey, DedupEntry> eldest) {
                boolean remove = size() > ProducerStateManager.this.maxEntries;
                if (remove) evictionCount.incrementAndGet();
                return remove;
            }
        };
    }

    /**
     * Atomic dedup-or-append for the leader's produce path. Under the dedup
     * lock:
     * <ul>
     *   <li>If a cached entry matches {@code (baseSequence, recordCount)}
     *       exactly, returns the cached offsets without invoking {@code append}.</li>
     *   <li>If a cached entry exists but {@code baseSequence} isn't the
     *       contiguous next slot ({@code cached.lastBaseSequence + cached.recordCount}),
     *       returns {@link Result#error} with an out-of-order message.</li>
     *   <li>Otherwise invokes {@code append} and caches its result.</li>
     * </ul>
     *
     * <p>{@code get()} touches the LinkedHashMap in access-order so
     * frequently-retried producers stay hot and aren't evicted prematurely.
     */
    public Result dedupOrAppend(DedupKey key, int baseSequence, int recordCount, Appender append) {
        synchronized (lock) {
            DedupEntry cached = state.get(key);
            if (cached != null && cached.lastBaseSequence == baseSequence && cached.recordCount == recordCount) {
                return Result.fromCached(cached);
            }
            if (cached != null) {
                int expected = cached.lastBaseSequence + cached.recordCount;
                if (baseSequence != expected) {
                    return Result.error("expected base_sequence " + expected + ", got " + baseSequence);
                }
            }
            long[] offsets;
            try {
                offsets = append.appendOrFail();
            } catch (Exception e) {
                return Result.error(e.getMessage() == null ? e.toString() : e.getMessage());
            }
            if (offsets == null) {
                return Result.error("append returned null");
            }
            state.put(key, new DedupEntry(baseSequence, recordCount, offsets[0], offsets[1]));
            return Result.fresh(offsets[0], offsets[1]);
        }
    }

    /**
     * Observe an already-committed batch (follower's replica-fetch apply path,
     * or the leader side right after a non-idempotent append). Updates the
     * cached entry if this batch advances the sequence, ignores otherwise —
     * a stale re-observation (e.g., a fetch that overlaps a just-replayed
     * range) must not drag state backwards.
     */
    public void observeAppend(DedupKey key, int baseSequence, int recordCount, long baseOffset, long lastOffset) {
        synchronized (lock) {
            DedupEntry cached = state.get(key);
            if (cached != null && cached.lastBaseSequence >= baseSequence) {
                return;
            }
            state.put(key, new DedupEntry(baseSequence, recordCount, baseOffset, lastOffset));
        }
    }

    /** Remove every entry belonging to a partition — topic-deletion path. */
    public void evictPartition(String topic, int partition) {
        synchronized (lock) {
            state.keySet().removeIf(k -> k.topic().equals(topic) && k.partition() == partition);
        }
    }

    /** Remove every entry belonging to {@code topic} across all partitions. Called on DeleteTopic. */
    public void evictTopic(String topic) {
        synchronized (lock) {
            state.keySet().removeIf(k -> k.topic().equals(topic));
        }
    }

    /** Test-only accessor. */
    public Optional<DedupEntry> get(DedupKey key) {
        synchronized (lock) {
            return Optional.ofNullable(state.get(key));
        }
    }

    public int size() {
        synchronized (lock) {
            return state.size();
        }
    }

    /** Number of entries evicted due to the LRU cap since startup. */
    public long evictionCount() {
        return evictionCount.get();
    }

    /** Configured maximum number of entries before LRU eviction kicks in. */
    public int maxEntries() {
        return maxEntries;
    }
}
