package jbroker.broker;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

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
 * <p>State is keyed by {@link DedupKey} and stores the highest-sequence batch
 * observed per key. A produce RPC is a duplicate iff its
 * {@code (baseSequence, recordCount)} matches the cached entry exactly; any
 * other sequence that isn't the contiguous next one is rejected as out-of-order.
 */
public final class ProducerStateManager {

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
     * Callback invoked under the per-key lock when a fresh append is needed.
     * Must perform the actual append and return either the new offsets (on
     * success) or an error string (which becomes the {@link Result#errorMessage}).
     */
    @FunctionalInterface
    public interface Appender {
        /**
         * @return {@code null} on failure (caller leaves cached state unchanged),
         *         or a {@code long[]{baseOffset, lastOffset}} on success.
         */
        long[] appendOrFail() throws Exception;
    }

    private final ConcurrentHashMap<DedupKey, DedupEntry> state = new ConcurrentHashMap<>();

    /**
     * Atomic dedup-or-append for the leader's produce path. Under the per-key
     * compute lock:
     * <ul>
     *   <li>If a cached entry matches {@code (baseSequence, recordCount)}
     *       exactly, returns the cached offsets without invoking {@code append}.</li>
     *   <li>If a cached entry exists but {@code baseSequence} isn't the
     *       contiguous next slot ({@code cached.lastBaseSequence + cached.recordCount}),
     *       returns {@link Result#error} with an out-of-order message.</li>
     *   <li>Otherwise invokes {@code append} and caches its result.</li>
     * </ul>
     */
    public Result dedupOrAppend(DedupKey key, int baseSequence, int recordCount, Appender append) {
        var holder = new Object() {
            Result result;
        };
        BiFunction<DedupKey, DedupEntry, DedupEntry> fn = (k, cached) -> {
            if (cached != null && cached.lastBaseSequence == baseSequence && cached.recordCount == recordCount) {
                holder.result = Result.fromCached(cached);
                return cached;
            }
            if (cached != null) {
                int expected = cached.lastBaseSequence + cached.recordCount;
                if (baseSequence != expected) {
                    holder.result = Result.error("expected base_sequence " + expected + ", got " + baseSequence);
                    return cached;
                }
            }
            long[] offsets;
            try {
                offsets = append.appendOrFail();
            } catch (Exception e) {
                holder.result = Result.error(e.getMessage() == null ? e.toString() : e.getMessage());
                return cached;
            }
            if (offsets == null) {
                holder.result = Result.error("append returned null");
                return cached;
            }
            holder.result = Result.fresh(offsets[0], offsets[1]);
            return new DedupEntry(baseSequence, recordCount, offsets[0], offsets[1]);
        };
        state.compute(key, fn);
        return holder.result;
    }

    /**
     * Observe an already-committed batch (follower's replica-fetch apply path,
     * or the leader side right after a non-idempotent append). Updates the
     * cached entry if this batch advances the sequence, ignores otherwise —
     * a stale re-observation (e.g., a fetch that overlaps a just-replayed
     * range) must not drag state backwards.
     */
    public void observeAppend(DedupKey key, int baseSequence, int recordCount, long baseOffset, long lastOffset) {
        state.compute(key, (k, cached) -> {
            if (cached != null && cached.lastBaseSequence >= baseSequence) {
                return cached;
            }
            return new DedupEntry(baseSequence, recordCount, baseOffset, lastOffset);
        });
    }

    /** Remove every entry belonging to a partition — topic-deletion path. */
    public void evictPartition(String topic, int partition) {
        state.keySet().removeIf(k -> k.topic().equals(topic) && k.partition() == partition);
    }

    /** Remove every entry belonging to {@code topic} across all partitions. Called on DeleteTopic. */
    public void evictTopic(String topic) {
        state.keySet().removeIf(k -> k.topic().equals(topic));
    }

    /** Test-only accessor. */
    public Optional<DedupEntry> get(DedupKey key) {
        return Optional.ofNullable(state.get(key));
    }

    public int size() {
        return state.size();
    }
}
