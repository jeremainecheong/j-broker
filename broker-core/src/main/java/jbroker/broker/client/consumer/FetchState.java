package jbroker.broker.client.consumer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import jbroker.proto.common.TopicPartition;

/**
 * Per-partition fetch bookkeeping for {@link Consumer}: the next position
 * to hand the application, the paused set, and records already fetched
 * but not yet returned by {@code poll}.
 *
 * <p>The position advances only when a record is actually returned to the
 * application ({@link #drain}), never at fetch time — so a position
 * snapshot taken for commit can never cover a record the application
 * hasn't seen. Records fetched past the {@code max.poll.records} bound
 * wait in the buffer, in order, for the next poll.
 *
 * <p>Package-private and single-threaded — guarded by the consumer's own
 * lock. Split out at this level so the pure bookkeeping is testable
 * without a broker.
 */
final class FetchState<K, V> {

    private final Map<TopicPartition, Long> positions = new HashMap<>();
    private final Set<TopicPartition> paused = new HashSet<>();
    // Insertion-ordered so a partition carrying surplus from the previous
    // poll drains ahead of freshly-fetched ones on the next.
    private final LinkedHashMap<TopicPartition, ArrayDeque<ConsumerRecord<K, V>>> buffered = new LinkedHashMap<>();

    boolean hasPosition(TopicPartition tp) {
        return positions.containsKey(tp);
    }

    /** Next offset to hand the application; 0 when never primed. */
    long position(TopicPartition tp) {
        return positions.getOrDefault(tp, 0L);
    }

    void position(TopicPartition tp, long offset) {
        positions.put(tp, offset);
    }

    void pause(TopicPartition tp) {
        paused.add(tp);
    }

    void resume(TopicPartition tp) {
        paused.remove(tp);
    }

    Set<TopicPartition> paused() {
        return Set.copyOf(paused);
    }

    /**
     * Whether a fetch should be issued for {@code tp} this tick: not
     * paused, and nothing already buffered — the fetch offset of a
     * buffered partition lags the records it holds, so fetching it again
     * would just re-download them.
     */
    boolean fetchable(TopicPartition tp) {
        return !paused.contains(tp) && !buffered.containsKey(tp);
    }

    void buffer(TopicPartition tp, List<ConsumerRecord<K, V>> records) {
        if (records.isEmpty()) return;
        buffered.computeIfAbsent(tp, k -> new ArrayDeque<>()).addAll(records);
    }

    int bufferedCount() {
        int total = 0;
        for (var q : buffered.values()) total += q.size();
        return total;
    }

    /**
     * Hand back up to {@code maxRecords} buffered records in per-partition
     * order, skipping paused partitions (their records stay buffered until
     * {@link #resume}), and advance each partition's position past what was
     * returned. Surplus stays buffered for the next call.
     */
    Map<TopicPartition, List<ConsumerRecord<K, V>>> drain(int maxRecords) {
        var out = new LinkedHashMap<TopicPartition, List<ConsumerRecord<K, V>>>();
        int remaining = maxRecords;
        var it = buffered.entrySet().iterator();
        while (it.hasNext() && remaining > 0) {
            var e = it.next();
            if (paused.contains(e.getKey())) continue;
            var q = e.getValue();
            var taken = new ArrayList<ConsumerRecord<K, V>>(Math.min(remaining, q.size()));
            while (remaining > 0 && !q.isEmpty()) {
                taken.add(q.poll());
                remaining--;
            }
            out.put(e.getKey(), taken);
            positions.put(e.getKey(), taken.get(taken.size() - 1).offset() + 1);
            if (q.isEmpty()) it.remove();
        }
        return out;
    }

    /** Forget everything about {@code tp} — revoked partitions leave no residue. */
    void forget(TopicPartition tp) {
        positions.remove(tp);
        paused.remove(tp);
        buffered.remove(tp);
    }
}
