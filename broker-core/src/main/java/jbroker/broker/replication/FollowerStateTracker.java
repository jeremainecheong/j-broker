package jbroker.broker.replication;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-partition, per-follower {@code (LEO, lastFetchMillis)} record. The
 * leader updates this on every {@link jbroker.broker.ReplicaFetchHandler}
 * invocation; the high-watermark advance and ISR-lag checks read from it.
 *
 * <p>Kafka semantics: the follower's {@code fetch_offset} is the first
 * offset it still needs, i.e. its local log-end offset (LEO). HWM across an
 * ISR is {@code min(LEO)}; if any ISR member has never fetched, HWM stays
 * at 0 — we don't have proof that member has the data yet.
 *
 * <p>Thread-safe for concurrent fetch-handler writers + ISR-manager reader.
 */
public final class FollowerStateTracker {

    public record Match(long leo, long lastFetchMillis) {}

    private record Key(String topic, int partition, int brokerId) {}

    private final ConcurrentHashMap<Key, Match> state = new ConcurrentHashMap<>();

    public void record(String topic, int partition, int brokerId, long leo, long nowMillis) {
        state.put(new Key(topic, partition, brokerId), new Match(leo, nowMillis));
    }

    public Optional<Match> get(String topic, int partition, int brokerId) {
        return Optional.ofNullable(state.get(new Key(topic, partition, brokerId)));
    }

    /**
     * HWM = min(LEO across ISR). Leader's own LEO is passed in explicitly
     * rather than read from the map so we don't require the leader to
     * self-record. If any non-leader ISR member has no tracked LEO yet, HWM
     * is conservatively 0.
     */
    public long computeHwm(String topic, int partition, List<Integer> isr, int leaderBrokerId, long leaderLeo) {
        long min = leaderLeo;
        for (int broker : isr) {
            if (broker == leaderBrokerId) continue;
            var m = state.get(new Key(topic, partition, broker));
            if (m == null) return 0L;
            if (m.leo() < min) min = m.leo();
        }
        return min;
    }

    /**
     * Returns the subset of {@code candidates} whose last recorded fetch is
     * older than {@code nowMillis - lagTimeoutMs}, or who have never
     * fetched at all. The leader uses this to decide which followers to
     * shrink out of the ISR.
     */
    public List<Integer> laggardsOf(
            String topic, int partition, List<Integer> candidates, long nowMillis, long lagTimeoutMs) {
        var out = new ArrayList<Integer>();
        long cutoff = nowMillis - lagTimeoutMs;
        for (int broker : candidates) {
            var m = state.get(new Key(topic, partition, broker));
            if (m == null || m.lastFetchMillis() < cutoff) {
                out.add(broker);
            }
        }
        return out;
    }
}
