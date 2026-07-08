package jbroker.admin.api;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import jbroker.proto.broker.DescribeMetricsResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Bounded in-memory history of cluster-wide throughput +
 * latency samples, populated by {@link MetricsScraper}'s fixed-rate
 * tick. Prior to this, the admin UI's sparklines only showed samples
 * collected while a browser tab was open; navigating away reset the
 * history to zero.
 *
 * <p>Cluster-wide aggregation here mirrors what {@link MetricsController}
 * does per-tick: produce/fetch bytes/sec are summed across brokers; p99
 * latency is taken as max-across-brokers (conservative). Per-broker
 * time-series would require a bigger UI redesign; summarised is enough
 * for the overview + metrics sparklines.
 *
 * <p>Thread-safety: single writer (the scraper's single-threaded tick),
 * many readers (one per REST request). Guarded by {@code this}. The
 * deque stays small enough (600 entries ≈ 50 min at 5s) that the
 * lock-held window is negligible.
 */
@Component
public class ThroughputHistory {

    /** One tuple captured per scrape tick. */
    public record Sample(
            long ts, double produceBytesPerSec, double fetchBytesPerSec, long produceP99Nanos, long fetchP99Nanos) {}

    private final int capacity;
    private final Deque<Sample> ring = new ArrayDeque<>();

    public ThroughputHistory(@Value("${jbroker.metrics.history.capacity:600}") int capacity) {
        if (capacity < 1) throw new IllegalArgumentException("capacity must be ≥ 1, got " + capacity);
        this.capacity = capacity;
    }

    /** Record one summary tuple derived from the current scraper snapshot. */
    public synchronized void append(MetricsScraper.Snapshot snapshot) {
        double produceBps = 0, fetchBps = 0;
        long produceP99 = 0, fetchP99 = 0;
        for (DescribeMetricsResponse r : snapshot.responses()) {
            produceBps += r.getProduceBytesPerSec();
            fetchBps += r.getFetchBytesPerSec();
            produceP99 = Math.max(produceP99, r.getProduceP99Nanos());
            fetchP99 = Math.max(fetchP99, r.getFetchP99Nanos());
        }
        var sample = new Sample(System.currentTimeMillis(), produceBps, fetchBps, produceP99, fetchP99);
        if (ring.size() >= capacity) ring.removeFirst();
        ring.addLast(sample);
    }

    /**
     * Returns every sample whose timestamp is within {@code window} of
     * now. Returned list is a snapshot — safe to iterate without holding
     * the lock.
     */
    public synchronized List<Sample> since(Duration window) {
        long cutoff = System.currentTimeMillis() - window.toMillis();
        var out = new ArrayList<Sample>(ring.size());
        for (var s : ring) {
            if (s.ts() >= cutoff) out.add(s);
        }
        return out;
    }

    /** All retained samples. Primarily for tests. */
    public synchronized List<Sample> all() {
        return new ArrayList<>(ring);
    }

    public int capacity() {
        return capacity;
    }
}
