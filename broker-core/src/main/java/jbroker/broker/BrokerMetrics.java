package jbroker.broker;

import java.util.concurrent.atomic.LongAdder;

/**
 * Minimal broker-local counter holder. Each broker owns one instance; tests
 * and, eventually, the observability endpoint in Milestone 9, read counters
 * from this bag. Metrics are append-only and thread-safe via
 * {@link LongAdder}.
 */
public final class BrokerMetrics {

    /**
     * incremented once per {@code Fetch} RPC that arrives carrying a
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
}
