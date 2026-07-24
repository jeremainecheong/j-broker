package jbroker.broker.replication;

/**
 * Per-fetch byte budget for a follower, consulted by the fetcher pump before
 * each poll. The default grants the full ceiling to every partition; the
 * broker installs a reassignment-aware policy that meters an "adding" replica's
 * catch-up fetches through a {@link ReassignmentThrottle}.
 */
@FunctionalInterface
public interface FetchThrottle {

    /**
     * Bytes the fetcher for {@code (topic, partition)} may request on its next
     * poll. Returning 0 tells the pump to skip this tick (the throttle is
     * empty); it retries on the next one.
     */
    int maxBytes(String topic, int partition);

    /** No throttling: every fetch gets the full default ceiling. */
    FetchThrottle UNTHROTTLED = (topic, partition) -> ReplicaFetcher.DEFAULT_MAX_FETCH_BYTES;
}
