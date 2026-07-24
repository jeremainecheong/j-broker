package jbroker.broker.replication;

import java.util.function.LongSupplier;

/**
 * Token-bucket byte-rate limiter for reassignment catch-up fetches. A fetcher
 * for an "adding" replica asks {@link #reserve} how many bytes it may pull on
 * the next fetch; the bucket refills at {@code bytesPerSec} and never lets the
 * sustained grant rate exceed that, so a newcomer catching up cannot saturate
 * the link and starve live produce/consume traffic.
 *
 * <p>A non-positive {@code bytesPerSec} disables throttling: {@link #reserve}
 * grants the full request every time. The bucket holds at most one second of
 * budget, so an idle reassignment cannot bank an unbounded burst.
 */
public final class ReassignmentThrottle {

    private final long bytesPerSec;
    private final LongSupplier nowNanos;
    private final double capacity;

    private double tokens;
    private long lastRefillNanos;

    public ReassignmentThrottle(long bytesPerSec, LongSupplier nowNanos) {
        this.bytesPerSec = bytesPerSec;
        this.nowNanos = nowNanos;
        this.capacity = Math.max(0, bytesPerSec);
        // Start empty: the first fetch waits for the bucket to refill, so the
        // rate is bounded from t=0 with no free opening burst.
        this.tokens = 0;
        this.lastRefillNanos = nowNanos.getAsLong();
    }

    /**
     * Grant up to {@code wanted} bytes for an immediate fetch, consuming the
     * granted amount from the bucket. Returns 0 when the bucket is empty (the
     * caller should skip this fetch and retry on its next tick). When
     * throttling is disabled, always returns {@code wanted}.
     */
    public synchronized int reserve(int wanted) {
        if (bytesPerSec <= 0) return wanted;
        if (wanted <= 0) return 0;
        refill();
        int grant = (int) Math.min(wanted, Math.floor(tokens));
        if (grant > 0) tokens -= grant;
        return grant;
    }

    private void refill() {
        long now = nowNanos.getAsLong();
        long elapsed = now - lastRefillNanos;
        if (elapsed <= 0) return;
        tokens = Math.min(capacity, tokens + bytesPerSec * (elapsed / 1_000_000_000.0));
        lastRefillNanos = now;
    }
}
