package jbroker.raft.core;

/**
 * Abstract source of monotonic time. Production uses {@link MonotonicClock};
 * the deterministic simulator (Milestone 3) will substitute a virtual clock.
 */
public interface Clock {
    long nanoTime();
}
