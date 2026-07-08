package jbroker.raft.core;

/**
 * Abstract source of monotonic time. Production uses {@link MonotonicClock};
 * a future deterministic simulator will substitute a virtual clock.
 */
public interface Clock {
    long nanoTime();
}
