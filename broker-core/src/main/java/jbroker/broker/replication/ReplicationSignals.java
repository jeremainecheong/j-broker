package jbroker.broker.replication;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Per-(topic, partition) wakeup hub for replication-side waits. The
 * acks=all replication wait and the leader-side parked replica fetch both
 * park here instead of polling; signals come from the append path (new
 * data), the HWM recompute path (follower progress), and partition-change
 * applies (leadership loss and ISR shrink must wake waiters so they
 * re-run their exit checks).
 *
 * <p>Lost-wakeup safety: callers read {@link #stamp} BEFORE evaluating
 * their wait predicate and pass it to {@link #await} — a signal landing
 * between the two bumps the generation, so the await returns without
 * parking. Callers additionally slice their waits (≤100 ms), so even a
 * stale stamp degrades to a coarse poll, never a hang.
 *
 * <p>Parking is {@link Condition#awaitNanos} on a {@link ReentrantLock} —
 * LockSupport-based, interruptible, and safe on virtual threads (no
 * carrier pinning). Signals never run user code and never block: a bump
 * is a lock acquire, a generation increment, and a signalAll.
 */
public final class ReplicationSignals {

    private static final class Monitor {
        final ReentrantLock lock = new ReentrantLock();
        final Condition changed = lock.newCondition();
        long generation;
    }

    private record Key(String topic, int partition) {}

    // Monitors are never evicted; the map is bounded by the partitions
    // this broker has ever touched, same shape as the tracker's HWM floor.
    private final ConcurrentHashMap<Key, Monitor> monitors = new ConcurrentHashMap<>();
    private volatile boolean shutdown;

    private Monitor monitor(String topic, int partition) {
        return monitors.computeIfAbsent(new Key(topic, partition), k -> new Monitor());
    }

    /** Generation to read BEFORE evaluating a wait predicate. */
    public long stamp(String topic, int partition) {
        var m = monitor(topic, partition);
        m.lock.lock();
        try {
            return m.generation;
        } finally {
            m.lock.unlock();
        }
    }

    /** New data appended on the partition (leader append or follower apply). */
    public void signalAppend(String topic, int partition) {
        bump(monitor(topic, partition));
    }

    /** The partition's HWM floor advanced, or a partition-change record applied. */
    public void signalHwmOrState(String topic, int partition) {
        bump(monitor(topic, partition));
    }

    private static void bump(Monitor m) {
        m.lock.lock();
        try {
            m.generation++;
            m.changed.signalAll();
        } finally {
            m.lock.unlock();
        }
    }

    /**
     * Park until the partition's generation moves past {@code stamp}, up
     * to {@code nanos}. Returns immediately when a signal already landed
     * after the stamp was taken. Spurious returns are fine — every caller
     * re-evaluates its predicate on wakeup.
     */
    public void await(String topic, int partition, long stamp, long nanos) throws InterruptedException {
        if (nanos <= 0) return;
        var m = monitor(topic, partition);
        m.lock.lock();
        try {
            if (m.generation != stamp) return;
            m.changed.awaitNanos(nanos);
        } finally {
            m.lock.unlock();
        }
    }

    /**
     * Broker-shutdown wake: releases every parked waiter once so long-poll
     * loops can observe {@link #isShutdown()} and stop re-parking. Awaits
     * after shutdown still park normally — waiters that must run out their
     * own deadlines (the acks=all wait) keep today's exit behavior instead
     * of degrading into a busy spin.
     */
    public void shutdown() {
        shutdown = true;
        for (var m : monitors.values()) {
            bump(m);
        }
    }

    public boolean isShutdown() {
        return shutdown;
    }
}
