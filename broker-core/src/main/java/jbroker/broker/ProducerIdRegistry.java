package jbroker.broker;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Monotonic producer-id counter, advanced by the active controller on
 * {@code InitProducerId} and replicated via {@code ProducerIdAssignmentRecord}.
 *
 * <p>The counter is 0-based; each allocation returns the current value and
 * increments. Apply-side uses {@link #applyAssignment(long)} to bump the
 * counter to the observed {@code nextProducerId} — never regressing, so a
 * stale replay during snapshot restore or follower catch-up is a no-op.
 *
 * <p>In Phase 6.7 single-broker mode, the controller (== this broker) is
 * the only producer-id allocator, so the counter on the apply side and the
 * proposer side always match. Multi-broker Phase 6 will keep the counter
 * in the Raft state machine so a new active controller picks up where the
 * previous one left off.
 */
public final class ProducerIdRegistry {

    private final AtomicLong nextProducerId = new AtomicLong(0L);

    /**
     * Allocate the next producer id and advance the counter by one. Called
     * by the controller on the proposer side before writing a
     * {@code ProducerIdAssignmentRecord}.
     */
    public long allocateNext() {
        return nextProducerId.getAndIncrement();
    }

    /**
     * The id that would be returned by the next {@link #allocateNext}. Used
     * by the snapshot writer.
     */
    public long peekNextProducerId() {
        return nextProducerId.get();
    }

    /**
     * Advance the counter to {@code observedNext} if the current value is
     * lower; otherwise no-op. Called by the state machine on applying a
     * {@code ProducerIdAssignmentRecord} and by {@link #restore(long)} on
     * snapshot restore.
     */
    public void applyAssignment(long observedNext) {
        nextProducerId.accumulateAndGet(observedNext, Math::max);
    }

    /**
     * Reset the counter to {@code value}. Intended for tests; the apply and
     * snapshot paths go through {@link #applyAssignment}.
     */
    void resetForTest(long value) {
        nextProducerId.set(value);
    }
}
