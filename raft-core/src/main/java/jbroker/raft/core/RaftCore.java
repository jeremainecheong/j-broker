package jbroker.raft.core;

import java.util.List;

/**
 * Pure Raft state machine. {@link #step} is deterministic: given the same
 * sequence of inputs it always produces the same sequence of effects.
 *
 * <p>The core performs no IO, owns no threads, and does not observe the
 * system clock. The driver feeds it {@link RaftEvent.Tick} events using
 * whatever clock it wishes.
 */
public interface RaftCore {

    /**
     * Applies an event and returns the effects the driver must execute.
     * Effects returned from one call must be fully executed before the
     * next event is delivered (otherwise replies that depend on persisted
     * state may race).
     */
    List<RaftEffect> step(RaftEvent event);

    /** Current role, exposed for observability only (not for control flow). */
    Role role();

    /** Current term, exposed for observability only. */
    Term currentTerm();
}
