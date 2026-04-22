package jbroker.raft.core;

import java.util.List;
import java.util.Optional;

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

    /**
     * The leader this node currently recognises, if any — the {@code leaderId}
     * field Raft maintains on every follower as {@code AppendEntries} arrive
     * and on every leader as it starts a term. Empty during elections and on
     * freshly-started nodes that haven't heard from a leader yet.
     *
     * <p>Exposed for observability only. In particular, an admin REST layer
     * uses this to report {@code controllerId} for the spec {@code /cluster}
     * without round-tripping through the Raft protocol.
     */
    Optional<NodeId> currentLeader();

    /** point-in-time Raft-node state exposed by {@code /api/v1/raft}. */
    record Observability(
            long currentTerm, long commitIndex, long lastApplied, long lastLogIndex, long lastLogTerm, int votedFor) {}

    /**
     * Snapshot of the bookkeeping fields the admin REST layer surfaces.
     * Called on the driver's pump thread via {@link #step} already, so this
     * returning a consistent read under concurrent {@code step} calls is a
     * non-goal — callers accept "most-recent applied state" semantics.
     */
    Observability observability();
}
