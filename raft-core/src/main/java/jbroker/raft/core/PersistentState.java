package jbroker.raft.core;

import java.util.Optional;

/**
 * Persistent Raft state: currentTerm + votedFor. Implementations must fsync
 * before {@link #update} returns.
 */
public interface PersistentState {

    Term currentTerm();

    Optional<NodeId> votedFor();

    void update(Term term, Optional<NodeId> vote);
}
