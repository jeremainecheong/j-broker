package jbroker.sim;

import java.util.Optional;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.PersistentState;
import jbroker.raft.core.Term;

public final class InMemoryPersistentState implements PersistentState {
    private Term currentTerm = Term.ZERO;
    private Optional<NodeId> votedFor = Optional.empty();

    @Override
    public Term currentTerm() {
        return currentTerm;
    }

    @Override
    public Optional<NodeId> votedFor() {
        return votedFor;
    }

    @Override
    public void update(Term term, Optional<NodeId> vote) {
        this.currentTerm = term;
        this.votedFor = vote;
    }
}
