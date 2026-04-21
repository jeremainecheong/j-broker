package jbroker.raft.core;

public enum Role {
    FOLLOWER,
    /**
     * Pre-vote phase (Ongaro thesis §9.6): soliciting a hypothetical vote at
     * term+1 without incrementing {@code currentTerm} or persisting a
     * {@code votedFor}. If a majority grants, the node transitions to
     * {@link #CANDIDATE} and starts a real election.
     */
    PRE_CANDIDATE,
    CANDIDATE,
    LEADER,
}
