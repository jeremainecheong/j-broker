package jbroker.raft.core;

import java.util.List;

/**
 * A Raft cluster's membership: the voting set plus any non-voting
 * learners. Learners receive replicated entries and catch up to the
 * leader's log, but do not count toward {@code quorum()}, are never sent
 * or granted votes, and cannot become candidates. Adding a new server as
 * a learner first — then promoting it once caught up — keeps a config
 * change from shrinking availability while the newcomer's log is empty
 * (Raft dissertation §4.2.1, the catch-up phase).
 *
 * @param voters   the voting set; quorum is {@code voters.size()/2 + 1}
 * @param learners non-voting members being caught up
 */
public record Membership(List<NodeId> voters, List<NodeId> learners) {

    public Membership {
        voters = List.copyOf(voters);
        learners = List.copyOf(learners);
        for (var l : learners) {
            if (voters.contains(l)) {
                throw new IllegalArgumentException("node " + l + " cannot be both a voter and a learner");
            }
        }
    }

    /** Voters only — the historical shape, no learners. */
    public static Membership ofVoters(List<NodeId> voters) {
        return new Membership(voters, List.of());
    }

    /** Every member that receives replication: voters followed by learners. */
    public List<NodeId> allMembers() {
        var out = new java.util.ArrayList<NodeId>(voters.size() + learners.size());
        out.addAll(voters);
        out.addAll(learners);
        return List.copyOf(out);
    }

    public boolean isLearner(NodeId id) {
        return learners.contains(id);
    }

    public boolean isVoter(NodeId id) {
        return voters.contains(id);
    }
}
