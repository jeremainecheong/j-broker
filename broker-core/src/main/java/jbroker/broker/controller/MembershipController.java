package jbroker.broker.controller;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import jbroker.raft.core.Membership;
import jbroker.raft.core.NodeId;

/**
 * Drives a single-server broker join to completion (Raft dissertation
 * §4.2.1): propose the newcomer as a non-voting learner, wait for its log
 * to catch up, then promote it into the voter set. Runs only on the active
 * controller (the Raft leader); a leadership change abandons any in-flight
 * join (the new controller can be asked to retry).
 *
 * <p>Pure orchestration over a minimal {@link RaftMembershipAccess} view of
 * the Raft driver — no threads, no clock. The broker-app layer calls
 * {@link #runOnce()} on its controller tick. One join at a time: Raft
 * permits only one config change in flight, so a second request while a
 * join is active is refused.
 */
public final class MembershipController {

    /** What the controller needs from the Raft driver. */
    public interface RaftMembershipAccess {
        boolean isLeader();

        List<NodeId> voters();

        List<NodeId> learners();

        long leaderLastIndex();

        /** Matched index for a peer, empty if the leader tracks none yet. */
        OptionalLong matchIndex(NodeId peer);

        void proposeMembership(Membership membership) throws InterruptedException;
    }

    /** Where a join is in its lifecycle. */
    public enum Phase {
        IDLE,
        ADDING_LEARNER,
        CATCHING_UP,
        PROMOTING,
        DONE,
        FAILED
    }

    /** Snapshot of an in-flight (or finished) join, for progress reporting. */
    public record Progress(Phase phase, NodeId target, long lag) {}

    /**
     * How far behind the leader a learner may be and still be promoted.
     * A few entries of slack absorbs the entries that land between the
     * catch-up read and the promotion committing.
     */
    private static final long CATCH_UP_SLACK = 8L;

    private final RaftMembershipAccess raft;

    private Phase phase = Phase.IDLE;
    private NodeId target;
    private long lag;

    public MembershipController(RaftMembershipAccess raft) {
        this.raft = raft;
    }

    /**
     * Request that broker {@code id} join. Rejected if a join is already in
     * flight, if {@code id} is already a voter, or if self is not the
     * controller. The address/registration side is the caller's job; this
     * only drives the Raft membership sequence.
     */
    public synchronized boolean requestAddBroker(NodeId id) {
        if (!raft.isLeader()) return false;
        if (phase != Phase.IDLE && phase != Phase.DONE && phase != Phase.FAILED) return false;
        if (raft.voters().contains(id)) return false;
        this.target = id;
        this.lag = Long.MAX_VALUE;
        this.phase = Phase.ADDING_LEARNER;
        return true;
    }

    /** Advance the in-flight join by one step. Idempotent per phase; safe to call every tick. */
    public synchronized void runOnce() throws InterruptedException {
        if (phase == Phase.IDLE || phase == Phase.DONE || phase == Phase.FAILED) return;
        // Only the controller orchestrates; losing leadership abandons the join.
        if (!raft.isLeader()) {
            phase = Phase.FAILED;
            return;
        }
        switch (phase) {
            case ADDING_LEARNER -> {
                if (raft.learners().contains(target) || raft.voters().contains(target)) {
                    phase = Phase.CATCHING_UP;
                } else {
                    // Propose (or re-propose after an in-flight rejection).
                    var next = new Membership(raft.voters(), withLearner(raft.learners(), target));
                    raft.proposeMembership(next);
                }
            }
            case CATCHING_UP -> {
                long leaderIdx = raft.leaderLastIndex();
                long matched = raft.matchIndex(target).orElse(0L);
                lag = Math.max(0L, leaderIdx - matched);
                if (matched >= leaderIdx - CATCH_UP_SLACK) {
                    phase = Phase.PROMOTING;
                }
            }
            case PROMOTING -> {
                if (raft.voters().contains(target)) {
                    lag = 0L;
                    phase = Phase.DONE;
                } else {
                    // Retry until the add-learner change commits (a still-in-flight
                    // change is rejected; membership stays put — safe).
                    raft.proposeMembership(Membership.ofVoters(withVoter(raft.voters(), target)));
                }
            }
            default -> {
                /* terminal */
            }
        }
    }

    public synchronized Progress progress() {
        return new Progress(phase, target, lag);
    }

    public synchronized Optional<NodeId> inFlightTarget() {
        return phase == Phase.IDLE || phase == Phase.DONE || phase == Phase.FAILED
                ? Optional.empty()
                : Optional.of(target);
    }

    private static List<NodeId> withLearner(List<NodeId> learners, NodeId id) {
        if (learners.contains(id)) return learners;
        var out = new java.util.ArrayList<>(learners);
        out.add(id);
        return out;
    }

    private static List<NodeId> withVoter(List<NodeId> voters, NodeId id) {
        if (voters.contains(id)) return voters;
        var out = new java.util.ArrayList<>(voters);
        out.add(id);
        return out;
    }
}
