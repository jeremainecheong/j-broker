package jbroker.broker.controller;

import java.util.List;
import java.util.Set;
import jbroker.broker.DecommissionPlanner;
import jbroker.broker.PartitionAssignment;
import jbroker.raft.core.Membership;
import jbroker.raft.core.NodeId;

/**
 * Drives a broker decommission to completion: plan the drain with
 * {@link DecommissionPlanner}, start the per-partition reassignments, wait
 * until no partition hosts a replica on the leaving broker, then remove it
 * from the Raft voter set with a single-server membership change. Runs only
 * on the active controller; a leadership change abandons the in-flight
 * decommission (the new controller can be asked to retry — reassignments
 * already started keep draining on their own).
 *
 * <p>Pure orchestration over a minimal {@link ClusterAccess} view, no
 * threads, no clock — the broker layer calls {@link #runOnce()} on its
 * controller tick, mirroring {@link MembershipController}. One decommission
 * at a time.
 */
public final class DecommissionController {

    /** What the controller needs from the broker + Raft layers. */
    public interface ClusterAccess {
        boolean isLeader();

        /** The cluster's replicated partition assignments. */
        List<PartitionAssignment> assignments();

        /** Brokers eligible to receive drained replicas (registered and alive). */
        Set<Integer> liveBrokers();

        /** Rack label per broker id; brokers without one absent. Rack-blind by default. */
        default java.util.Map<Integer, String> racks() {
            return java.util.Map.of();
        }

        /** Whether a reassignment is already pending for the partition. */
        boolean reassignmentPending(String topic, int partition);

        /** Propose a reassignment of the partition to {@code target}. */
        boolean startReassignment(String topic, int partition, List<Integer> target) throws InterruptedException;

        List<NodeId> voters();

        void proposeMembership(Membership membership) throws InterruptedException;
    }

    /** Where a decommission is in its lifecycle. */
    public enum Phase {
        IDLE,
        DRAINING,
        REMOVING_VOTER,
        DONE,
        REFUSED,
        FAILED
    }

    /** Snapshot for progress reporting. {@code remaining} = partitions still hosting the broker. */
    public record Progress(Phase phase, int brokerId, int remaining, String detail) {}

    private final ClusterAccess cluster;

    private Phase phase = Phase.IDLE;
    private int target = -1;
    private int remaining;
    private String detail = "";

    public DecommissionController(ClusterAccess cluster) {
        this.cluster = cluster;
    }

    /**
     * Request that broker {@code brokerId} be decommissioned. Refused (with
     * the planner's reason readable via {@link #progress()}) when the drain
     * cannot preserve every partition's replication factor; rejected when a
     * decommission is already in flight or self is not the controller.
     */
    public synchronized boolean requestDecommission(int brokerId) {
        if (!cluster.isLeader()) return false;
        if (phase == Phase.DRAINING || phase == Phase.REMOVING_VOTER) return false;
        // Feasibility check over partitions not already mid-reassignment
        // (see runOnce for why an in-flight expand must not be re-planned).
        var hosting = 0;
        var unmanaged = new java.util.ArrayList<PartitionAssignment>();
        for (var a : cluster.assignments()) {
            if (!a.state().replicas().contains(brokerId)) continue;
            hosting++;
            if (!cluster.reassignmentPending(a.topic(), a.partition())) {
                unmanaged.add(a);
            }
        }
        var result = DecommissionPlanner.plan(brokerId, cluster.liveBrokers(), unmanaged, cluster.racks());
        if (result instanceof DecommissionPlanner.Result.Refused refused) {
            this.target = brokerId;
            this.detail = refused.reason();
            this.phase = Phase.REFUSED;
            return false;
        }
        this.target = brokerId;
        this.detail = "";
        this.remaining = hosting;
        this.phase = Phase.DRAINING;
        return true;
    }

    /** Advance the in-flight decommission by one step. Idempotent per phase; safe to call every tick. */
    public synchronized void runOnce() throws InterruptedException {
        if (phase != Phase.DRAINING && phase != Phase.REMOVING_VOTER) return;
        if (!cluster.isLeader()) {
            phase = Phase.FAILED;
            return;
        }
        switch (phase) {
            case DRAINING -> {
                // Re-plan against current state each tick: moves whose
                // reassignment never landed are retried, completed ones
                // disappear, and a broker that died mid-drain shifts the
                // replacement choice. Partitions with a reassignment already
                // pending MUST be excluded from the re-plan, not just from
                // the starts: mid-expand their replica set is the union —
                // it still contains the leaver and every candidate — and
                // planning over that state misreads a healthy drain as
                // "no replacement broker left".
                var hosting = new java.util.ArrayList<PartitionAssignment>();
                var unmanaged = new java.util.ArrayList<PartitionAssignment>();
                for (var a : cluster.assignments()) {
                    if (!a.state().replicas().contains(target)) continue;
                    hosting.add(a);
                    if (!cluster.reassignmentPending(a.topic(), a.partition())) {
                        unmanaged.add(a);
                    }
                }
                remaining = hosting.size();
                if (hosting.isEmpty()) {
                    phase = Phase.REMOVING_VOTER;
                    return;
                }
                if (unmanaged.isEmpty()) {
                    // Everything left is draining under the reassignment
                    // engine; nothing to start this tick.
                    return;
                }
                var result = DecommissionPlanner.plan(target, cluster.liveBrokers(), unmanaged, cluster.racks());
                if (result instanceof DecommissionPlanner.Result.Refused refused) {
                    // A candidate died mid-drain and no replacement exists
                    // any more — surface it rather than spinning silently.
                    detail = refused.reason();
                    phase = Phase.FAILED;
                    return;
                }
                for (var move : ((DecommissionPlanner.Result.Plan) result).moves()) {
                    cluster.startReassignment(move.topic(), move.partition(), move.target());
                }
            }
            case REMOVING_VOTER -> {
                var voters = cluster.voters();
                var leavingId = new NodeId(target);
                if (!voters.contains(leavingId)) {
                    remaining = 0;
                    phase = Phase.DONE;
                    return;
                }
                // Retry until the change commits (an in-flight config change
                // rejects the proposal; membership stays put — safe).
                cluster.proposeMembership(Membership.ofVoters(
                        voters.stream().filter(v -> !v.equals(leavingId)).toList()));
            }
            default -> {
                /* terminal */
            }
        }
    }

    public synchronized Progress progress() {
        return new Progress(phase, target, remaining, detail);
    }
}
