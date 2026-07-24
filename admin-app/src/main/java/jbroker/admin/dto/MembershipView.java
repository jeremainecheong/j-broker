package jbroker.admin.dto;

import java.util.List;
import jbroker.proto.broker.DescribeMembershipResponse;

/**
 * REST/UI shape of {@code DescribeMembership}: the current voter set plus
 * the in-flight (or most recent) join and decommission. Phases are the
 * controller state names (e.g. {@code CATCHING_UP}, {@code DRAINING},
 * {@code DONE}, {@code FAILED}); {@code IDLE} when never started.
 */
public record MembershipView(List<Integer> voterIds, Join join, Decommission decommission) {

    public record Join(String phase, int brokerId, long lag) {
        /**
         * True while there is something for an operator to look at — an
         * in-flight phase or a failure. {@code IDLE} and {@code DONE} are
         * the quiet states the overview chips hide.
         */
        public boolean active() {
            return !phase.isEmpty() && !"IDLE".equals(phase) && !"DONE".equals(phase);
        }
    }

    public record Decommission(String phase, int brokerId, int remainingPartitions, String detail) {
        /** Same contract as {@link Join#active()}: hide IDLE and DONE, surface everything else. */
        public boolean active() {
            return !phase.isEmpty() && !"IDLE".equals(phase) && !"DONE".equals(phase);
        }
    }

    public static MembershipView of(DescribeMembershipResponse r) {
        return new MembershipView(
                r.getVoterIdsList(),
                new Join(r.getJoinPhase(), r.getJoinBrokerId(), r.getJoinLag()),
                new Decommission(
                        r.getDecommissionPhase(),
                        r.getDecommissionBrokerId(),
                        r.getDecommissionRemainingPartitions(),
                        r.getDecommissionDetail()));
    }
}
