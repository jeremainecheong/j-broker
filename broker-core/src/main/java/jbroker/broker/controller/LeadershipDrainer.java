package jbroker.broker.controller;

import java.util.ArrayList;
import java.util.List;
import jbroker.broker.TopicManager;

/**
 * Controlled-shutdown decision engine: for every partition this broker
 * leads, pick the replica that should take over before the process exits.
 * The preferred replica ({@code replicas[0]}) wins when it is another
 * in-sync member — handing off to it means the later preferred-leader
 * rebalance has nothing to move — otherwise the first other ISR member
 * takes it. Partitions whose ISR holds only this broker are skipped: a
 * drain must not promote an out-of-sync replica (that is exactly the
 * shorter-log promotion ISR-only election forbids); those partitions go
 * offline on exit and recover when the broker returns.
 *
 * <p>Pure decision engine, same shape as {@link PreferredLeaderBalancer}:
 * the broker-app layer turns proposals into CAS-guarded
 * {@code PartitionChangeRecord}s and proposes them through Raft (follower
 * proposals forward to the leader).
 */
public final class LeadershipDrainer {

    /** Proposal to move one partition's leadership off the draining broker. */
    public record Proposal(String topic, int partition, int newLeader, int newLeaderEpoch) {}

    private LeadershipDrainer() {}

    /** Compute handoff proposals for every partition currently led by {@code selfBrokerId}. */
    public static List<Proposal> proposeDrain(TopicManager topicManager, int selfBrokerId) {
        var proposals = new ArrayList<Proposal>();
        for (var pa : topicManager.allPartitionAssignments()) {
            var s = pa.state();
            if (s.leader() != selfBrokerId) continue;
            int target = -1;
            if (!s.replicas().isEmpty()) {
                int preferred = s.replicas().get(0);
                if (preferred != selfBrokerId && s.isr().contains(preferred)) {
                    target = preferred;
                }
            }
            if (target < 0) {
                for (int member : s.isr()) {
                    if (member != selfBrokerId) {
                        target = member;
                        break;
                    }
                }
            }
            if (target < 0) continue; // sole ISR member — offline on exit, not a bad promotion
            proposals.add(new Proposal(pa.topic(), pa.partition(), target, s.leaderEpoch() + 1));
        }
        return proposals;
    }
}
