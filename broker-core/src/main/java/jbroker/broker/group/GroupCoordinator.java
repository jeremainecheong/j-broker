package jbroker.broker.group;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import jbroker.proto.common.TopicPartition;

/**
 * In-memory consumer-group state for one or more groups whose coordinator
 * partition this broker leads. Members join via {@link #join}, send periodic
 * {@link #heartbeat} pings to prove liveness and accept new assignments,
 * and {@link #leave} (or get evicted via {@link #tickEvictions}) when their
 * session times out.
 *
 * <p>Per-group state machine:
 * <pre>
 *   join (member_id="" or member_epoch=0)
 *       → assign new memberId, bump generation, run assignor, return assignment
 *   heartbeat (steady)
 *       → if assignment changed since last fire, return new assignment + bumped epoch
 *       → else echo current state
 *   leave (member_epoch=-1)
 *       → drop member, bump generation, re-run assignor for survivors
 *   tickEvictions(nowNs)
 *       → drop any member with no heartbeat in &gt; sessionTimeoutMs, bump generation
 * </pre>
 *
 * <p>Cooperative incremental rebalance is staged in returns the
 * full new assignment immediately on every membership change, which is the
 * "stop-the-world" fallback the spec calls out. adds the kept-set
 * intermediate step.
 *
 * <p>{@link #join} returns a {@code memberEpoch} that the client must echo
 * on its next heartbeat. Stale epochs surface via
 * {@link HeartbeatOutcome#FENCED_MEMBER_EPOCH}; unknown ids surface via
 * {@link HeartbeatOutcome#UNKNOWN_MEMBER_ID}. The handler in
 * {@link jbroker.broker.ConsumerHandler} translates those to the matching
 * proto error codes.
 */
public final class GroupCoordinator {

    /** Default heartbeat interval (ms) the coordinator advertises in responses. */
    public static final int HEARTBEAT_INTERVAL_MS = 5_000;

    /** Default session timeout (ms) — members with no heartbeat in this window get evicted. */
    public static final long DEFAULT_SESSION_TIMEOUT_MS = 45_000L;

    public enum HeartbeatOutcome {
        OK,
        UNKNOWN_MEMBER_ID,
        FENCED_MEMBER_EPOCH
    }

    public record JoinResult(
            String memberId,
            int memberEpoch,
            int generation,
            List<TopicPartition> assignment,
            int heartbeatIntervalMs) {
        public JoinResult {
            assignment = List.copyOf(assignment);
        }
    }

    public record HeartbeatResult(
            HeartbeatOutcome outcome,
            int memberEpoch,
            Optional<List<TopicPartition>> newAssignment,
            int heartbeatIntervalMs) {
        public HeartbeatResult {
            newAssignment = newAssignment.map(List::copyOf);
        }
    }

    public record EvictedMember(String groupId, String memberId, String instanceId) {}

    /**
     * Captures the per-partition counts a topic exposes to assignors.
     * Sourced live from {@link jbroker.broker.TopicManager} via the supplier
     * the constructor receives; stored as a function so the coordinator
     * doesn't need a hard dependency on TopicManager (eases testing).
     */
    public interface PartitionCountSource {
        int partitionsFor(String topic);
    }

    private static final class MemberState {
        final String memberId;
        final String instanceId;
        Set<String> subscribedTopics;
        int memberEpoch;
        long lastHeartbeatNs;
        long sessionTimeoutNs;
        int rebalanceTimeoutMs;
        List<TopicPartition> currentAssignment;

        MemberState(
                String memberId,
                String instanceId,
                Set<String> subscribedTopics,
                int memberEpoch,
                long lastHeartbeatNs,
                long sessionTimeoutNs,
                int rebalanceTimeoutMs,
                List<TopicPartition> currentAssignment) {
            this.memberId = memberId;
            this.instanceId = instanceId;
            this.subscribedTopics = Set.copyOf(subscribedTopics);
            this.memberEpoch = memberEpoch;
            this.lastHeartbeatNs = lastHeartbeatNs;
            this.sessionTimeoutNs = sessionTimeoutNs;
            this.rebalanceTimeoutMs = rebalanceTimeoutMs;
            this.currentAssignment = List.copyOf(currentAssignment);
        }
    }

    private static final class GroupState {
        final String groupId;
        int generation;
        // Insertion-order preserving so iteration is deterministic.
        final Map<String, MemberState> members = new HashMap<>();
        // For static membership lookups (instance_id → member_id).
        final Map<String, String> instanceIndex = new HashMap<>();
        final ReentrantLock lock = new ReentrantLock();

        GroupState(String groupId) {
            this.groupId = groupId;
        }
    }

    private final Map<String, GroupState> groups = new HashMap<>();
    private final ReentrantLock groupsLock = new ReentrantLock();
    private final PartitionCountSource partitionSource;
    private final PartitionAssignor assignor;
    private final Function<String, String> memberIdGenerator;

    public GroupCoordinator(PartitionCountSource partitionSource, PartitionAssignor assignor) {
        this(partitionSource, assignor, instanceId -> UUID.randomUUID().toString());
    }

    /**
     * Test-only constructor: replaces the random UUID generator so unit
     * tests can predict the assigned member ids.
     */
    public GroupCoordinator(
            PartitionCountSource partitionSource,
            PartitionAssignor assignor,
            Function<String, String> memberIdGenerator) {
        this.partitionSource = partitionSource;
        this.assignor = assignor;
        this.memberIdGenerator = memberIdGenerator;
    }

    public JoinResult join(
            String groupId,
            String instanceId,
            Set<String> subscribedTopics,
            long sessionTimeoutMs,
            int rebalanceTimeoutMs,
            long nowNs) {
        var group = groupOf(groupId);
        group.lock.lock();
        try {
            // Static membership: existing instance_id reuses its slot.
            if (instanceId != null && !instanceId.isEmpty() && group.instanceIndex.containsKey(instanceId)) {
                var existingMemberId = group.instanceIndex.get(instanceId);
                var existing = group.members.get(existingMemberId);
                if (existing != null) {
                    existing.subscribedTopics = Set.copyOf(subscribedTopics);
                    existing.lastHeartbeatNs = nowNs;
                    existing.sessionTimeoutNs = sessionTimeoutMs * 1_000_000L;
                    existing.rebalanceTimeoutMs = rebalanceTimeoutMs;
                    // Static-rejoin keeps the same memberEpoch + assignment;
                    // no generation bump (subscription change handling lands
                    // in incremental rebalance).
                    return new JoinResult(
                            existing.memberId,
                            existing.memberEpoch,
                            group.generation,
                            existing.currentAssignment,
                            HEARTBEAT_INTERVAL_MS);
                }
            }

            // Fresh join: allocate id, add member, bump generation, run assignor.
            var newId = memberIdGenerator.apply(instanceId);
            var member = new MemberState(
                    newId,
                    instanceId == null ? "" : instanceId,
                    subscribedTopics,
                    /*memberEpoch*/ 0,
                    nowNs,
                    sessionTimeoutMs * 1_000_000L,
                    rebalanceTimeoutMs,
                    List.of());
            group.members.put(newId, member);
            if (instanceId != null && !instanceId.isEmpty()) {
                group.instanceIndex.put(instanceId, newId);
            }
            group.generation++;
            recomputeAssignment(group);
            return new JoinResult(
                    newId, member.memberEpoch, group.generation, member.currentAssignment, HEARTBEAT_INTERVAL_MS);
        } finally {
            group.lock.unlock();
        }
    }

    public HeartbeatResult heartbeat(
            String groupId, String memberId, int memberEpoch, List<TopicPartition> ownedPartitions, long nowNs) {
        var group = groupOf(groupId);
        group.lock.lock();
        try {
            var member = group.members.get(memberId);
            if (member == null) {
                return new HeartbeatResult(
                        HeartbeatOutcome.UNKNOWN_MEMBER_ID, 0, Optional.empty(), HEARTBEAT_INTERVAL_MS);
            }
            // Member sent a future epoch — means this coordinator restarted
            // and lost some state, or there's a misbehaving client. Force a
            // rejoin so state realigns.
            if (memberEpoch > member.memberEpoch) {
                return new HeartbeatResult(
                        HeartbeatOutcome.FENCED_MEMBER_EPOCH,
                        member.memberEpoch,
                        Optional.empty(),
                        HEARTBEAT_INTERVAL_MS);
            }
            member.lastHeartbeatNs = nowNs;
            if (memberEpoch < member.memberEpoch) {
                // Member is behind — coordinator recomputed assignment since
                // its last heartbeat. Surface the new assignment + bumped
                // epoch so the client catches up without a full rejoin.
                return new HeartbeatResult(
                        HeartbeatOutcome.OK,
                        member.memberEpoch,
                        Optional.of(member.currentAssignment),
                        HEARTBEAT_INTERVAL_MS);
            }
            // Steady-state: epochs match → no change since last heartbeat.
            return new HeartbeatResult(
                    HeartbeatOutcome.OK, member.memberEpoch, Optional.empty(), HEARTBEAT_INTERVAL_MS);
        } finally {
            group.lock.unlock();
        }
    }

    public void leave(String groupId, String memberId) {
        var group = groupOf(groupId);
        group.lock.lock();
        try {
            var removed = group.members.remove(memberId);
            if (removed == null) return;
            if (!removed.instanceId.isEmpty()) {
                group.instanceIndex.remove(removed.instanceId);
            }
            group.generation++;
            recomputeAssignment(group);
        } finally {
            group.lock.unlock();
        }
    }

    public List<EvictedMember> tickEvictions(long nowNs) {
        var evicted = new ArrayList<EvictedMember>();
        groupsLock.lock();
        var snapshot = new ArrayList<>(groups.values());
        groupsLock.unlock();
        for (var group : snapshot) {
            group.lock.lock();
            try {
                var toEvict = new ArrayList<MemberState>();
                for (var m : group.members.values()) {
                    if (nowNs - m.lastHeartbeatNs > m.sessionTimeoutNs) {
                        toEvict.add(m);
                    }
                }
                if (toEvict.isEmpty()) continue;
                for (var m : toEvict) {
                    group.members.remove(m.memberId);
                    if (!m.instanceId.isEmpty()) {
                        group.instanceIndex.remove(m.instanceId);
                    }
                    evicted.add(new EvictedMember(group.groupId, m.memberId, m.instanceId));
                }
                group.generation++;
                recomputeAssignment(group);
            } finally {
                group.lock.unlock();
            }
        }
        return List.copyOf(evicted);
    }

    /**
     * Test/debug accessor — returns the live assignment for a member, or
     * empty if the member doesn't exist.
     */
    public Optional<List<TopicPartition>> assignmentFor(String groupId, String memberId) {
        var group = groupOf(groupId);
        group.lock.lock();
        try {
            var m = group.members.get(memberId);
            return m == null ? Optional.empty() : Optional.of(m.currentAssignment);
        } finally {
            group.lock.unlock();
        }
    }

    /** Test/debug accessor — current generation for the group, or -1 if unknown. */
    public int generationOf(String groupId) {
        groupsLock.lock();
        try {
            var g = groups.get(groupId);
            return g == null ? -1 : g.generation;
        } finally {
            groupsLock.unlock();
        }
    }

    /** Test/debug accessor — number of members in the group. */
    public int memberCountOf(String groupId) {
        var g = groupOf(groupId);
        g.lock.lock();
        try {
            return g.members.size();
        } finally {
            g.lock.unlock();
        }
    }

    private GroupState groupOf(String groupId) {
        groupsLock.lock();
        try {
            return groups.computeIfAbsent(groupId, GroupState::new);
        } finally {
            groupsLock.unlock();
        }
    }

    private void recomputeAssignment(GroupState group) {
        if (group.members.isEmpty()) return;
        var sortedIds = new ArrayList<>(group.members.keySet());
        java.util.Collections.sort(sortedIds);
        var subscriptions = new HashMap<String, Set<String>>();
        var allTopics = new HashSet<String>();
        for (var m : group.members.values()) {
            subscriptions.put(m.memberId, m.subscribedTopics);
            allTopics.addAll(m.subscribedTopics);
        }
        var partitionsByTopic = new HashMap<String, Integer>();
        for (var t : allTopics) {
            partitionsByTopic.put(t, partitionSource.partitionsFor(t));
        }
        var assignment = assignor.assign(sortedIds, subscriptions, partitionsByTopic);
        for (var m : group.members.values()) {
            var newSet = assignment.getOrDefault(m.memberId, List.of());
            if (!sameAssignment(m.currentAssignment, newSet)) {
                m.currentAssignment = List.copyOf(newSet);
                m.memberEpoch++;
            }
        }
    }

    private static boolean sameAssignment(Collection<TopicPartition> a, Collection<TopicPartition> b) {
        if (a.size() != b.size()) return false;
        var as = new HashSet<>(a);
        var bs = new HashSet<>(b);
        return as.equals(bs);
    }
}
