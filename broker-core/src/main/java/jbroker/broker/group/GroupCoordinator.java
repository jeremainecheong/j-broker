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
 * <p>Cooperative incremental rebalance is staged in P7.7 — P7.4 returns the
 * full new assignment immediately on every membership change, which is the
 * "stop-the-world" fallback the spec calls out. P7.7 adds the kept-set
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
        // What the member currently owns (and what the coordinator has
        // already advertised). This may be a strict subset of the eventual
        // target during cooperative incremental rebalance — see pendingTarget.
        List<TopicPartition> currentAssignment;
        // P7.7 — non-null while a cooperative rebalance is mid-flight.
        // currentAssignment carries the "kept" set during stage 1; once the
        // member confirms it has revoked the lost partitions (its next
        // heartbeat carries owned_partitions == currentAssignment), the
        // coordinator advances currentAssignment to pendingTarget (stage 2)
        // and clears pendingTarget.
        List<TopicPartition> pendingTarget;

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
            this.pendingTarget = null;
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

    /**
     * Notified after every membership change to a group so the caller can
     * persist a Type-2 GroupMetadataValue record into
     * {@code __consumer_offsets}. Caller is expected to encode the
     * snapshot via
     * {@link jbroker.broker.ConsumerOffsetsTopic#valueForGroupMetadata}
     * and append to the appropriate partition log.
     *
     * <p>Fires on join (new member, dynamic or static), leave, and
     * eviction. Steady-state heartbeats and stage advancement do NOT
     * fire — only structural changes to membership do, which keeps the
     * write rate bounded and predictable.
     */
    @FunctionalInterface
    public interface SnapshotListener {
        void onGroupSnapshot(String groupId, jbroker.broker.ConsumerOffsetsTopic.GroupMetadataValue snapshot);
    }

    private final Map<String, GroupState> groups = new HashMap<>();
    private final ReentrantLock groupsLock = new ReentrantLock();
    private final PartitionCountSource partitionSource;
    private final PartitionAssignor assignor;
    private final Function<String, String> memberIdGenerator;
    private volatile SnapshotListener snapshotListener;

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
        this.snapshotListener = (g, s) -> {};
    }

    /**
     * Plug in a snapshot listener. Typically called from {@code Broker.start}
     * once the {@link jbroker.storage.LogManager} and topic state are
     * available. Replaces any prior listener atomically.
     */
    public void setSnapshotListener(SnapshotListener listener) {
        this.snapshotListener = listener == null ? (g, s) -> {} : listener;
    }

    public JoinResult join(
            String groupId,
            String instanceId,
            Set<String> subscribedTopics,
            long sessionTimeoutMs,
            int rebalanceTimeoutMs,
            long nowNs) {
        var group = groupOf(groupId);
        JoinResult result;
        boolean structuralChange;
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
                    // no generation bump and NO snapshot fire — the on-disk
                    // record from the original join is still valid.
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
            structuralChange = true;
            result = new JoinResult(
                    newId, member.memberEpoch, group.generation, member.currentAssignment, HEARTBEAT_INTERVAL_MS);
        } finally {
            group.lock.unlock();
        }
        if (structuralChange) fireSnapshot(group);
        return result;
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
            // P7.7 — if the client reports it owns exactly the (stage-1)
            // kept set we advertised, and a pendingTarget is queued,
            // advance to stage 2 now.
            boolean advanced = advanceCooperativeIfReady(member, ownedPartitions);
            if (memberEpoch < member.memberEpoch || advanced) {
                // Member is behind — either a membership change recomputed
                // the assignment since its last heartbeat, or we just
                // advanced past the staged revoke. Either way, surface the
                // new assignment + bumped epoch.
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
        boolean changed = false;
        group.lock.lock();
        try {
            var removed = group.members.remove(memberId);
            if (removed == null) return;
            if (!removed.instanceId.isEmpty()) {
                group.instanceIndex.remove(removed.instanceId);
            }
            group.generation++;
            recomputeAssignment(group);
            changed = true;
        } finally {
            group.lock.unlock();
        }
        if (changed) fireSnapshot(group);
    }

    public List<EvictedMember> tickEvictions(long nowNs) {
        var evicted = new ArrayList<EvictedMember>();
        groupsLock.lock();
        var snapshot = new ArrayList<>(groups.values());
        groupsLock.unlock();
        var changedGroups = new ArrayList<GroupState>();
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
                changedGroups.add(group);
            } finally {
                group.lock.unlock();
            }
        }
        // Snapshot fires happen outside the group lock so the listener
        // (typically a Log.appendRaw on __consumer_offsets) can't deadlock
        // against an inbound RPC under concurrent load.
        for (var g : changedGroups) fireSnapshot(g);
        return List.copyOf(evicted);
    }

    /**
     * Side-effect-free member validation used by RPCs that need to gate on
     * group membership (CommitOffsets, FetchOffsets) without bumping the
     * member's lastHeartbeatNs. Returns:
     * <ul>
     *   <li>{@link HeartbeatOutcome#UNKNOWN_MEMBER_ID} — no such member.</li>
     *   <li>{@link HeartbeatOutcome#FENCED_MEMBER_EPOCH} — epoch mismatch (either direction).</li>
     *   <li>{@link HeartbeatOutcome#OK} — member exists and epoch matches.</li>
     * </ul>
     */
    public HeartbeatOutcome validateMember(String groupId, String memberId, int memberEpoch) {
        var group = groupOf(groupId);
        group.lock.lock();
        try {
            var member = group.members.get(memberId);
            if (member == null) return HeartbeatOutcome.UNKNOWN_MEMBER_ID;
            // CommitOffsets accepts epoch-behind (member is catching up) the
            // same way heartbeat does — only stale-future epochs are fenced.
            if (memberEpoch > member.memberEpoch) return HeartbeatOutcome.FENCED_MEMBER_EPOCH;
            return HeartbeatOutcome.OK;
        } finally {
            group.lock.unlock();
        }
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
            var target = assignment.getOrDefault(m.memberId, List.of());
            applyTargetCooperatively(m, target);
        }
    }

    /**
     * P7.7 — cooperative incremental rebalance step. Given a freshly-computed
     * {@code target} for the member:
     * <ul>
     *   <li>If the member loses no partitions (target ⊇ current OR member is
     *       brand new), advance directly to {@code target} — no staging
     *       needed because no other member is waiting on this one to release
     *       anything.</li>
     *   <li>Otherwise stage: advertise {@code current ∩ target} (the "kept"
     *       set) as the new {@code currentAssignment} and stash {@code target}
     *       in {@code pendingTarget}. The member acks the revoke by sending
     *       {@code owned_partitions == currentAssignment} on its next
     *       heartbeat; {@link #advanceCooperativeIfReady} then promotes
     *       currentAssignment to pendingTarget.</li>
     * </ul>
     * In both branches {@code memberEpoch} is bumped only when
     * {@code currentAssignment} actually changes (the assignor produced a
     * new set), so a no-op recompute under churn doesn't churn epochs.
     */
    private static void applyTargetCooperatively(MemberState m, List<TopicPartition> target) {
        var currentSet = new HashSet<>(m.currentAssignment);
        var targetSet = new HashSet<>(target);
        var lost = new HashSet<>(currentSet);
        lost.removeAll(targetSet);

        List<TopicPartition> newCurrent;
        List<TopicPartition> pending;
        if (lost.isEmpty()) {
            // Pure additions (or no change) — go straight to target.
            newCurrent = target;
            pending = null;
        } else {
            // Staging: advertise the kept set first; member must ack before
            // we hand over the additions (if any).
            var kept = new ArrayList<TopicPartition>();
            for (var tp : m.currentAssignment) {
                if (targetSet.contains(tp)) kept.add(tp);
            }
            newCurrent = kept;
            // Only stash a pendingTarget if there are additions — if target
            // == kept (member only loses), staging completes in one round
            // (member acks the revoke, no follow-up needed).
            pending = sameAssignment(kept, target) ? null : target;
        }

        if (!sameAssignment(m.currentAssignment, newCurrent)) {
            m.currentAssignment = List.copyOf(newCurrent);
            m.memberEpoch++;
        }
        // pendingTarget swap doesn't bump epoch on its own — only when the
        // advertised currentAssignment changes does the client need to react.
        m.pendingTarget = pending == null ? null : List.copyOf(pending);
    }

    /**
     * Called from {@link #heartbeat} when a member reports
     * {@code ownedPartitions} matching its currently-advertised assignment.
     * If a {@code pendingTarget} is queued, advance to it (the member has
     * confirmed it released the revoked partitions) and bump epoch so the
     * heartbeat response carries the new assignment.
     *
     * @return {@code true} if currentAssignment changed (caller should
     *     return the new assignment in the heartbeat response).
     */
    private static boolean advanceCooperativeIfReady(MemberState m, List<TopicPartition> ownedPartitions) {
        if (m.pendingTarget == null) return false;
        if (!sameAssignment(ownedPartitions, m.currentAssignment)) return false;
        // Member acked revoke (owned_partitions matches what we advertised).
        m.currentAssignment = m.pendingTarget;
        m.pendingTarget = null;
        m.memberEpoch++;
        return true;
    }

    private static boolean sameAssignment(Collection<TopicPartition> a, Collection<TopicPartition> b) {
        if (a.size() != b.size()) return false;
        var as = new HashSet<>(a);
        var bs = new HashSet<>(b);
        return as.equals(bs);
    }

    /**
     * P7.8 — capture the group's current state in a form that can be encoded
     * via {@link jbroker.broker.ConsumerOffsetsTopic#valueForGroupMetadata}
     * and persisted to {@code __consumer_offsets}.
     */
    public Optional<jbroker.broker.ConsumerOffsetsTopic.GroupMetadataValue> snapshotGroup(String groupId) {
        var group = groupOf(groupId);
        group.lock.lock();
        try {
            if (group.members.isEmpty()) {
                return Optional.of(new jbroker.broker.ConsumerOffsetsTopic.GroupMetadataValue(
                        group.generation, java.util.List.of()));
            }
            var snapshots = new java.util.ArrayList<jbroker.broker.ConsumerOffsetsTopic.MemberSnapshot>();
            // Stable ordering on member_id so the persisted bytes are
            // reproducible regardless of HashMap iteration order.
            var sortedIds = new java.util.ArrayList<>(group.members.keySet());
            java.util.Collections.sort(sortedIds);
            for (var mid : sortedIds) {
                var m = group.members.get(mid);
                var subs = new java.util.ArrayList<>(m.subscribedTopics);
                java.util.Collections.sort(subs);
                snapshots.add(new jbroker.broker.ConsumerOffsetsTopic.MemberSnapshot(
                        m.memberId, m.instanceId, m.memberEpoch, subs, new java.util.ArrayList<>(m.currentAssignment)));
            }
            return Optional.of(new jbroker.broker.ConsumerOffsetsTopic.GroupMetadataValue(group.generation, snapshots));
        } finally {
            group.lock.unlock();
        }
    }

    /**
     * P7.8 — rebuild a group's state from a persisted snapshot. Called by
     * the recovery walk after coordinator activation. Existing in-memory
     * state for the group is overwritten — the on-disk record is the
     * source of truth.
     *
     * <p>{@code lastHeartbeatNs} for each member is reset to {@code nowNs}
     * so that members aren't immediately evicted before they have a chance
     * to send their first post-failover heartbeat. Their next heartbeat
     * also re-establishes their session-timeout commitment.
     */
    public void restoreGroup(
            String groupId, jbroker.broker.ConsumerOffsetsTopic.GroupMetadataValue snapshot, long nowNs) {
        var group = groupOf(groupId);
        group.lock.lock();
        try {
            group.members.clear();
            group.instanceIndex.clear();
            group.generation = snapshot.generation();
            for (var m : snapshot.members()) {
                var member = new MemberState(
                        m.memberId(),
                        m.instanceId() == null ? "" : m.instanceId(),
                        new HashSet<>(m.subscribedTopics()),
                        m.memberEpoch(),
                        nowNs,
                        DEFAULT_SESSION_TIMEOUT_MS * 1_000_000L,
                        /*rebalanceTimeoutMs*/ 60_000,
                        m.assignment());
                group.members.put(member.memberId, member);
                if (!member.instanceId.isEmpty()) {
                    group.instanceIndex.put(member.instanceId, member.memberId);
                }
            }
        } finally {
            group.lock.unlock();
        }
    }

    private void fireSnapshot(GroupState group) {
        try {
            var snap = snapshotGroup(group.groupId).orElse(null);
            if (snap != null) snapshotListener.onGroupSnapshot(group.groupId, snap);
        } catch (Exception ignored) {
            // Snapshot persistence failures must not break the live RPC
            // path. The next membership change will retry; the recovery
            // walk falls back to whatever's already on disk if a window
            // of writes was lost.
        }
    }
}
