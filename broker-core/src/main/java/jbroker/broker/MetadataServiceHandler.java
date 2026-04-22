package jbroker.broker;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import jbroker.proto.broker.BrokerInfo;
import jbroker.proto.broker.ConsumerGroupSummary;
import jbroker.proto.broker.DescribeClusterRequest;
import jbroker.proto.broker.DescribeClusterResponse;
import jbroker.proto.broker.DescribeConsumerGroupRequest;
import jbroker.proto.broker.DescribeConsumerGroupResponse;
import jbroker.proto.broker.DescribeMetricsRequest;
import jbroker.proto.broker.DescribeMetricsResponse;
import jbroker.proto.broker.DescribeRaftRequest;
import jbroker.proto.broker.DescribeRaftResponse;
import jbroker.proto.broker.DescribeTopicPartitionsRequest;
import jbroker.proto.broker.DescribeTopicPartitionsResponse;
import jbroker.proto.broker.ListConsumerGroupsRequest;
import jbroker.proto.broker.ListConsumerGroupsResponse;
import jbroker.proto.broker.MemberInfo;
import jbroker.proto.broker.PartitionLag;
import jbroker.proto.broker.PartitionStateInfo;
import jbroker.proto.broker.TopicPartitions;
import jbroker.proto.common.ErrorCode;
import jbroker.proto.common.TopicPartition;
import jbroker.storage.Log;

/**
 * Handler for the Phase 8 {@code Metadata} gRPC service — the read-only
 * observability surface that the admin-app REST layer consumes.
 *
 * <p>Phase 8 slice responsibilities:
 * <ul>
 *   <li>P8.2 — {@link #describeCluster} (implemented below)</li>
 *   <li>P8.3 — {@link #describeTopicPartitions}</li>
 *   <li>P8.4 — {@link #listConsumerGroups} + {@link #describeConsumerGroup}</li>
 *   <li>P8.5 — {@link #describeRaft}</li>
 * </ul>
 */
public final class MetadataServiceHandler {

    private final int selfBrokerId;
    private final BrokerRegistry brokerRegistry;
    private final BrokerLiveness brokerLiveness;
    private final Supplier<String> selfRole;
    private final Supplier<Optional<Integer>> currentLeaderId;
    private final LongSupplier currentTerm;
    private final LongSupplier metadataOffset;
    private final Supplier<Long> nowNanos;
    private final long stalenessThresholdNanos;
    private final TopicManager topicManager;
    private final jbroker.storage.LogManager logManager;
    private final jbroker.broker.group.GroupCoordinator groupCoordinator;
    private final jbroker.broker.group.OffsetCache offsetCache;
    private final Supplier<jbroker.raft.core.RaftCore.Observability> raftObservability;
    private final BrokerMetrics brokerMetrics;

    /** Default staleness threshold: matches {@code BrokerFencer}'s 3s default. */
    public static final long DEFAULT_STALENESS_NANOS = TimeUnit.SECONDS.toNanos(3);

    public MetadataServiceHandler(
            int selfBrokerId,
            BrokerRegistry brokerRegistry,
            BrokerLiveness brokerLiveness,
            Supplier<String> selfRole,
            Supplier<Optional<Integer>> currentLeaderId,
            LongSupplier currentTerm,
            LongSupplier metadataOffset,
            Supplier<Long> nowNanos,
            long stalenessThresholdNanos,
            TopicManager topicManager,
            jbroker.storage.LogManager logManager) {
        this(
                selfBrokerId,
                brokerRegistry,
                brokerLiveness,
                selfRole,
                currentLeaderId,
                currentTerm,
                metadataOffset,
                nowNanos,
                stalenessThresholdNanos,
                topicManager,
                logManager,
                null,
                null);
    }

    /** P8.4 back-compat overload without Raft observability / metrics. */
    public MetadataServiceHandler(
            int selfBrokerId,
            BrokerRegistry brokerRegistry,
            BrokerLiveness brokerLiveness,
            Supplier<String> selfRole,
            Supplier<Optional<Integer>> currentLeaderId,
            LongSupplier currentTerm,
            LongSupplier metadataOffset,
            Supplier<Long> nowNanos,
            long stalenessThresholdNanos,
            TopicManager topicManager,
            jbroker.storage.LogManager logManager,
            jbroker.broker.group.GroupCoordinator groupCoordinator,
            jbroker.broker.group.OffsetCache offsetCache) {
        this(
                selfBrokerId,
                brokerRegistry,
                brokerLiveness,
                selfRole,
                currentLeaderId,
                currentTerm,
                metadataOffset,
                nowNanos,
                stalenessThresholdNanos,
                topicManager,
                logManager,
                groupCoordinator,
                offsetCache,
                null,
                null);
    }

    /** P8.5 — full constructor with Raft observability + broker metrics for /raft + /metrics endpoints. */
    public MetadataServiceHandler(
            int selfBrokerId,
            BrokerRegistry brokerRegistry,
            BrokerLiveness brokerLiveness,
            Supplier<String> selfRole,
            Supplier<Optional<Integer>> currentLeaderId,
            LongSupplier currentTerm,
            LongSupplier metadataOffset,
            Supplier<Long> nowNanos,
            long stalenessThresholdNanos,
            TopicManager topicManager,
            jbroker.storage.LogManager logManager,
            jbroker.broker.group.GroupCoordinator groupCoordinator,
            jbroker.broker.group.OffsetCache offsetCache,
            Supplier<jbroker.raft.core.RaftCore.Observability> raftObservability,
            BrokerMetrics brokerMetrics) {
        this.selfBrokerId = selfBrokerId;
        this.brokerRegistry = brokerRegistry;
        this.brokerLiveness = brokerLiveness;
        this.selfRole = selfRole;
        this.currentLeaderId = currentLeaderId;
        this.currentTerm = currentTerm;
        this.metadataOffset = metadataOffset;
        this.nowNanos = nowNanos;
        this.stalenessThresholdNanos = stalenessThresholdNanos;
        this.topicManager = topicManager;
        this.logManager = logManager;
        this.groupCoordinator = groupCoordinator;
        this.offsetCache = offsetCache;
        this.raftObservability = raftObservability;
        this.brokerMetrics = brokerMetrics;
    }

    /** P8.2 back-compat overload — no TopicManager / LogManager wired. */
    public MetadataServiceHandler(
            int selfBrokerId,
            BrokerRegistry brokerRegistry,
            BrokerLiveness brokerLiveness,
            Supplier<String> selfRole,
            Supplier<Optional<Integer>> currentLeaderId,
            LongSupplier currentTerm,
            LongSupplier metadataOffset,
            Supplier<Long> nowNanos,
            long stalenessThresholdNanos) {
        this(
                selfBrokerId,
                brokerRegistry,
                brokerLiveness,
                selfRole,
                currentLeaderId,
                currentTerm,
                metadataOffset,
                nowNanos,
                stalenessThresholdNanos,
                null,
                null);
    }

    /**
     * Back-compat overload used by P8.1 tests that didn't need real
     * observability data. Wires every supplier to a harmless default so the
     * handler still returns well-formed {@code UNIMPLEMENTED} responses for
     * the RPCs P8.2 does not implement.
     */
    public MetadataServiceHandler() {
        this(
                0,
                new BrokerRegistry(),
                new BrokerLiveness(),
                () -> "UNKNOWN",
                Optional::empty,
                () -> 0L,
                () -> 0L,
                System::nanoTime,
                DEFAULT_STALENESS_NANOS,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    public DescribeClusterResponse describeCluster(DescribeClusterRequest req) {
        var builder = DescribeClusterResponse.newBuilder()
                .setError(ErrorCode.OK)
                .setControllerId(currentLeaderId.get().orElse(-1))
                .setCurrentTerm(currentTerm.getAsLong())
                .setMetadataOffset(metadataOffset.getAsLong());
        long now = nowNanos.get();
        // Gather every broker the metadata replica knows about. Fall back to
        // self if the registry is still empty (single-broker freshly-booted
        // clusters) so the admin-app never sees an empty node list.
        var known = new java.util.TreeSet<>(brokerRegistry.knownBrokerIds());
        if (known.isEmpty()) known.add(selfBrokerId);
        for (int bid : known) {
            var addr = brokerRegistry.addressFor(bid);
            String host = addr.map(BrokerRegistry.HostPort::host).orElse("");
            int port = addr.map(BrokerRegistry.HostPort::port).orElse(0);
            String role = bid == selfBrokerId ? selfRole.get() : "UNKNOWN";
            boolean alive;
            long lastSeenMillis;
            if (bid == selfBrokerId) {
                // A broker knows itself is alive so long as it is answering
                // this RPC. Last-seen for self is simply "now" (millis).
                alive = true;
                lastSeenMillis = System.currentTimeMillis();
            } else {
                var sig = brokerLiveness.lastSignal(bid);
                if (sig.isEmpty()) {
                    alive = false;
                    lastSeenMillis = 0L;
                } else {
                    long ageNanos = now - sig.get().wallClockNanos();
                    alive = ageNanos <= stalenessThresholdNanos;
                    lastSeenMillis = System.currentTimeMillis() - TimeUnit.NANOSECONDS.toMillis(ageNanos);
                }
            }
            builder.addNodes(BrokerInfo.newBuilder()
                    .setBrokerId(bid)
                    .setHost(host)
                    .setPort(port)
                    .setRole(role)
                    .setAlive(alive)
                    .setLastSeenMillis(lastSeenMillis)
                    .build());
        }
        return builder.build();
    }

    public DescribeTopicPartitionsResponse describeTopicPartitions(DescribeTopicPartitionsRequest req) {
        if (topicManager == null) {
            return DescribeTopicPartitionsResponse.newBuilder()
                    .setError(ErrorCode.UNIMPLEMENTED)
                    .setTopic(req.getTopic())
                    .build();
        }
        var desc = topicManager.describe(req.getTopic());
        if (desc.isEmpty()) {
            return DescribeTopicPartitionsResponse.newBuilder()
                    .setError(ErrorCode.UNKNOWN)
                    .setTopic(req.getTopic())
                    .build();
        }
        var td = desc.get();
        var builder = DescribeTopicPartitionsResponse.newBuilder()
                .setError(ErrorCode.OK)
                .setTopic(td.topic())
                .setPartitions(td.partitions())
                .setReplicationFactor(td.replicationFactor())
                .setInternal(td.internal())
                .setCompact(td.compact())
                .setCreatedMillis(td.createdMillis())
                .putAllConfig(td.config());
        for (int p = 0; p < td.partitions(); p++) {
            var state = topicManager.partitionState(td.topic(), p).orElse(null);
            var ps = PartitionStateInfo.newBuilder().setPartition(p);
            long hwm = -1L;
            long leo = -1L;
            if (state != null) {
                ps.setLeader(state.leader());
                for (int b : state.isr()) {
                    ps.addIsr(b);
                }
                for (int b : state.replicas()) {
                    ps.addReplicas(b);
                }
                ps.setLeaderEpoch(state.leaderEpoch());
                ps.setPartitionEpoch(state.partitionEpoch());
                // HWM + LEO readable only if self is this partition's leader
                // (Phase 5/6 single-leader model). Followers return -1 so
                // the admin-app fan-out can pick the leader's answer.
                if (logManager != null && state.leader() == selfBrokerId) {
                    try {
                        Log log = logManager.logFor(td.topic(), p);
                        long next = log.nextOffset();
                        hwm = next;
                        leo = next;
                    } catch (Exception ignored) {
                        // log missing on a leader is unusual — fall through
                        // with sentinels rather than fail the whole response
                    }
                }
            } else {
                ps.setLeader(-1);
            }
            ps.setHighWatermark(hwm);
            ps.setLogEndOffset(leo);
            builder.addPartitionStates(ps.build());
        }
        return builder.build();
    }

    public ListConsumerGroupsResponse listConsumerGroups(ListConsumerGroupsRequest req) {
        if (groupCoordinator == null) {
            return ListConsumerGroupsResponse.newBuilder()
                    .setError(ErrorCode.UNIMPLEMENTED)
                    .build();
        }
        var builder = ListConsumerGroupsResponse.newBuilder().setError(ErrorCode.OK);
        for (var g : groupCoordinator.listGroups()) {
            int coordPart = -1;
            var offsetsTopic = topicManager == null
                    ? Optional.<TopicDescription>empty()
                    : topicManager.describe(jbroker.broker.ConsumerOffsetsTopic.NAME);
            if (offsetsTopic.isPresent()) {
                coordPart =
                        Math.floorMod(g.groupId().hashCode(), offsetsTopic.get().partitions());
            }
            builder.addGroups(ConsumerGroupSummary.newBuilder()
                    .setGroupId(g.groupId())
                    .setState(g.state())
                    .setMemberCount(g.memberCount())
                    .setGeneration(g.generation())
                    .setAssignor("range")
                    .setCoordinatorPartition(coordPart)
                    .build());
        }
        return builder.build();
    }

    public DescribeConsumerGroupResponse describeConsumerGroup(DescribeConsumerGroupRequest req) {
        if (groupCoordinator == null) {
            return DescribeConsumerGroupResponse.newBuilder()
                    .setError(ErrorCode.UNIMPLEMENTED)
                    .setGroupId(req.getGroupId())
                    .build();
        }
        var detail = groupCoordinator.describeGroup(req.getGroupId());
        if (detail.isEmpty()) {
            // Wrong coordinator or unknown group — admin-app fan-out
            // retries on another broker. NOT_COORDINATOR signals "keep
            // trying elsewhere"; UNKNOWN_MEMBER_ID would be wrong here
            // because we can't tell the two cases apart without extra
            // state, so lean towards the routing hint.
            return DescribeConsumerGroupResponse.newBuilder()
                    .setError(ErrorCode.NOT_COORDINATOR)
                    .setGroupId(req.getGroupId())
                    .build();
        }
        var d = detail.get();
        var builder = DescribeConsumerGroupResponse.newBuilder()
                .setError(ErrorCode.OK)
                .setGroupId(d.groupId())
                .setGeneration(d.generation())
                .setState(d.state())
                .setAssignor("range");
        // Per-member echo.
        for (var m : d.members()) {
            var mi = MemberInfo.newBuilder()
                    .setMemberId(m.memberId())
                    .setInstanceId(m.instanceId())
                    .setMemberEpoch(m.memberEpoch());
            for (var t : m.subscribedTopics()) {
                mi.addSubscribedTopics(t);
            }
            // Group owned_partitions by topic so the UI can render them
            // compactly. Same shape the cooperative-rebalance heartbeat
            // uses on the wire.
            var byTopic = new java.util.LinkedHashMap<String, java.util.List<Integer>>();
            for (var tp : m.ownedPartitions()) {
                byTopic.computeIfAbsent(tp.getTopic(), k -> new java.util.ArrayList<>())
                        .add(tp.getPartition());
            }
            for (var e : byTopic.entrySet()) {
                var tps = TopicPartitions.newBuilder().setTopic(e.getKey());
                for (int p : e.getValue()) {
                    tps.addPartitions(p);
                }
                mi.addOwnedPartitions(tps.build());
            }
            builder.addMembers(mi.build());
        }
        // Per-partition lag. For each committed (tp), look up HWM on self's
        // log if this broker leads the partition; otherwise emit -1 so the
        // admin-app can fan-out to the leader and merge.
        if (offsetCache != null) {
            var committed = offsetCache.snapshotForGroup(req.getGroupId());
            // Determine owner member per (topic, partition) for the UI.
            var ownerByTp = new java.util.HashMap<TopicPartition, String>();
            for (var m : d.members()) {
                for (var tp : m.ownedPartitions()) {
                    ownerByTp.put(tp, m.memberId());
                }
            }
            for (var entry : committed.entrySet()) {
                var tp = entry.getKey();
                long committedOff = entry.getValue().offset();
                long hwm = -1L;
                if (logManager != null && topicManager != null) {
                    var ps = topicManager.partitionState(tp.getTopic(), tp.getPartition());
                    if (ps.isPresent() && ps.get().leader() == selfBrokerId) {
                        try {
                            Log log = logManager.logFor(tp.getTopic(), tp.getPartition());
                            hwm = log.nextOffset();
                        } catch (Exception ignored) {
                            // fall through with -1
                        }
                    }
                }
                long lag = hwm < 0 ? -1L : Math.max(0L, hwm - committedOff);
                builder.addPartitions(PartitionLag.newBuilder()
                        .setTp(tp)
                        .setCommittedOffset(committedOff)
                        .setHighWatermark(hwm)
                        .setLag(lag)
                        .setOwnerMemberId(ownerByTp.getOrDefault(tp, ""))
                        .build());
            }
        }
        return builder.build();
    }

    public DescribeRaftResponse describeRaft(DescribeRaftRequest req) {
        if (raftObservability == null) {
            return DescribeRaftResponse.newBuilder()
                    .setError(ErrorCode.UNIMPLEMENTED)
                    .build();
        }
        var o = raftObservability.get();
        return DescribeRaftResponse.newBuilder()
                .setError(ErrorCode.OK)
                .setNodeId(selfBrokerId)
                .setRole(selfRole.get())
                .setCurrentTerm(o.currentTerm())
                .setCommitIndex(o.commitIndex())
                .setLastApplied(o.lastApplied())
                .setVotedFor(o.votedFor())
                .setLastLogIndex(o.lastLogIndex())
                .setLastLogTerm(o.lastLogTerm())
                .build();
    }

    /**
     * P8.5 — snapshot the broker's rolling metrics. The admin REST layer
     * fans out across brokers and returns the union (or aggregated) view.
     * Returns {@code null} if no {@link BrokerMetrics} is wired (tests).
     */
    public BrokerMetrics brokerMetrics() {
        return brokerMetrics;
    }

    public DescribeMetricsResponse describeMetrics(DescribeMetricsRequest req) {
        if (brokerMetrics == null) {
            return DescribeMetricsResponse.newBuilder()
                    .setError(ErrorCode.UNIMPLEMENTED)
                    .setBrokerId(selfBrokerId)
                    .build();
        }
        var t = brokerMetrics.throughputSnapshot();
        var pl = brokerMetrics.produceLatencySnapshot();
        var fl = brokerMetrics.fetchLatencySnapshot();
        return DescribeMetricsResponse.newBuilder()
                .setError(ErrorCode.OK)
                .setBrokerId(selfBrokerId)
                .setWindowSeconds(t.windowSeconds())
                .setProduceCount(t.produceCount())
                .setProduceBytes(t.produceBytes())
                .setFetchCount(t.fetchCount())
                .setFetchBytes(t.fetchBytes())
                .setProduceBytesPerSec(t.produceBytesPerSec())
                .setFetchBytesPerSec(t.fetchBytesPerSec())
                .setProduceP50Nanos(pl.p50Nanos())
                .setProduceP99Nanos(pl.p99Nanos())
                .setProduceP999Nanos(pl.p999Nanos())
                .setFetchP50Nanos(fl.p50Nanos())
                .setFetchP99Nanos(fl.p99Nanos())
                .setFetchP999Nanos(fl.p999Nanos())
                .setIncrementalFetchHits(brokerMetrics.incrementalFetchHits())
                .build();
    }

    /** Static helper: turn a {@code Role} enum name into the string PRD §8.7 uses. */
    public static String roleName(Object raftRole) {
        return raftRole == null ? "UNKNOWN" : raftRole.toString();
    }

    /** Visible for tests: returns the configured broker ids of every known peer (self first). */
    List<Integer> knownBrokerIds() {
        var out = new java.util.ArrayList<Integer>(brokerRegistry.knownBrokerIds());
        java.util.Collections.sort(out);
        if (!out.contains(selfBrokerId)) out.add(0, selfBrokerId);
        return List.copyOf(out);
    }
}
