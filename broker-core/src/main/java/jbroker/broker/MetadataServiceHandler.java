package jbroker.broker;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import jbroker.proto.broker.BrokerInfo;
import jbroker.proto.broker.DescribeClusterRequest;
import jbroker.proto.broker.DescribeClusterResponse;
import jbroker.proto.broker.DescribeConsumerGroupRequest;
import jbroker.proto.broker.DescribeConsumerGroupResponse;
import jbroker.proto.broker.DescribeRaftRequest;
import jbroker.proto.broker.DescribeRaftResponse;
import jbroker.proto.broker.DescribeTopicPartitionsRequest;
import jbroker.proto.broker.DescribeTopicPartitionsResponse;
import jbroker.proto.broker.ListConsumerGroupsRequest;
import jbroker.proto.broker.ListConsumerGroupsResponse;
import jbroker.proto.broker.PartitionStateInfo;
import jbroker.proto.common.ErrorCode;
import jbroker.storage.Log;

/**
 * Handler for the Milestone 8 {@code Metadata} gRPC service — the read-only
 * observability surface that the admin-app REST layer consumes.
 *
 * <p>Milestone 8 slice responsibilities:
 * <ul>
 *   <li>{@link #describeCluster} (implemented below)</li>
 *   <li>{@link #describeTopicPartitions}</li>
 *   <li>{@link #listConsumerGroups} + {@link #describeConsumerGroup}</li>
 *   <li>{@link #describeRaft}</li>
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
    }

    /** back-compat overload — no TopicManager / LogManager wired. */
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
     * Back-compat overload used by tests that didn't need real
     * observability data. Wires every supplier to a harmless default so the
     * handler still returns well-formed {@code UNIMPLEMENTED} responses for
     * the RPCs does not implement.
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
                DEFAULT_STALENESS_NANOS);
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
                // (Milestone 5/6 single-leader model). Followers return -1 so
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
        return ListConsumerGroupsResponse.newBuilder()
                .setError(ErrorCode.UNIMPLEMENTED)
                .build();
    }

    public DescribeConsumerGroupResponse describeConsumerGroup(DescribeConsumerGroupRequest req) {
        return DescribeConsumerGroupResponse.newBuilder()
                .setError(ErrorCode.UNIMPLEMENTED)
                .setGroupId(req.getGroupId())
                .build();
    }

    public DescribeRaftResponse describeRaft(DescribeRaftRequest req) {
        return DescribeRaftResponse.newBuilder()
                .setError(ErrorCode.UNIMPLEMENTED)
                .build();
    }

    /** Static helper: turn a {@code Role} enum name into the string the spec uses. */
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
