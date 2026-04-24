package jbroker.broker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import jbroker.broker.group.GroupCoordinator;
import jbroker.broker.group.OffsetCache;
import jbroker.proto.broker.Assignment;
import jbroker.proto.broker.CommitOffsetsRequest;
import jbroker.proto.broker.CommitOffsetsResponse;
import jbroker.proto.broker.CommitResult;
import jbroker.proto.broker.ConsumerGroupHeartbeatRequest;
import jbroker.proto.broker.ConsumerGroupHeartbeatResponse;
import jbroker.proto.broker.FetchOffsetsRequest;
import jbroker.proto.broker.FetchOffsetsResponse;
import jbroker.proto.broker.FindCoordinatorRequest;
import jbroker.proto.broker.FindCoordinatorResponse;
import jbroker.proto.broker.ListOffsetsRequest;
import jbroker.proto.broker.ListOffsetsResponse;
import jbroker.proto.broker.ListOffsetsResult;
import jbroker.proto.broker.OffsetFetchResult;
import jbroker.proto.broker.TopicPartitions;
import jbroker.proto.common.BrokerEndpoint;
import jbroker.proto.common.ErrorCode;
import jbroker.storage.LogManager;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;

/**
 * Routes the Phase 7 consumer-group RPCs ({@code FindCoordinator},
 * {@code ConsumerGroupHeartbeat}, {@code CommitOffsets},
 * {@code FetchOffsets}, {@code ListOffsets}).
 *
 * <p>P7.1 wires the surface only — coordinator-routed RPCs return
 * {@link ErrorCode#COORDINATOR_NOT_AVAILABLE} until the
 * {@code GroupCoordinator} lands in P7.4 / P7.5. {@code FetchOffsets}
 * returns {@link ErrorCode#OFFSET_OUT_OF_RANGE} until P7.6 plugs in the
 * persisted offset cache.
 *
 * <p>{@code ListOffsets} IS implemented in P7.1 — it just delegates to
 * {@link LogManager} and returns the partition's {@code nextOffset()} for
 * the latest sentinel ({@code timestamp == -1}), {@code logStartOffset()}
 * (currently always 0) for the earliest sentinel ({@code timestamp == -2}),
 * and ignores by-timestamp lookups for now (returns latest as a safe
 * upper bound — refined in a later milestone when the time index gains
 * a public lookup API).
 */
public final class ConsumerHandler {

    private final TopicManager topicManager;
    private final LogManager logManager;
    private final BrokerRegistry brokerRegistry;
    private final GroupCoordinator groupCoordinator;
    private final OffsetCache offsetCache;
    private final int selfBrokerId;
    private final java.util.function.LongSupplier nanoClock;
    private final java.util.function.LongSupplier wallClockMillis;

    /**
     * Phase 7.1 / 7.3 form — no group coordinator. Coordinator-routed RPCs
     * fall back to the placeholder error codes. Used by tests that don't
     * exercise the heartbeat path; production wiring uses the full form.
     */
    public ConsumerHandler(TopicManager topicManager, LogManager logManager, BrokerRegistry brokerRegistry) {
        this(
                topicManager,
                logManager,
                brokerRegistry,
                /*groupCoordinator*/ null,
                /*offsetCache*/ null,
                /*selfBrokerId*/ -1,
                System::nanoTime,
                System::currentTimeMillis);
    }

    /** P7.5 form — kept for tests that don't exercise offset persistence. */
    public ConsumerHandler(
            TopicManager topicManager,
            LogManager logManager,
            BrokerRegistry brokerRegistry,
            GroupCoordinator groupCoordinator,
            int selfBrokerId,
            java.util.function.LongSupplier nanoClock) {
        this(
                topicManager,
                logManager,
                brokerRegistry,
                groupCoordinator,
                /*offsetCache*/ null,
                selfBrokerId,
                nanoClock,
                System::currentTimeMillis);
    }

    public ConsumerHandler(
            TopicManager topicManager,
            LogManager logManager,
            BrokerRegistry brokerRegistry,
            GroupCoordinator groupCoordinator,
            OffsetCache offsetCache,
            int selfBrokerId,
            java.util.function.LongSupplier nanoClock,
            java.util.function.LongSupplier wallClockMillis) {
        this.topicManager = topicManager;
        this.logManager = logManager;
        this.brokerRegistry = brokerRegistry;
        this.groupCoordinator = groupCoordinator;
        this.offsetCache = offsetCache;
        this.selfBrokerId = selfBrokerId;
        this.nanoClock = nanoClock;
        this.wallClockMillis = wallClockMillis;
    }

    /**
     * Routes a group to the broker that hosts its coordinator partition in
     * {@code __consumer_offsets}. Coordinator partition is
     * {@code Math.floorMod(group_id.hashCode(), partitionCount)}; that
     * partition's leader is the coordinator. Returns
     * {@link ErrorCode#COORDINATOR_NOT_AVAILABLE} when:
     * <ul>
     *   <li>{@code __consumer_offsets} doesn't exist yet (controller hasn't
     *       finished auto-create — racing with broker startup), or</li>
     *   <li>the coordinator partition's current leader is the {@code -1}
     *       sentinel (no surviving ISR member after a fence), or</li>
     *   <li>the leader broker has no entry in {@link BrokerRegistry} (the
     *       controller proposed the registration but it hasn't applied on
     *       this broker yet).</li>
     * </ul>
     * The client retries on this error after a short backoff.
     */
    public FindCoordinatorResponse findCoordinator(FindCoordinatorRequest req) {
        var topicDesc = topicManager.describe(ConsumerOffsetsTopic.NAME);
        if (topicDesc.isEmpty()) {
            return FindCoordinatorResponse.newBuilder()
                    .setError(ErrorCode.COORDINATOR_NOT_AVAILABLE)
                    .build();
        }
        int partitionCount = topicDesc.get().partitions();
        int partition = Math.floorMod(req.getKey().hashCode(), partitionCount);
        var partitionState = topicManager.partitionState(ConsumerOffsetsTopic.NAME, partition);
        if (partitionState.isEmpty() || partitionState.get().leader() <= 0) {
            return FindCoordinatorResponse.newBuilder()
                    .setError(ErrorCode.COORDINATOR_NOT_AVAILABLE)
                    .build();
        }
        int leaderId = partitionState.get().leader();
        var address = brokerRegistry.addressFor(leaderId);
        if (address.isEmpty()) {
            return FindCoordinatorResponse.newBuilder()
                    .setError(ErrorCode.COORDINATOR_NOT_AVAILABLE)
                    .build();
        }
        return FindCoordinatorResponse.newBuilder()
                .setError(ErrorCode.OK)
                .setCoordinator(BrokerEndpoint.newBuilder()
                        .setNodeId(leaderId)
                        .setHost(address.get().host())
                        .setPort(address.get().port())
                        .build())
                .build();
    }

    /**
     * KIP-848 server-driven heartbeat. The single RPC carries join /
     * steady-state / leave semantics depending on {@code member_epoch}:
     * <ul>
     *   <li>{@code member_id == ""} or {@code member_epoch == 0}: join.
     *       Coordinator allocates a member id, runs the assignor, returns
     *       the assignment + bumped epoch.</li>
     *   <li>{@code member_epoch == -1}: leave. Coordinator drops the
     *       member, recomputes assignment for survivors, returns OK with
     *       {@code member_epoch = 0}.</li>
     *   <li>else: steady-state heartbeat. Returns OK with the new
     *       assignment (if recomputed since last fire) or empty (no
     *       change). Stale epochs surface FENCED_MEMBER_EPOCH; unknown
     *       member ids surface UNKNOWN_MEMBER_ID.</li>
     * </ul>
     *
     * <p>Coordinator routing guard: if this broker doesn't lead the group's
     * coordinator partition, returns NOT_COORDINATOR so the client refreshes
     * via {@link #findCoordinator}. NOT_COORDINATOR also fires when the
     * coordinator partition is unavailable (no leader) — that case is
     * indistinguishable from the routing-mismatch case for the client (both
     * trigger the same retry path).
     */
    public ConsumerGroupHeartbeatResponse consumerGroupHeartbeat(ConsumerGroupHeartbeatRequest req) {
        if (groupCoordinator == null) {
            return ConsumerGroupHeartbeatResponse.newBuilder()
                    .setError(ErrorCode.COORDINATOR_NOT_AVAILABLE)
                    .build();
        }
        var routing = coordinatorRoutingFor(req.getGroupId());
        if (routing != ErrorCode.OK) {
            return ConsumerGroupHeartbeatResponse.newBuilder().setError(routing).build();
        }
        long nowNs = nanoClock.getAsLong();

        // Leave (member_epoch == -1)
        if (req.getMemberEpoch() == -1 && !req.getMemberId().isEmpty()) {
            groupCoordinator.leave(req.getGroupId(), req.getMemberId());
            return ConsumerGroupHeartbeatResponse.newBuilder()
                    .setError(ErrorCode.OK)
                    .setMemberId(req.getMemberId())
                    .setMemberEpoch(0)
                    .setHeartbeatIntervalMs(GroupCoordinator.HEARTBEAT_INTERVAL_MS)
                    .build();
        }

        // Join (member_id == "" or member_epoch == 0)
        if (req.getMemberId().isEmpty() || req.getMemberEpoch() == 0) {
            long sessionTimeoutMs = req.getRebalanceTimeoutMs() > 0
                    ? req.getRebalanceTimeoutMs()
                    : GroupCoordinator.DEFAULT_SESSION_TIMEOUT_MS;
            var subscribed = new HashSet<>(req.getSubscribedTopicsList());
            var join = groupCoordinator.join(
                    req.getGroupId(),
                    req.getInstanceId(),
                    subscribed,
                    sessionTimeoutMs,
                    req.getRebalanceTimeoutMs(),
                    nowNs);
            return ConsumerGroupHeartbeatResponse.newBuilder()
                    .setError(ErrorCode.OK)
                    .setMemberId(join.memberId())
                    .setMemberEpoch(join.memberEpoch())
                    .setHeartbeatIntervalMs(join.heartbeatIntervalMs())
                    .setAssignment(toProtoAssignment(join.assignment()))
                    .build();
        }

        // Steady-state heartbeat. Flatten the request's owned_partitions
        // (one TopicPartitions per topic) into a flat list of TopicPartition
        // pairs so the coordinator can compare it against currentAssignment
        // for cooperative incremental rebalance ack detection (P7.7).
        var owned = new java.util.ArrayList<jbroker.proto.common.TopicPartition>();
        for (var tp : req.getOwnedPartitionsList()) {
            for (int p : tp.getPartitionsList()) {
                owned.add(jbroker.proto.common.TopicPartition.newBuilder()
                        .setTopic(tp.getTopic())
                        .setPartition(p)
                        .build());
            }
        }
        var hb = groupCoordinator.heartbeat(req.getGroupId(), req.getMemberId(), req.getMemberEpoch(), owned, nowNs);
        var b = ConsumerGroupHeartbeatResponse.newBuilder()
                .setMemberId(req.getMemberId())
                .setMemberEpoch(hb.memberEpoch())
                .setHeartbeatIntervalMs(hb.heartbeatIntervalMs());
        switch (hb.outcome()) {
            case OK -> {
                b.setError(ErrorCode.OK);
                hb.newAssignment().ifPresent(a -> b.setAssignment(toProtoAssignment(a)));
            }
            case UNKNOWN_MEMBER_ID -> b.setError(ErrorCode.UNKNOWN_MEMBER_ID);
            case FENCED_MEMBER_EPOCH -> b.setError(ErrorCode.FENCED_MEMBER_EPOCH);
        }
        return b.build();
    }

    /**
     * Three-way routing decision:
     * <ul>
     *   <li>{@link ErrorCode#OK} — this broker is the coordinator for the group;
     *       proceed with the request.</li>
     *   <li>{@link ErrorCode#COORDINATOR_NOT_AVAILABLE} — nobody is the
     *       coordinator yet (topic missing or no partition leader). Client
     *       should back off and retry.</li>
     *   <li>{@link ErrorCode#NOT_COORDINATOR} — coordinator exists but is
     *       another broker. Client should refresh via {@link #findCoordinator}
     *       and retry against the indicated host.</li>
     * </ul>
     */
    private ErrorCode coordinatorRoutingFor(String groupId) {
        var topicDesc = topicManager.describe(ConsumerOffsetsTopic.NAME);
        if (topicDesc.isEmpty()) return ErrorCode.COORDINATOR_NOT_AVAILABLE;
        int partitionCount = topicDesc.get().partitions();
        int partition = Math.floorMod(groupId.hashCode(), partitionCount);
        var ps = topicManager.partitionState(ConsumerOffsetsTopic.NAME, partition);
        if (ps.isEmpty() || ps.get().leader() <= 0) return ErrorCode.COORDINATOR_NOT_AVAILABLE;
        return ps.get().leader() == selfBrokerId ? ErrorCode.OK : ErrorCode.NOT_COORDINATOR;
    }

    private static Assignment toProtoAssignment(java.util.List<jbroker.proto.common.TopicPartition> partitions) {
        // Group by topic, preserve insertion order so equality checks in tests
        // are stable.
        var byTopic = new java.util.LinkedHashMap<String, java.util.List<Integer>>();
        for (var tp : partitions) {
            byTopic.computeIfAbsent(tp.getTopic(), k -> new java.util.ArrayList<>())
                    .add(tp.getPartition());
        }
        var b = Assignment.newBuilder();
        for (var entry : byTopic.entrySet()) {
            b.addAssignedPartitions(TopicPartitions.newBuilder()
                    .setTopic(entry.getKey())
                    .addAllPartitions(entry.getValue())
                    .build());
        }
        return b.build();
    }

    /**
     * Persist a batch of (tp, offset) commits for {@code group_id}. P7.6
     * implementation:
     * <ol>
     *   <li>Routing guard — same three-way as the heartbeat path.</li>
     *   <li>Membership validation — caller must own a current member_id +
     *       member_epoch (UNKNOWN_MEMBER_ID / FENCED_MEMBER_EPOCH applied
     *       per-tp).</li>
     *   <li>For each commit, encode an offset record into the group's
     *       coordinator partition of {@code __consumer_offsets} (one log
     *       append per request — multiple commits in one request batch
     *       together).</li>
     *   <li>Update {@link OffsetCache} so the next {@code FetchOffsets}
     *       reads the new value without a round-trip through the log.</li>
     * </ol>
     */
    public CommitOffsetsResponse commitOffsets(CommitOffsetsRequest req) {
        var b = CommitOffsetsResponse.newBuilder();
        if (groupCoordinator == null || offsetCache == null) {
            for (var commit : req.getCommitsList()) {
                b.addResults(CommitResult.newBuilder()
                        .setTp(commit.getTp())
                        .setError(ErrorCode.COORDINATOR_NOT_AVAILABLE)
                        .build());
            }
            return b.build();
        }
        var routing = coordinatorRoutingFor(req.getGroupId());
        if (routing != ErrorCode.OK) {
            for (var commit : req.getCommitsList()) {
                b.addResults(CommitResult.newBuilder()
                        .setTp(commit.getTp())
                        .setError(routing)
                        .build());
            }
            return b.build();
        }
        var membership = groupCoordinator.validateMember(
                req.getGroupId(), req.getMemberId(), req.getGenerationIdOrMemberEpoch());
        if (membership != GroupCoordinator.HeartbeatOutcome.OK) {
            var code = membership == GroupCoordinator.HeartbeatOutcome.UNKNOWN_MEMBER_ID
                    ? ErrorCode.UNKNOWN_MEMBER_ID
                    : ErrorCode.FENCED_MEMBER_EPOCH;
            for (var commit : req.getCommitsList()) {
                b.addResults(CommitResult.newBuilder()
                        .setTp(commit.getTp())
                        .setError(code)
                        .build());
            }
            return b.build();
        }

        long now = wallClockMillis.getAsLong();
        int coordinatorPartition = coordinatorPartitionFor(req.getGroupId());
        var records = new java.util.ArrayList<Record>(req.getCommitsCount());
        for (int i = 0; i < req.getCommitsCount(); i++) {
            var commit = req.getCommits(i);
            byte[] key = ConsumerOffsetsTopic.keyForOffset(
                    req.getGroupId(), commit.getTp().getTopic(), commit.getTp().getPartition());
            byte[] value = ConsumerOffsetsTopic.valueForOffset(
                    commit.getOffset(), commit.getLeaderEpoch(), commit.getMetadata(), now);
            records.add(new Record(/*offsetDelta*/ i, /*timestampDelta*/ 0L, key, value));
        }
        try {
            appendOffsetBatch(coordinatorPartition, records, now);
        } catch (IOException e) {
            for (var commit : req.getCommitsList()) {
                b.addResults(CommitResult.newBuilder()
                        .setTp(commit.getTp())
                        .setError(ErrorCode.UNKNOWN)
                        .build());
            }
            return b.build();
        }
        // Update cache + emit per-tp success.
        for (var commit : req.getCommitsList()) {
            offsetCache.put(
                    req.getGroupId(),
                    commit.getTp(),
                    new OffsetCache.OffsetAndMetadata(
                            commit.getOffset(), commit.getLeaderEpoch(), commit.getMetadata(), now));
            b.addResults(CommitResult.newBuilder()
                    .setTp(commit.getTp())
                    .setError(ErrorCode.OK)
                    .build());
        }
        return b.build();
    }

    public FetchOffsetsResponse fetchOffsets(FetchOffsetsRequest req) {
        var b = FetchOffsetsResponse.newBuilder();
        if (groupCoordinator == null || offsetCache == null) {
            for (var tp : req.getTpsList()) {
                b.addResults(OffsetFetchResult.newBuilder()
                        .setTp(tp)
                        .setOffset(-1L)
                        .setError(ErrorCode.OFFSET_OUT_OF_RANGE)
                        .build());
            }
            return b.build();
        }
        var routing = coordinatorRoutingFor(req.getGroupId());
        if (routing != ErrorCode.OK) {
            for (var tp : req.getTpsList()) {
                b.addResults(OffsetFetchResult.newBuilder()
                        .setTp(tp)
                        .setOffset(-1L)
                        .setError(routing)
                        .build());
            }
            return b.build();
        }
        for (var tp : req.getTpsList()) {
            var entry = offsetCache.get(req.getGroupId(), tp);
            if (entry.isEmpty()) {
                b.addResults(OffsetFetchResult.newBuilder()
                        .setTp(tp)
                        .setOffset(-1L)
                        .setError(ErrorCode.OFFSET_OUT_OF_RANGE)
                        .build());
            } else {
                var oam = entry.get();
                b.addResults(OffsetFetchResult.newBuilder()
                        .setTp(tp)
                        .setOffset(oam.offset())
                        .setLeaderEpoch(oam.leaderEpoch())
                        .setMetadata(oam.metadata())
                        .setError(ErrorCode.OK)
                        .build());
            }
        }
        return b.build();
    }

    private int coordinatorPartitionFor(String groupId) {
        var topicDesc = topicManager.describe(ConsumerOffsetsTopic.NAME).orElseThrow();
        return Math.floorMod(groupId.hashCode(), topicDesc.partitions());
    }

    private void appendOffsetBatch(int partition, List<Record> records, long nowMillis) throws IOException {
        var log = logManager.logFor(ConsumerOffsetsTopic.NAME, partition);
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        long baseOffset = log.nextOffset();
        RecordBatch.encode(
                buf,
                baseOffset,
                /*partitionLeaderEpoch*/ 0,
                /*firstTimestamp*/ nowMillis,
                /*maxTimestamp*/ nowMillis,
                /*producerId*/ -1L,
                /*producerEpoch*/ (short) -1,
                /*baseSequence*/ -1,
                records);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        log.appendRaw(bytes, baseOffset);
    }

    /**
     * P12.7 — admin-initiated consumer-group removal. Returns one of:
     * <ul>
     *   <li>{@code OK} — group existed and was removed from in-memory state.
     *       Offset commits in {@code __consumer_offsets} are left intact
     *       (sparse-offset compaction will eventually GC them if a new
     *       tombstone row arrives); they become invisible because the
     *       group is no longer listed.
     *   <li>{@code NOT_COORDINATOR} — self doesn't lead the group's
     *       coordinator partition.
     *   <li>{@code UNKNOWN_GROUP} — group isn't registered on this broker.
     * </ul>
     */
    public ErrorCode deleteConsumerGroupAdmin(String groupId) {
        if (groupCoordinator == null) return ErrorCode.COORDINATOR_NOT_AVAILABLE;
        var routing = coordinatorRoutingFor(groupId);
        if (routing != ErrorCode.OK) return routing;
        boolean removed = groupCoordinator.removeGroup(groupId);
        if (offsetCache != null) offsetCache.dropGroup(groupId);
        return removed ? ErrorCode.OK : ErrorCode.UNKNOWN_GROUP;
    }

    /**
     * P12.7 — admin-initiated offset reset. Skips the member-validation
     * step of {@link #commitOffsets} because admin operators aren't group
     * members. Returns one per-tp {@link ErrorCode} alongside the top-level
     * routing code.
     */
    public java.util.List<ErrorCode> resetConsumerGroupOffsetsAdmin(
            String groupId, java.util.List<jbroker.proto.broker.OffsetReset> resets, ErrorCode[] topLevelOut)
            throws IOException {
        if (groupCoordinator == null || offsetCache == null) {
            topLevelOut[0] = ErrorCode.COORDINATOR_NOT_AVAILABLE;
            return java.util.Collections.nCopies(resets.size(), ErrorCode.COORDINATOR_NOT_AVAILABLE);
        }
        var routing = coordinatorRoutingFor(groupId);
        if (routing != ErrorCode.OK) {
            topLevelOut[0] = routing;
            return java.util.Collections.nCopies(resets.size(), routing);
        }
        topLevelOut[0] = ErrorCode.OK;

        long now = wallClockMillis.getAsLong();
        int coordinatorPartition = coordinatorPartitionFor(groupId);
        var records = new java.util.ArrayList<Record>(resets.size());
        for (int i = 0; i < resets.size(); i++) {
            var r = resets.get(i);
            byte[] key = ConsumerOffsetsTopic.keyForOffset(
                    groupId, r.getTp().getTopic(), r.getTp().getPartition());
            // metadata intentionally empty — admin-driven resets are
            // operator actions, not member commits. "" distinguishes them
            // in downstream audits.
            byte[] value = ConsumerOffsetsTopic.valueForOffset(r.getOffset(), r.getLeaderEpoch(), "", now);
            records.add(new Record(i, 0L, key, value));
        }
        try {
            appendOffsetBatch(coordinatorPartition, records, now);
        } catch (IOException e) {
            var err = new java.util.ArrayList<ErrorCode>(resets.size());
            for (int i = 0; i < resets.size(); i++) err.add(ErrorCode.UNKNOWN);
            return err;
        }
        var result = new java.util.ArrayList<ErrorCode>(resets.size());
        for (var r : resets) {
            offsetCache.put(
                    groupId, r.getTp(), new OffsetCache.OffsetAndMetadata(r.getOffset(), r.getLeaderEpoch(), "", now));
            result.add(ErrorCode.OK);
        }
        return result;
    }

    public ListOffsetsResponse listOffsets(ListOffsetsRequest req) {
        var b = ListOffsetsResponse.newBuilder();
        for (var part : req.getPartitionsList()) {
            var tp = part.getTp();
            var topic = topicManager.describe(tp.getTopic());
            if (topic.isEmpty()) {
                b.addResults(ListOffsetsResult.newBuilder()
                        .setTp(tp)
                        .setError(ErrorCode.UNKNOWN)
                        .build());
                continue;
            }
            if (tp.getPartition() < 0 || tp.getPartition() >= topic.get().partitions()) {
                b.addResults(ListOffsetsResult.newBuilder()
                        .setTp(tp)
                        .setError(ErrorCode.UNKNOWN)
                        .build());
                continue;
            }
            try {
                long offset;
                long timestamp = part.getTimestamp();
                if (timestamp == -2) {
                    // Earliest. Log compaction is a Phase 9+ concern, so the
                    // log start offset is always 0 today.
                    offset = 0L;
                } else {
                    // -1 (latest) and any positive ts both fall back to LEO
                    // for now. Refining by-ts lookups requires a public
                    // search API on TimeIndex — left for a follow-up slice
                    // when a real client needs it.
                    offset = logManager.logFor(tp.getTopic(), tp.getPartition()).nextOffset();
                }
                b.addResults(ListOffsetsResult.newBuilder()
                        .setTp(tp)
                        .setError(ErrorCode.OK)
                        .setOffset(offset)
                        .setTimestamp(timestamp)
                        .build());
            } catch (IOException e) {
                b.addResults(ListOffsetsResult.newBuilder()
                        .setTp(tp)
                        .setError(ErrorCode.UNKNOWN)
                        .build());
            }
        }
        return b.build();
    }
}
