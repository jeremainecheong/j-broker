package jbroker.broker;

import java.util.ArrayList;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import jbroker.broker.auth.AclStore;
import jbroker.proto.broker.AclEntry;
import jbroker.proto.broker.CreateAclRequest;
import jbroker.proto.broker.CreateAclResponse;
import jbroker.proto.broker.CreateTopicRequest;
import jbroker.proto.broker.CreateTopicResponse;
import jbroker.proto.broker.DeleteAclRequest;
import jbroker.proto.broker.DeleteAclResponse;
import jbroker.proto.broker.DeleteTopicRequest;
import jbroker.proto.broker.DeleteTopicResponse;
import jbroker.proto.broker.DescribeTopicRequest;
import jbroker.proto.broker.DescribeTopicResponse;
import jbroker.proto.broker.ForceCompactPartitionRequest;
import jbroker.proto.broker.ForceCompactPartitionResponse;
import jbroker.proto.broker.ListAclsRequest;
import jbroker.proto.broker.ListAclsResponse;
import jbroker.proto.broker.ListTopicsRequest;
import jbroker.proto.broker.ListTopicsResponse;
import jbroker.proto.broker.UpdateTopicConfigRequest;
import jbroker.proto.broker.UpdateTopicConfigResponse;
import jbroker.proto.raft.AclRecord;
import jbroker.proto.raft.CreateTopicRecord;
import jbroker.proto.raft.DeleteTopicRecord;
import jbroker.proto.raft.MetadataRecord;
import jbroker.proto.raft.PartitionChangeRecord;
import jbroker.proto.raft.RemoveAclRecord;
import jbroker.proto.raft.TopicRecord;
import jbroker.proto.raft.UpdateTopicConfigRecord;

/**
 * Routes {@code Admin} RPCs. {@code CreateTopic}/{@code DeleteTopic}/
 * {@code UpdateTopicConfig} propose a {@link MetadataRecord} through
 * {@link MetadataProposer}; {@code ListTopics} and {@code DescribeTopic}
 * read from {@link TopicManager}.
 */
public final class AdminHandler {

    public interface MetadataProposer {
        /**
         * Propose a metadata record; blocks until it has been applied. Throws
         * on failure (e.g., not leader, timeout).
         */
        void proposeAndWait(byte[] payload, long timeoutMillis) throws Exception;
    }

    /** Resolves the current Raft leader id for NOT_LEADER hint population. */
    @FunctionalInterface
    public interface LeaderIdLookup {
        Optional<Integer> currentLeaderId();
    }

    /**
     * Abstracts the LogManager handle so AdminHandler can trigger a
     * synchronous compaction without depending on {@code broker-storage}
     * directly. Returns {@link OptionalInt#empty()} when the target log
     * isn't open on this broker (non-hosting replica, or cold handle).
     */
    @FunctionalInterface
    public interface Compactor {
        OptionalInt compactLogNowIfPresent(String topic, int partition) throws java.io.IOException;
    }

    private final TopicManager topicManager;
    private final MetadataProposer proposer;
    private final int selfBrokerId;
    private final Supplier<Set<Integer>> knownBrokers;
    private final LeaderIdLookup leaderLookup;
    private final BrokerRegistry addressBook;
    private final Compactor compactor;

    /**
     * Replicated ACL cache, set by the broker after the metadata state
     * machine is built (it owns the instance). Null only in test harnesses
     * that never exercise the ACL RPCs; {@code listAcls} then answers empty.
     */
    private AclStore aclStore;

    public void setAclStore(AclStore store) {
        this.aclStore = store;
    }

    public AdminHandler(
            TopicManager topicManager,
            MetadataProposer proposer,
            int selfBrokerId,
            Supplier<Set<Integer>> knownBrokers,
            LeaderIdLookup leaderLookup,
            BrokerRegistry addressBook,
            Compactor compactor) {
        this.topicManager = topicManager;
        this.proposer = proposer;
        this.selfBrokerId = selfBrokerId;
        this.knownBrokers = knownBrokers;
        this.leaderLookup = leaderLookup;
        this.addressBook = addressBook;
        this.compactor = compactor;
    }

    /** Back-compat overload for pre-existing call sites: no compactor wired. */
    public AdminHandler(
            TopicManager topicManager,
            MetadataProposer proposer,
            int selfBrokerId,
            Supplier<Set<Integer>> knownBrokers,
            LeaderIdLookup leaderLookup,
            BrokerRegistry addressBook) {
        this(topicManager, proposer, selfBrokerId, knownBrokers, leaderLookup, addressBook, null);
    }

    /** Back-compat overload: no leader hint / address book (tests only). */
    public AdminHandler(
            TopicManager topicManager,
            MetadataProposer proposer,
            int selfBrokerId,
            Supplier<Set<Integer>> knownBrokers) {
        this(topicManager, proposer, selfBrokerId, knownBrokers, Optional::empty, new BrokerRegistry());
    }

    /**
     * Back-compat overload: self-only replica set. Used by tests
     * that don't spin up a BrokerRegistry.
     */
    public AdminHandler(TopicManager topicManager, MetadataProposer proposer, int selfBrokerId) {
        this(topicManager, proposer, selfBrokerId, () -> Set.of(selfBrokerId));
    }

    public CreateTopicResponse createTopic(CreateTopicRequest req) {
        if (topicManager.exists(req.getTopic())) {
            return CreateTopicResponse.newBuilder()
                    .setError(buildError(ErrorCodes.TOPIC_ALREADY_EXISTS, "topic already exists: " + req.getTopic()))
                    .build();
        }
        // Pick up to replicationFactor brokers from the known set (self
        // always included so the controller leads its own partitions).
        // If fewer brokers are registered than requested, clamp quietly —
        // existing single-broker tests request rf=3 against a one-broker
        // cluster and expect the topic to be created as a single replica.
        var candidates = new ArrayList<Integer>();
        candidates.add(selfBrokerId);
        for (int b : knownBrokers.get()) {
            if (b != selfBrokerId) candidates.add(b);
        }
        int rf = Math.min(req.getReplicationFactor(), candidates.size());
        // Validate against the CLAMPED rf — that is the replica count the
        // topic actually gets, so it is the bound min.insync.replicas must
        // respect (a single-broker cluster creating rf=3 really has rf=1).
        var configError = validateTopicConfig(req.getConfigMap(), rf);
        if (configError != null) {
            return CreateTopicResponse.newBuilder()
                    .setError(buildError(ErrorCodes.INVALID_CONFIG, configError))
                    .build();
        }
        var replicas = candidates.subList(0, rf);
        boolean internal = req.getTopic().startsWith("__");
        var topicBuilder = TopicRecord.newBuilder()
                .setTopic(req.getTopic())
                .setPartitions(req.getPartitions())
                .setReplicationFactor(rf)
                .setCreatedMillis(System.currentTimeMillis())
                .setInternal(internal)
                .setCompact(internal);
        topicBuilder.putAllConfig(req.getConfigMap());
        var ct = CreateTopicRecord.newBuilder().setTopic(topicBuilder.build());
        for (int p = 0; p < req.getPartitions(); p++) {
            var pc = PartitionChangeRecord.newBuilder()
                    .setTopic(req.getTopic())
                    .setPartition(p)
                    .setLeader(selfBrokerId)
                    .setLeaderEpoch(0);
            for (int r : replicas) {
                pc.addIsr(r);
                pc.addReplicas(r);
            }
            ct.addPartitionChanges(pc);
        }
        var record = MetadataRecord.newBuilder().setCreateTopic(ct.build()).build();
        var notLeader = requireLeadership();
        if (notLeader.isPresent()) {
            return CreateTopicResponse.newBuilder().setError(notLeader.get()).build();
        }
        try {
            proposer.proposeAndWait(record.toByteArray(), TimeUnit.SECONDS.toMillis(5));
        } catch (Exception e) {
            return CreateTopicResponse.newBuilder().setError(notLeaderError(e)).build();
        }
        return CreateTopicResponse.newBuilder().build();
    }

    public DeleteTopicResponse deleteTopic(DeleteTopicRequest req) {
        if (!topicManager.exists(req.getTopic())) {
            return DeleteTopicResponse.newBuilder()
                    .setError(buildError(ErrorCodes.UNKNOWN_TOPIC, "unknown topic: " + req.getTopic()))
                    .build();
        }
        var record = MetadataRecord.newBuilder()
                .setDeleteTopic(
                        DeleteTopicRecord.newBuilder().setTopic(req.getTopic()).build())
                .build();
        var notLeader = requireLeadership();
        if (notLeader.isPresent()) {
            return DeleteTopicResponse.newBuilder().setError(notLeader.get()).build();
        }
        try {
            proposer.proposeAndWait(record.toByteArray(), TimeUnit.SECONDS.toMillis(5));
        } catch (Exception e) {
            return DeleteTopicResponse.newBuilder().setError(notLeaderError(e)).build();
        }
        return DeleteTopicResponse.newBuilder().build();
    }

    public UpdateTopicConfigResponse updateTopicConfig(UpdateTopicConfigRequest req) {
        var existing = topicManager.describe(req.getTopic());
        if (existing.isEmpty()) {
            return UpdateTopicConfigResponse.newBuilder()
                    .setError(buildError(ErrorCodes.UNKNOWN_TOPIC, "unknown topic: " + req.getTopic()))
                    .build();
        }
        var configError = validateTopicConfig(req.getConfigMap(), existing.get().replicationFactor());
        if (configError != null) {
            return UpdateTopicConfigResponse.newBuilder()
                    .setError(buildError(ErrorCodes.INVALID_CONFIG, configError))
                    .build();
        }
        var record = MetadataRecord.newBuilder()
                .setUpdateTopicConfig(UpdateTopicConfigRecord.newBuilder()
                        .setTopic(req.getTopic())
                        .putAllConfig(req.getConfigMap())
                        .build())
                .build();
        var notLeader = requireLeadership();
        if (notLeader.isPresent()) {
            return UpdateTopicConfigResponse.newBuilder()
                    .setError(notLeader.get())
                    .build();
        }
        try {
            proposer.proposeAndWait(record.toByteArray(), TimeUnit.SECONDS.toMillis(5));
        } catch (Exception e) {
            return UpdateTopicConfigResponse.newBuilder()
                    .setError(notLeaderError(e))
                    .build();
        }
        var merged = topicManager
                .describe(req.getTopic())
                .map(TopicDescription::config)
                .orElse(java.util.Map.of());
        return UpdateTopicConfigResponse.newBuilder().putAllConfig(merged).build();
    }

    /**
     * Validates recognized topic-config keys before they are committed to
     * the metadata log. Unrecognized keys pass through untouched — the
     * config map is deliberately open (the admin UI displays arbitrary
     * entries). Returns an error message, or {@code null} when valid.
     *
     * <p>{@code min.insync.replicas} must be an integer in
     * {@code [1, replicationFactor]}: a floor above the replica count can
     * never be satisfied and would brick every acks=all produce on the
     * topic ({@link ProduceHandler} enforces the floor).
     */
    private static String validateTopicConfig(java.util.Map<String, String> config, int replicationFactor) {
        var minIsr = validateIntConfig(
                config,
                TopicDescription.MIN_INSYNC_REPLICAS_CONFIG,
                1,
                replicationFactor,
                "the replication factor (" + replicationFactor + ")");
        if (minIsr != null) return minIsr;
        var maxMessage = validateIntConfig(
                config,
                TopicDescription.MAX_MESSAGE_BYTES_CONFIG,
                1,
                TopicDescription.MAX_MESSAGE_BYTES_HARD_CAP,
                "the hard cap (" + TopicDescription.MAX_MESSAGE_BYTES_HARD_CAP
                        + " bytes — gRPC frame limits bound every hop)");
        if (maxMessage != null) return maxMessage;
        var retentionMs = validateMinusOneOrPositive(config, TopicDescription.RETENTION_MS_CONFIG);
        if (retentionMs != null) return retentionMs;
        var retentionBytes = validateMinusOneOrPositive(config, TopicDescription.RETENTION_BYTES_CONFIG);
        if (retentionBytes != null) return retentionBytes;
        var flushMessages = validateMinusOneOrPositive(config, TopicDescription.FLUSH_MESSAGES_CONFIG);
        if (flushMessages != null) return flushMessages;
        var flushMs = validateMinusOneOrPositive(config, TopicDescription.FLUSH_MS_CONFIG);
        if (flushMs != null) return flushMs;
        return validateLongConfig(
                config,
                TopicDescription.SEGMENT_BYTES_CONFIG,
                TopicDescription.SEGMENT_BYTES_FLOOR,
                TopicDescription.SEGMENT_BYTES_HARD_CAP,
                "the hard cap (" + TopicDescription.SEGMENT_BYTES_HARD_CAP
                        + " bytes — segment file positions are int-indexed)");
    }

    /** Retention limits and flush triggers accept -1 (unlimited / off) or a positive value. */
    private static String validateMinusOneOrPositive(java.util.Map<String, String> config, String key) {
        var raw = config.get(key);
        if (raw == null) return null;
        long value;
        try {
            value = Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            return key + " must be an integer, got: " + raw;
        }
        if (value != -1 && value < 1) {
            return key + " must be -1 (unlimited) or ≥ 1, got: " + value;
        }
        return null;
    }

    /** Bounds-check one long config key. Returns an error message, or {@code null} when absent/valid. */
    private static String validateLongConfig(
            java.util.Map<String, String> config, String key, long min, long max, String maxDescription) {
        var raw = config.get(key);
        if (raw == null) return null;
        long value;
        try {
            value = Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            return key + " must be an integer, got: " + raw;
        }
        if (value < min) {
            return key + " must be ≥ " + min + ", got: " + value;
        }
        if (value > max) {
            return key + " (" + value + ") must not exceed " + maxDescription;
        }
        return null;
    }

    /** Bounds-check one integer config key. Returns an error message, or {@code null} when absent/valid. */
    private static String validateIntConfig(
            java.util.Map<String, String> config, String key, int min, int max, String maxDescription) {
        var raw = config.get(key);
        if (raw == null) return null;
        int value;
        try {
            value = Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            return key + " must be an integer, got: " + raw;
        }
        if (value < min) {
            return key + " must be ≥ " + min + ", got: " + value;
        }
        if (value > max) {
            return key + " (" + value + ") must not exceed " + maxDescription;
        }
        return null;
    }

    public ListTopicsResponse listTopics(ListTopicsRequest req) {
        var b = ListTopicsResponse.newBuilder();
        for (var t : topicManager.list()) {
            b.addTopics(toDescriptionProto(t));
        }
        return b.build();
    }

    /**
     * Synchronously compact this broker's local log for
     * {@code (topic, partition)}. Topic-level validations (unknown topic,
     * out-of-range partition) return a populated {@code error}; a missing
     * local log (non-hosting broker) returns {@code records_kept = -1}
     * with no error so the admin-app can fan out safely.
     */
    public ForceCompactPartitionResponse forceCompactPartition(ForceCompactPartitionRequest req) {
        var b = ForceCompactPartitionResponse.newBuilder().setRecordsKept(-1);
        if (compactor == null) {
            return b.setError(buildError(ErrorCodes.UNIMPLEMENTED, "compactor not wired"))
                    .build();
        }
        var desc = topicManager.describe(req.getTopic());
        if (desc.isEmpty()) {
            return b.setError(buildError(ErrorCodes.UNKNOWN_TOPIC, "unknown topic: " + req.getTopic()))
                    .build();
        }
        int partitions = desc.get().partitions();
        if (req.getPartition() < 0 || req.getPartition() >= partitions) {
            return b.setError(buildError(
                            ErrorCodes.INVALID_PARTITION,
                            "partition out of range: " + req.getPartition() + " (topic has " + partitions + ")"))
                    .build();
        }
        try {
            var kept = compactor.compactLogNowIfPresent(req.getTopic(), req.getPartition());
            return b.setRecordsKept(kept.orElse(-1)).build();
        } catch (java.io.IOException e) {
            return b.setError(buildError(ErrorCodes.IO_ERROR, e.getMessage() == null ? e.toString() : e.getMessage()))
                    .build();
        }
    }

    private static final Set<String> ACL_RESOURCE_TYPES = Set.of("topic", "group", "cluster");
    private static final Set<String> ACL_OPERATIONS = Set.of("produce", "consume", "admin", "*");

    public CreateAclResponse createAcl(CreateAclRequest req) {
        var e = req.getEntry();
        var problem = validateAclEntry(e);
        if (problem != null) {
            return CreateAclResponse.newBuilder()
                    .setError(buildError(ErrorCodes.INVALID_CONFIG, problem))
                    .build();
        }
        var record = MetadataRecord.newBuilder()
                .setAcl(AclRecord.newBuilder()
                        .setPrincipal(e.getPrincipal())
                        .setResourceType(e.getResourceType())
                        .setResourceName(e.getResourceName())
                        .setPrefix(e.getPrefix())
                        .setOperation(e.getOperation())
                        .setAllow(e.getAllow()))
                .build();
        var notLeader = requireLeadership();
        if (notLeader.isPresent()) {
            return CreateAclResponse.newBuilder().setError(notLeader.get()).build();
        }
        try {
            proposer.proposeAndWait(record.toByteArray(), TimeUnit.SECONDS.toMillis(5));
        } catch (Exception ex) {
            return CreateAclResponse.newBuilder().setError(notLeaderError(ex)).build();
        }
        return CreateAclResponse.newBuilder().build();
    }

    public DeleteAclResponse deleteAcl(DeleteAclRequest req) {
        var e = req.getEntry();
        var problem = validateAclEntry(e);
        if (problem != null) {
            return DeleteAclResponse.newBuilder()
                    .setError(buildError(ErrorCodes.INVALID_CONFIG, problem))
                    .build();
        }
        var record = MetadataRecord.newBuilder()
                .setRemoveAcl(RemoveAclRecord.newBuilder()
                        .setPrincipal(e.getPrincipal())
                        .setResourceType(e.getResourceType())
                        .setResourceName(e.getResourceName())
                        .setPrefix(e.getPrefix())
                        .setOperation(e.getOperation()))
                .build();
        var notLeader = requireLeadership();
        if (notLeader.isPresent()) {
            return DeleteAclResponse.newBuilder().setError(notLeader.get()).build();
        }
        try {
            proposer.proposeAndWait(record.toByteArray(), TimeUnit.SECONDS.toMillis(5));
        } catch (Exception ex) {
            return DeleteAclResponse.newBuilder().setError(notLeaderError(ex)).build();
        }
        return DeleteAclResponse.newBuilder().build();
    }

    public ListAclsResponse listAcls(ListAclsRequest req) {
        var b = ListAclsResponse.newBuilder();
        if (aclStore == null) return b.build();
        for (var a : aclStore.list()) {
            b.addEntries(AclEntry.newBuilder()
                    .setPrincipal(a.principal())
                    .setResourceType(a.resourceType())
                    .setResourceName(a.resourceName())
                    .setPrefix(a.prefix())
                    .setOperation(a.operation())
                    .setAllow(a.allow()));
        }
        return b.build();
    }

    private static String validateAclEntry(AclEntry e) {
        if (e.getPrincipal().isBlank()) return "acl principal must be non-blank";
        if (!ACL_RESOURCE_TYPES.contains(e.getResourceType())) {
            return "acl resource_type must be one of " + ACL_RESOURCE_TYPES + ", got: " + e.getResourceType();
        }
        if (e.getResourceName().isBlank()) return "acl resource_name must be non-blank";
        if (!ACL_OPERATIONS.contains(e.getOperation())) {
            return "acl operation must be one of " + ACL_OPERATIONS + ", got: " + e.getOperation();
        }
        return null;
    }

    public DescribeTopicResponse describeTopic(DescribeTopicRequest req) {
        var desc = topicManager.describe(req.getTopic());
        if (desc.isEmpty()) {
            return DescribeTopicResponse.newBuilder()
                    .setError(buildError(ErrorCodes.UNKNOWN_TOPIC, "unknown topic: " + req.getTopic()))
                    .build();
        }
        return DescribeTopicResponse.newBuilder()
                .setTopic(toDescriptionProto(desc.get()))
                .build();
    }

    private jbroker.proto.broker.TopicDescription toDescriptionProto(TopicDescription t) {
        return jbroker.proto.broker.TopicDescription.newBuilder()
                .setTopic(t.topic())
                .setPartitions(t.partitions())
                .setReplicationFactor(t.replicationFactor())
                .setCreatedMillis(t.createdMillis())
                .setInternal(t.internal())
                .setCompact(t.compact())
                .putAllConfig(t.config())
                .build();
    }

    private jbroker.proto.broker.Error buildError(int code, String message) {
        return jbroker.proto.broker.Error.newBuilder()
                .setCode(code)
                .setMessage(message)
                .build();
    }

    /**
     * Convert a propose-failure into a NOT_LEADER error envelope with
     * best-effort suggested_leader_* hints. When the proposer fails because
     * self is not the Raft leader, the admin-app should retry against the
     * hinted broker (if known).
     */

    /**
     * A Raft follower's propose is silently dropped (fire-and-forget in
     * RaftDriver), so {@code proposeAndWait} would burn its full timeout
     * before surfacing NOT_LEADER — a ~5s cliff on every admin mutation
     * routed to a non-leader (found by the k6 admin-api smoke). The
     * handler already knows the current leader; fail fast with the same
     * hint envelope. Empty leader (election window) also fails fast —
     * callers/pools retry.
     */
    private Optional<jbroker.proto.broker.Error> requireLeadership() {
        var leader = leaderLookup.currentLeaderId();
        if (leader.isPresent() && leader.get() == selfBrokerId) return Optional.empty();
        var b = jbroker.proto.broker.Error.newBuilder()
                .setCode(ErrorCodes.NOT_LEADER)
                .setMessage(
                        leader.isPresent()
                                ? "not the controller; leader is broker " + leader.get()
                                : "no Raft leader elected yet");
        leader.ifPresent(leaderId -> {
            b.putHint("suggested_leader_id", Integer.toString(leaderId));
            addressBook.addressFor(leaderId).ifPresent(hp -> {
                b.putHint("suggested_leader_host", hp.host());
                b.putHint("suggested_leader_port", Integer.toString(hp.port()));
            });
        });
        return Optional.of(b.build());
    }

    private jbroker.proto.broker.Error notLeaderError(Exception cause) {
        var b = jbroker.proto.broker.Error.newBuilder()
                .setCode(ErrorCodes.NOT_LEADER)
                .setMessage(cause.getMessage() == null ? cause.toString() : cause.getMessage());
        leaderLookup.currentLeaderId().ifPresent(leaderId -> {
            b.putHint("suggested_leader_id", Integer.toString(leaderId));
            addressBook.addressFor(leaderId).ifPresent(hp -> {
                b.putHint("suggested_leader_host", hp.host());
                b.putHint("suggested_leader_port", Integer.toString(hp.port()));
            });
        });
        return b.build();
    }
}
