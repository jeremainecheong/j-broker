package jbroker.broker;

import java.util.ArrayList;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import jbroker.proto.broker.CreateTopicRequest;
import jbroker.proto.broker.CreateTopicResponse;
import jbroker.proto.broker.DeleteTopicRequest;
import jbroker.proto.broker.DeleteTopicResponse;
import jbroker.proto.broker.DescribeTopicRequest;
import jbroker.proto.broker.DescribeTopicResponse;
import jbroker.proto.broker.ForceCompactPartitionRequest;
import jbroker.proto.broker.ForceCompactPartitionResponse;
import jbroker.proto.broker.ListTopicsRequest;
import jbroker.proto.broker.ListTopicsResponse;
import jbroker.proto.broker.UpdateTopicConfigRequest;
import jbroker.proto.broker.UpdateTopicConfigResponse;
import jbroker.proto.raft.CreateTopicRecord;
import jbroker.proto.raft.DeleteTopicRecord;
import jbroker.proto.raft.MetadataRecord;
import jbroker.proto.raft.PartitionChangeRecord;
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
     * P13.1 — abstracts the LogManager handle so AdminHandler can trigger a
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

    /** P13.1 pre-existing-call-sites overload: no compactor wired. */
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
     * Back-compat overload: self-only replica set. Used by Phase 5 tests
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
        var record = MetadataRecord.newBuilder()
                .setUpdateTopicConfig(UpdateTopicConfigRecord.newBuilder()
                        .setTopic(req.getTopic())
                        .putAllConfig(req.getConfigMap())
                        .build())
                .build();
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

    public ListTopicsResponse listTopics(ListTopicsRequest req) {
        var b = ListTopicsResponse.newBuilder();
        for (var t : topicManager.list()) {
            b.addTopics(toDescriptionProto(t));
        }
        return b.build();
    }

    /**
     * P13.1 — synchronously compact this broker's local log for
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
