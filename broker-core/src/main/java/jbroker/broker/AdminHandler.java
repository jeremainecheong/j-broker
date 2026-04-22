package jbroker.broker;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import jbroker.proto.broker.CreateTopicRequest;
import jbroker.proto.broker.CreateTopicResponse;
import jbroker.proto.broker.DescribeTopicRequest;
import jbroker.proto.broker.DescribeTopicResponse;
import jbroker.proto.broker.ListTopicsRequest;
import jbroker.proto.broker.ListTopicsResponse;
import jbroker.proto.raft.CreateTopicRecord;
import jbroker.proto.raft.MetadataRecord;
import jbroker.proto.raft.PartitionChangeRecord;
import jbroker.proto.raft.TopicRecord;

/**
 * Routes {@code Admin} RPCs. {@code CreateTopic} proposes a
 * {@link MetadataRecord} through {@link MetadataProposer}; {@code ListTopics}
 * and {@code DescribeTopic} read from {@link TopicManager}.
 */
public final class AdminHandler {

    public interface MetadataProposer {
        /**
         * Propose a metadata record; blocks until it has been applied. Throws
         * on failure (e.g., not leader, timeout).
         */
        void proposeAndWait(byte[] payload, long timeoutMillis) throws Exception;
    }

    private final TopicManager topicManager;
    private final MetadataProposer proposer;
    private final int selfBrokerId;
    private final Supplier<Set<Integer>> knownBrokers;

    public AdminHandler(
            TopicManager topicManager,
            MetadataProposer proposer,
            int selfBrokerId,
            Supplier<Set<Integer>> knownBrokers) {
        this.topicManager = topicManager;
        this.proposer = proposer;
        this.selfBrokerId = selfBrokerId;
        this.knownBrokers = knownBrokers;
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
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.TOPIC_ALREADY_EXISTS)
                            .setMessage("topic already exists: " + req.getTopic())
                            .build())
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
        // Bundle the TopicRecord and per-partition leader assignments into
        // a single CreateTopicRecord so both halves commit in one
        // state-machine transition — no half-initialised topic window.
        // Clamp TopicRecord.replicationFactor to the actual replicas count
        // so downstream readers (DescribeTopic, future ISR/fencer checks)
        // see a self-consistent record rather than rf=3 / replicas=[self].
        // P7.2: topics whose name starts with "__" are auto-marked internal
        // (they're hidden from Admin.ListTopics by TopicManager#list).
        boolean internal = req.getTopic().startsWith("__");
        var ct = CreateTopicRecord.newBuilder()
                .setTopic(TopicRecord.newBuilder()
                        .setTopic(req.getTopic())
                        .setPartitions(req.getPartitions())
                        .setReplicationFactor(rf)
                        .setCreatedMillis(System.currentTimeMillis())
                        .setInternal(internal)
                        .setCompact(internal) // first internal topic, __consumer_offsets, is compacted
                        .build());
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
            return CreateTopicResponse.newBuilder()
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.NOT_LEADER)
                            .setMessage(e.getMessage() == null ? e.toString() : e.getMessage())
                            .build())
                    .build();
        }
        return CreateTopicResponse.newBuilder().build();
    }

    public ListTopicsResponse listTopics(ListTopicsRequest req) {
        var b = ListTopicsResponse.newBuilder();
        for (var t : topicManager.list()) {
            b.addTopics(jbroker.proto.broker.TopicDescription.newBuilder()
                    .setTopic(t.topic())
                    .setPartitions(t.partitions())
                    .setReplicationFactor(t.replicationFactor())
                    .setCreatedMillis(t.createdMillis())
                    .build());
        }
        return b.build();
    }

    public DescribeTopicResponse describeTopic(DescribeTopicRequest req) {
        var desc = topicManager.describe(req.getTopic());
        if (desc.isEmpty()) {
            return DescribeTopicResponse.newBuilder()
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.UNKNOWN_TOPIC)
                            .setMessage("unknown topic: " + req.getTopic())
                            .build())
                    .build();
        }
        return DescribeTopicResponse.newBuilder()
                .setTopic(jbroker.proto.broker.TopicDescription.newBuilder()
                        .setTopic(desc.get().topic())
                        .setPartitions(desc.get().partitions())
                        .setReplicationFactor(desc.get().replicationFactor())
                        .setCreatedMillis(desc.get().createdMillis())
                        .build())
                .build();
    }
}
