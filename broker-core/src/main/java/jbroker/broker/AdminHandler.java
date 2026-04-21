package jbroker.broker;

import java.util.concurrent.TimeUnit;
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

    public AdminHandler(TopicManager topicManager, MetadataProposer proposer, int selfBrokerId) {
        this.topicManager = topicManager;
        this.proposer = proposer;
        this.selfBrokerId = selfBrokerId;
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
        // Single-broker : self is controller and the sole replica of
        // every partition. Bundle the TopicRecord and per-partition leader
        // assignments into one CreateTopicRecord so both halves commit in the
        // same state-machine transition — no half-initialised topic window.
        // Multi-broker Milestone 6+ will keep the same atomic shape with the
        // controller picking replicas per partition.
        var ct = CreateTopicRecord.newBuilder()
                .setTopic(TopicRecord.newBuilder()
                        .setTopic(req.getTopic())
                        .setPartitions(req.getPartitions())
                        .setReplicationFactor(req.getReplicationFactor())
                        .setCreatedMillis(System.currentTimeMillis())
                        .build());
        for (int p = 0; p < req.getPartitions(); p++) {
            ct.addPartitionChanges(PartitionChangeRecord.newBuilder()
                    .setTopic(req.getTopic())
                    .setPartition(p)
                    .setLeader(selfBrokerId)
                    .addIsr(selfBrokerId)
                    .addReplicas(selfBrokerId)
                    .setLeaderEpoch(0)
                    .build());
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
