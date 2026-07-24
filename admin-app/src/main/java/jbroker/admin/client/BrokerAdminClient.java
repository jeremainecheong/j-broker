package jbroker.admin.client;

import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import jbroker.proto.broker.AdminGrpc;
import jbroker.proto.broker.CreateTopicRequest;
import jbroker.proto.broker.CreateTopicResponse;
import jbroker.proto.broker.DeleteTopicRequest;
import jbroker.proto.broker.DeleteTopicResponse;
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
import jbroker.proto.broker.ListTopicsRequest;
import jbroker.proto.broker.ListTopicsResponse;
import jbroker.proto.broker.MetadataGrpc;
import jbroker.proto.broker.UpdateTopicConfigRequest;
import jbroker.proto.broker.UpdateTopicConfigResponse;
import jbroker.tls.TlsConfig;
import jbroker.tls.TlsContexts;

/**
 * Thin gRPC wrapper the admin-app uses to talk to a single broker. One
 * instance per configured broker; callers that need cluster-wide fan-out
 * iterate a {@link BrokerAdminClientPool}.
 *
 * <p>Carries {@code Admin} + {@code Metadata} stubs; later
 * slices add {@code Consumer} for consumer-group reads and the streaming
 * events RPC.
 */
public final class BrokerAdminClient implements AutoCloseable {

    private final String address;
    private final ManagedChannel channel;
    private final MetadataGrpc.MetadataBlockingStub metadata;
    private final AdminGrpc.AdminBlockingStub admin;

    public BrokerAdminClient(String host, int port) {
        this(host, port, TlsConfig.DISABLED);
    }

    /** TLS-aware constructor. Admin-app→broker shares the cluster TLS bundle. */
    public BrokerAdminClient(String host, int port, TlsConfig tls) {
        this.address = host + ":" + port;
        var b = NettyChannelBuilder.forAddress(host, port);
        try {
            var sslCtx = TlsContexts.clientContext(tls == null ? TlsConfig.DISABLED : tls);
            if (sslCtx == null) {
                b.usePlaintext();
            } else {
                b.sslContext(sslCtx);
            }
        } catch (javax.net.ssl.SSLException e) {
            throw new IllegalStateException("TLS client context build failed", e);
        }
        this.channel = b.build();
        this.metadata = MetadataGrpc.newBlockingStub(channel);
        this.admin = AdminGrpc.newBlockingStub(channel);
    }

    public static BrokerAdminClient parse(String hostPort) {
        return parse(hostPort, TlsConfig.DISABLED);
    }

    /** Parse helper that threads a TLS bundle into the created client. */
    public static BrokerAdminClient parse(String hostPort, TlsConfig tls) {
        int colon = hostPort.indexOf(':');
        if (colon < 0) throw new IllegalArgumentException("broker address needs host:port, got " + hostPort);
        return new BrokerAdminClient(
                hostPort.substring(0, colon), Integer.parseInt(hostPort.substring(colon + 1)), tls);
    }

    public String address() {
        return address;
    }

    /**
     * Issues a {@code DescribeCluster} RPC with a short deadline. gRPC status
     * errors (unreachable broker, deadline) bubble up as
     * {@link StatusRuntimeException}; callers that fan out across brokers are
     * expected to swallow them and move on to the next endpoint.
     */
    public DescribeClusterResponse describeCluster() {
        return metadata.withDeadlineAfter(3, TimeUnit.SECONDS)
                .describeCluster(DescribeClusterRequest.newBuilder().build());
    }

    public ListTopicsResponse listTopics() {
        return admin.withDeadlineAfter(3, TimeUnit.SECONDS)
                .listTopics(ListTopicsRequest.newBuilder().build());
    }

    public DescribeTopicPartitionsResponse describeTopicPartitions(String topic) {
        return metadata.withDeadlineAfter(3, TimeUnit.SECONDS)
                .describeTopicPartitions(DescribeTopicPartitionsRequest.newBuilder()
                        .setTopic(topic)
                        .build());
    }

    public CreateTopicResponse createTopic(String topic, int partitions, int rf, Map<String, String> config) {
        var b = CreateTopicRequest.newBuilder()
                .setTopic(topic)
                .setPartitions(partitions)
                .setReplicationFactor(rf);
        b.putAllConfig(config);
        return admin.withDeadlineAfter(5, TimeUnit.SECONDS).createTopic(b.build());
    }

    public DeleteTopicResponse deleteTopic(String topic) {
        return admin.withDeadlineAfter(5, TimeUnit.SECONDS)
                .deleteTopic(DeleteTopicRequest.newBuilder().setTopic(topic).build());
    }

    public ListConsumerGroupsResponse listConsumerGroups() {
        return metadata.withDeadlineAfter(3, TimeUnit.SECONDS)
                .listConsumerGroups(ListConsumerGroupsRequest.newBuilder().build());
    }

    public DescribeConsumerGroupResponse describeConsumerGroup(String groupId) {
        return metadata.withDeadlineAfter(3, TimeUnit.SECONDS)
                .describeConsumerGroup(DescribeConsumerGroupRequest.newBuilder()
                        .setGroupId(groupId)
                        .build());
    }

    public DescribeRaftResponse describeRaft() {
        return metadata.withDeadlineAfter(3, TimeUnit.SECONDS)
                .describeRaft(DescribeRaftRequest.newBuilder().build());
    }

    public DescribeMetricsResponse describeMetrics() {
        return metadata.withDeadlineAfter(3, TimeUnit.SECONDS)
                .describeMetrics(DescribeMetricsRequest.newBuilder().build());
    }

    public UpdateTopicConfigResponse updateTopicConfig(String topic, Map<String, String> overlay) {
        var b = UpdateTopicConfigRequest.newBuilder().setTopic(topic);
        b.putAllConfig(overlay);
        return admin.withDeadlineAfter(5, TimeUnit.SECONDS).updateTopicConfig(b.build());
    }

    /**
     * Synchronous compaction on this broker's local log for
     * {@code (topic, partition)}. Non-hosting brokers return
     * {@code records_kept = -1}; the admin-app fans out across brokers and
     * sums survivor counts from hosting replicas.
     */
    public jbroker.proto.broker.ForceCompactPartitionResponse forceCompactPartition(String topic, int partition) {
        return admin.withDeadlineAfter(10, TimeUnit.SECONDS)
                .forceCompactPartition(jbroker.proto.broker.ForceCompactPartitionRequest.newBuilder()
                        .setTopic(topic)
                        .setPartition(partition)
                        .build());
    }

    /** Admin-initiated consumer-group removal. */
    public jbroker.proto.broker.DeleteConsumerGroupResponse deleteConsumerGroup(String groupId) {
        return admin.withDeadlineAfter(5, TimeUnit.SECONDS)
                .deleteConsumerGroup(jbroker.proto.broker.DeleteConsumerGroupRequest.newBuilder()
                        .setGroupId(groupId)
                        .build());
    }

    /** Admin-initiated offset reset for a group. */
    public jbroker.proto.broker.ResetConsumerGroupOffsetsResponse resetConsumerGroupOffsets(
            String groupId, java.util.List<jbroker.proto.broker.OffsetReset> resets) {
        var b = jbroker.proto.broker.ResetConsumerGroupOffsetsRequest.newBuilder()
                .setGroupId(groupId);
        for (var r : resets) b.addResets(r);
        return admin.withDeadlineAfter(5, TimeUnit.SECONDS).resetConsumerGroupOffsets(b.build());
    }

    // ---- Cluster lifecycle. All controller-routed (non-leaders answer
    // NOT_LEADER with suggested_leader_* hints) except listReassignments,
    // which any broker answers from its replicated cache. ----

    /** Admit a running broker into the cluster: learner first, voter once caught up. */
    public jbroker.proto.broker.AddBrokerResponse addBroker(int brokerId, String host, int raftPort, int brokerPort) {
        return admin.withDeadlineAfter(10, TimeUnit.SECONDS)
                .addBroker(jbroker.proto.broker.AddBrokerRequest.newBuilder()
                        .setBrokerId(brokerId)
                        .setHost(host)
                        .setRaftPort(raftPort)
                        .setBrokerPort(brokerPort)
                        .build());
    }

    /** Drain every replica off the broker, then remove its Raft vote. */
    public jbroker.proto.broker.DecommissionBrokerResponse decommissionBroker(int brokerId) {
        return admin.withDeadlineAfter(10, TimeUnit.SECONDS)
                .decommissionBroker(jbroker.proto.broker.DecommissionBrokerRequest.newBuilder()
                        .setBrokerId(brokerId)
                        .build());
    }

    /** Voter set plus join/decommission progress, from the controller. */
    public jbroker.proto.broker.DescribeMembershipResponse describeMembership() {
        return admin.withDeadlineAfter(5, TimeUnit.SECONDS)
                .describeMembership(jbroker.proto.broker.DescribeMembershipRequest.newBuilder()
                        .build());
    }

    /** Reassign one partition's replicas to the target set (expand-then-contract). */
    public jbroker.proto.broker.ReassignPartitionResponse reassignPartition(
            String topic, int partition, java.util.List<Integer> replicas) {
        return admin.withDeadlineAfter(10, TimeUnit.SECONDS)
                .reassignPartition(jbroker.proto.broker.ReassignPartitionRequest.newBuilder()
                        .setTopic(topic)
                        .setPartition(partition)
                        .addAllReplicas(replicas)
                        .build());
    }

    /** Pending reassignments, from the responding broker's replicated cache. */
    public jbroker.proto.broker.ListReassignmentsResponse listReassignments() {
        return admin.withDeadlineAfter(5, TimeUnit.SECONDS)
                .listReassignments(jbroker.proto.broker.ListReassignmentsRequest.newBuilder()
                        .build());
    }

    /** Revert a pending reassignment to its original replica set. */
    public jbroker.proto.broker.CancelReassignmentResponse cancelReassignment(String topic, int partition) {
        return admin.withDeadlineAfter(10, TimeUnit.SECONDS)
                .cancelReassignment(jbroker.proto.broker.CancelReassignmentRequest.newBuilder()
                        .setTopic(topic)
                        .setPartition(partition)
                        .build());
    }

    /** Kick a preferred-leader rebalance; the response counts the moves proposed. */
    public jbroker.proto.broker.RebalanceLeadershipResponse rebalanceLeadership() {
        return admin.withDeadlineAfter(10, TimeUnit.SECONDS)
                .rebalanceLeadership(jbroker.proto.broker.RebalanceLeadershipRequest.newBuilder()
                        .build());
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            channel.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
