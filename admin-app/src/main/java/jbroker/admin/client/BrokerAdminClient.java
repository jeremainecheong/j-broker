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
import jbroker.proto.broker.DescribeTopicPartitionsRequest;
import jbroker.proto.broker.DescribeTopicPartitionsResponse;
import jbroker.proto.broker.ListConsumerGroupsRequest;
import jbroker.proto.broker.ListConsumerGroupsResponse;
import jbroker.proto.broker.ListTopicsRequest;
import jbroker.proto.broker.ListTopicsResponse;
import jbroker.proto.broker.MetadataGrpc;
import jbroker.proto.broker.UpdateTopicConfigRequest;
import jbroker.proto.broker.UpdateTopicConfigResponse;

/**
 * Thin gRPC wrapper the admin-app uses to talk to a single broker. One
 * instance per configured broker; callers that need cluster-wide fan-out
 * iterate a {@link BrokerAdminClientPool}.
 *
 * <p>As of P8.3 carries {@code Admin} + {@code Metadata} stubs; later
 * slices add {@code Consumer} for consumer-group reads and the streaming
 * events RPC.
 */
public final class BrokerAdminClient implements AutoCloseable {

    private final String address;
    private final ManagedChannel channel;
    private final MetadataGrpc.MetadataBlockingStub metadata;
    private final AdminGrpc.AdminBlockingStub admin;

    public BrokerAdminClient(String host, int port) {
        this.address = host + ":" + port;
        this.channel = NettyChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.metadata = MetadataGrpc.newBlockingStub(channel);
        this.admin = AdminGrpc.newBlockingStub(channel);
    }

    public static BrokerAdminClient parse(String hostPort) {
        int colon = hostPort.indexOf(':');
        if (colon < 0) throw new IllegalArgumentException("broker address needs host:port, got " + hostPort);
        return new BrokerAdminClient(hostPort.substring(0, colon), Integer.parseInt(hostPort.substring(colon + 1)));
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

    public UpdateTopicConfigResponse updateTopicConfig(String topic, Map<String, String> overlay) {
        var b = UpdateTopicConfigRequest.newBuilder().setTopic(topic);
        b.putAllConfig(overlay);
        return admin.withDeadlineAfter(5, TimeUnit.SECONDS).updateTopicConfig(b.build());
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
