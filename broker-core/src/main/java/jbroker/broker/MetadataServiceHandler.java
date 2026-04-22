package jbroker.broker;

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
import jbroker.proto.common.ErrorCode;

/**
 * Handler for the Milestone 8 {@code Metadata} gRPC service — the read-only
 * observability surface that the admin-app REST layer consumes.
 *
 * <p>lands the surface; every RPC returns {@link ErrorCode#UNIMPLEMENTED}
 * until its owning slice replaces the body:
 * <ul>
 *   <li>{@link #describeCluster}</li>
 *   <li>{@link #describeTopicPartitions}</li>
 *   <li>{@link #listConsumerGroups} + {@link #describeConsumerGroup}</li>
 *   <li>{@link #describeRaft}</li>
 * </ul>
 */
public final class MetadataServiceHandler {

    public DescribeClusterResponse describeCluster(DescribeClusterRequest req) {
        return DescribeClusterResponse.newBuilder()
                .setError(ErrorCode.UNIMPLEMENTED)
                .build();
    }

    public DescribeTopicPartitionsResponse describeTopicPartitions(DescribeTopicPartitionsRequest req) {
        return DescribeTopicPartitionsResponse.newBuilder()
                .setError(ErrorCode.UNIMPLEMENTED)
                .setTopic(req.getTopic())
                .build();
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
}
