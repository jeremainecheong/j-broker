package jbroker.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import jbroker.app.Broker;
import jbroker.app.VoterAddress;
import jbroker.broker.ConsumerOffsetsTopic;
import jbroker.proto.broker.AdminGrpc;
import jbroker.proto.broker.CommitOffsetsRequest;
import jbroker.proto.broker.ConsumerGroupHeartbeatRequest;
import jbroker.proto.broker.ConsumerGrpc;
import jbroker.proto.broker.DeleteConsumerGroupRequest;
import jbroker.proto.broker.FetchOffsetsRequest;
import jbroker.proto.broker.ListConsumerGroupsRequest;
import jbroker.proto.broker.MetadataGrpc;
import jbroker.proto.broker.OffsetCommit;
import jbroker.proto.common.ErrorCode;
import jbroker.proto.common.TopicPartition;
import jbroker.raft.core.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * P13.3 — prove {@code DELETE /api/v1/consumer-groups/{id}} actually
 * removes a group: subsequent {@code ListConsumerGroups} no longer lists
 * it, and {@code FetchOffsets} against its old (topic, partition) returns
 * {@code OFFSET_OUT_OF_RANGE}. Exercises both
 * {@code GroupCoordinator.removeGroup} and {@code OffsetCache.dropGroup}
 * in the admin-initiated path.
 */
class E2E_13_3_DeleteGroupRoundTripIT {

    @Test
    void deleteGroupDropsGroupFromListingAndClearsCommittedOffsets(@TempDir Path dir) throws Exception {
        int brokerPort = freePort();
        int raftPort = freePort();
        var voters = java.util.List.of(new VoterAddress(new NodeId(1), "127.0.0.1", raftPort, brokerPort));

        try (var broker = Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort, voters))) {
            awaitCoordinatorReady(broker);

            var channel = NettyChannelBuilder.forAddress("127.0.0.1", brokerPort)
                    .usePlaintext()
                    .build();
            try (var client = new jbroker.broker.client.BrokerClient("127.0.0.1", brokerPort)) {
                client.createTopic("orders", 1, 1);
                for (int i = 0; i < 5; i++) {
                    client.produce("orders", 0, ("msg-" + i).getBytes(StandardCharsets.UTF_8));
                }

                var consumer = ConsumerGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
                var admin = AdminGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
                var metadata = MetadataGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);

                var tp = TopicPartition.newBuilder()
                        .setTopic("orders")
                        .setPartition(0)
                        .build();

                // Join + commit so the group has both coordinator state and
                // an OffsetCache entry to drop.
                var join = consumer.consumerGroupHeartbeat(ConsumerGroupHeartbeatRequest.newBuilder()
                        .setGroupId("g-doomed")
                        .setMemberEpoch(0)
                        .addSubscribedTopics("orders")
                        .build());
                assertThat(join.getError()).isEqualTo(ErrorCode.OK);

                var commit = consumer.commitOffsets(CommitOffsetsRequest.newBuilder()
                        .setGroupId("g-doomed")
                        .setMemberId(join.getMemberId())
                        .setGenerationIdOrMemberEpoch(join.getMemberEpoch())
                        .addCommits(OffsetCommit.newBuilder()
                                .setTp(tp)
                                .setOffset(3L)
                                .build())
                        .build());
                assertThat(commit.getResults(0).getError()).isEqualTo(ErrorCode.OK);

                // Pre-delete observability: group listed + offset present.
                var preListing = metadata.listConsumerGroups(
                        ListConsumerGroupsRequest.newBuilder().build());
                assertThat(preListing.getError()).isEqualTo(ErrorCode.OK);
                assertThat(preListing.getGroupsList())
                        .as("group shows up in ListConsumerGroups before deletion")
                        .anySatisfy(g -> assertThat(g.getGroupId()).isEqualTo("g-doomed"));

                var preOffset = consumer.fetchOffsets(FetchOffsetsRequest.newBuilder()
                        .setGroupId("g-doomed")
                        .addTps(tp)
                        .build());
                assertThat(preOffset.getResults(0).getError()).isEqualTo(ErrorCode.OK);
                assertThat(preOffset.getResults(0).getOffset()).isEqualTo(3L);

                // Admin delete — the whole point of this slice.
                var del = admin.deleteConsumerGroup(DeleteConsumerGroupRequest.newBuilder()
                        .setGroupId("g-doomed")
                        .build());
                assertThat(del.getError()).isEqualTo(ErrorCode.OK);

                // Post-delete: group absent from listing.
                var postListing = metadata.listConsumerGroups(
                        ListConsumerGroupsRequest.newBuilder().build());
                assertThat(postListing.getError()).isEqualTo(ErrorCode.OK);
                assertThat(postListing.getGroupsList())
                        .as("group disappears from ListConsumerGroups after deletion")
                        .noneSatisfy(g -> assertThat(g.getGroupId()).isEqualTo("g-doomed"));

                // Post-delete: FetchOffsets for the dropped group returns
                // OFFSET_OUT_OF_RANGE (the broker's "never committed" signal),
                // proving OffsetCache.dropGroup fired.
                var postOffset = consumer.fetchOffsets(FetchOffsetsRequest.newBuilder()
                        .setGroupId("g-doomed")
                        .addTps(tp)
                        .build());
                assertThat(postOffset.getResults(0).getError())
                        .as("dropped group's committed offsets must read as never-committed")
                        .isEqualTo(ErrorCode.OFFSET_OUT_OF_RANGE);
                assertThat(postOffset.getResults(0).getOffset()).isEqualTo(-1L);
            } finally {
                channel.shutdown();
                channel.awaitTermination(2, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void deleteOnUnknownGroupReturnsUnknownGroup(@TempDir Path dir) throws Exception {
        int brokerPort = freePort();
        int raftPort = freePort();
        var voters = java.util.List.of(new VoterAddress(new NodeId(1), "127.0.0.1", raftPort, brokerPort));

        try (var broker = Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort, voters))) {
            awaitCoordinatorReady(broker);

            var channel = NettyChannelBuilder.forAddress("127.0.0.1", brokerPort)
                    .usePlaintext()
                    .build();
            try {
                var admin = AdminGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
                var del = admin.deleteConsumerGroup(DeleteConsumerGroupRequest.newBuilder()
                        .setGroupId("never-existed")
                        .build());
                assertThat(del.getError()).isEqualTo(ErrorCode.UNKNOWN_GROUP);
            } finally {
                channel.shutdown();
                channel.awaitTermination(2, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void rejoinAfterDeleteStartsWithFreshCoordinatorState(@TempDir Path dir) throws Exception {
        // Subtle invariant: once admin deletes a group, a new client joining
        // with the same group id must land in a brand-new generation (member
        // epoch 0 after first heartbeat) — not inherit any state from the
        // old group. Prevents a regression where removeGroup left behind
        // stale member ids that would collide with a re-join.
        int brokerPort = freePort();
        int raftPort = freePort();
        var voters = java.util.List.of(new VoterAddress(new NodeId(1), "127.0.0.1", raftPort, brokerPort));

        try (var broker = Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort, voters))) {
            awaitCoordinatorReady(broker);

            var channel = NettyChannelBuilder.forAddress("127.0.0.1", brokerPort)
                    .usePlaintext()
                    .build();
            try (var client = new jbroker.broker.client.BrokerClient("127.0.0.1", brokerPort)) {
                client.createTopic("orders", 1, 1);
                var consumer = ConsumerGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
                var admin = AdminGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);

                var firstJoin = consumer.consumerGroupHeartbeat(ConsumerGroupHeartbeatRequest.newBuilder()
                        .setGroupId("g-recycle")
                        .setMemberEpoch(0)
                        .addSubscribedTopics("orders")
                        .build());
                assertThat(firstJoin.getError()).isEqualTo(ErrorCode.OK);
                String firstMemberId = firstJoin.getMemberId();
                assertThat(firstMemberId).isNotEmpty();

                var del = admin.deleteConsumerGroup(DeleteConsumerGroupRequest.newBuilder()
                        .setGroupId("g-recycle")
                        .build());
                assertThat(del.getError()).isEqualTo(ErrorCode.OK);

                var rejoin = consumer.consumerGroupHeartbeat(ConsumerGroupHeartbeatRequest.newBuilder()
                        .setGroupId("g-recycle")
                        .setMemberEpoch(0)
                        .addSubscribedTopics("orders")
                        .build());
                assertThat(rejoin.getError()).isEqualTo(ErrorCode.OK);
                assertThat(rejoin.getMemberId())
                        .as("after delete, rejoin allocates a fresh member id")
                        .isNotEqualTo(firstMemberId);
            } finally {
                channel.shutdown();
                channel.awaitTermination(2, TimeUnit.SECONDS);
            }
        }
    }

    private static void awaitCoordinatorReady(Broker broker) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (broker.topics().describe(ConsumerOffsetsTopic.NAME).isPresent()
                    && broker.brokerRegistry().addressFor(1).isPresent()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("__consumer_offsets did not auto-create within 10s");
    }

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
