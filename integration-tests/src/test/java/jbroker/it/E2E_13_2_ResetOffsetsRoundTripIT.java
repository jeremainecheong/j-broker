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
import jbroker.proto.broker.FetchOffsetsRequest;
import jbroker.proto.broker.OffsetCommit;
import jbroker.proto.broker.OffsetReset;
import jbroker.proto.broker.ResetConsumerGroupOffsetsRequest;
import jbroker.proto.common.ErrorCode;
import jbroker.proto.common.TopicPartition;
import jbroker.raft.core.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * P13.2 — prove that {@code POST /consumer-groups/{id}/reset-offsets} takes
 * effect on the OffsetCache, i.e. a subsequent {@code FetchOffsets} returns
 * the reset value rather than the prior commit.
 *
 * <p>P12.7 shipped the admin RPC and confirmed it returns {@code OK} +
 * writes a record, but nothing drove a live member through the full
 * commit → admin-reset → refetch loop. That's the gap this IT closes.
 */
class E2E_13_2_ResetOffsetsRoundTripIT {

    @Test
    void adminResetOverridesPriorCommitAndFetchOffsetsSeesTheNewValue(@TempDir Path dir) throws Exception {
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
                // Seed the log so offset 10 is a meaningful committed position.
                for (int i = 0; i < 20; i++) {
                    client.produce("orders", 0, ("msg-" + i).getBytes(StandardCharsets.UTF_8));
                }

                var consumer = ConsumerGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
                var admin = AdminGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);

                var join = consumer.consumerGroupHeartbeat(ConsumerGroupHeartbeatRequest.newBuilder()
                        .setGroupId("g1")
                        .setMemberEpoch(0)
                        .addSubscribedTopics("orders")
                        .build());
                assertThat(join.getError()).isEqualTo(ErrorCode.OK);

                var tp = TopicPartition.newBuilder()
                        .setTopic("orders")
                        .setPartition(0)
                        .build();

                var commit = consumer.commitOffsets(CommitOffsetsRequest.newBuilder()
                        .setGroupId("g1")
                        .setMemberId(join.getMemberId())
                        .setGenerationIdOrMemberEpoch(join.getMemberEpoch())
                        .addCommits(OffsetCommit.newBuilder()
                                .setTp(tp)
                                .setOffset(10L)
                                .setLeaderEpoch(0)
                                .setMetadata("pre-reset")
                                .build())
                        .build());
                assertThat(commit.getResults(0).getError()).isEqualTo(ErrorCode.OK);

                var preReset = consumer.fetchOffsets(FetchOffsetsRequest.newBuilder()
                        .setGroupId("g1")
                        .addTps(tp)
                        .build());
                assertThat(preReset.getResults(0).getError()).isEqualTo(ErrorCode.OK);
                assertThat(preReset.getResults(0).getOffset()).isEqualTo(10L);

                // Admin-level reset — the whole point of this slice.
                var reset = admin.resetConsumerGroupOffsets(ResetConsumerGroupOffsetsRequest.newBuilder()
                        .setGroupId("g1")
                        .addResets(OffsetReset.newBuilder()
                                .setTp(tp)
                                .setOffset(5L)
                                .setLeaderEpoch(0)
                                .build())
                        .build());
                assertThat(reset.getError()).isEqualTo(ErrorCode.OK);
                assertThat(reset.getResults(0).getError()).isEqualTo(ErrorCode.OK);

                // FetchOffsets must observe the reset, not the prior commit.
                var postReset = consumer.fetchOffsets(FetchOffsetsRequest.newBuilder()
                        .setGroupId("g1")
                        .addTps(tp)
                        .build());
                assertThat(postReset.getResults(0).getError()).isEqualTo(ErrorCode.OK);
                assertThat(postReset.getResults(0).getOffset())
                        .as("admin reset must supersede the member's earlier commit")
                        .isEqualTo(5L);
            } finally {
                channel.shutdown();
                channel.awaitTermination(2, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void resetOnNonExistentGroupProvisionsOffsetsForLaterJoin(@TempDir Path dir) throws Exception {
        // Mirrors Kafka's AdminClient.alterConsumerGroupOffsets semantics:
        // admin reset is authoritative and may pre-seed offsets for a group
        // that has not joined yet. That's asymmetric with DeleteConsumerGroup
        // (which returns UNKNOWN_GROUP), but intentionally so — the operator
        // workflow of "stage the offsets, then start the new consumer" would
        // otherwise require a dummy heartbeat first. This test pins the
        // behavior so a later change doesn't regress the UX by accident.
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
                var admin = AdminGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
                var consumer = ConsumerGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
                var tp = TopicPartition.newBuilder()
                        .setTopic("orders")
                        .setPartition(0)
                        .build();

                var reset = admin.resetConsumerGroupOffsets(ResetConsumerGroupOffsetsRequest.newBuilder()
                        .setGroupId("pre-seeded")
                        .addResets(
                                OffsetReset.newBuilder().setTp(tp).setOffset(7L).build())
                        .build());
                assertThat(reset.getError()).isEqualTo(ErrorCode.OK);
                assertThat(reset.getResults(0).getError()).isEqualTo(ErrorCode.OK);

                // The provisioned offset should be readable without the
                // group ever having joined.
                var fetch = consumer.fetchOffsets(FetchOffsetsRequest.newBuilder()
                        .setGroupId("pre-seeded")
                        .addTps(tp)
                        .build());
                assertThat(fetch.getResults(0).getError()).isEqualTo(ErrorCode.OK);
                assertThat(fetch.getResults(0).getOffset()).isEqualTo(7L);
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
