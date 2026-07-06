package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import jbroker.app.testkit.TestBrokers;
import jbroker.broker.ConsumerOffsetsTopic;
import jbroker.proto.broker.CommitOffsetsRequest;
import jbroker.proto.broker.ConsumerGroupHeartbeatRequest;
import jbroker.proto.broker.ConsumerGrpc;
import jbroker.proto.broker.FetchOffsetsRequest;
import jbroker.proto.broker.OffsetCommit;
import jbroker.proto.common.ErrorCode;
import jbroker.proto.common.TopicPartition;
import jbroker.raft.core.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * P7.6 — covers E2E-7-6 (offset commit + restart resumes from N) end-to-end:
 * <ol>
 *   <li>Single-broker cluster with auto-created __consumer_offsets.</li>
 *   <li>Join group, commit offset 42 for orders/0.</li>
 *   <li>FetchOffsets returns 42 immediately (in-memory cache).</li>
 *   <li>Close broker, restart with same dataDir.</li>
 *   <li>FetchOffsets after restart returns 42 (recovery walk over the
 *       __consumer_offsets log into the OffsetCache).</li>
 * </ol>
 */
class OffsetCommitFetchEndToEndIT {

    @Test
    void e2e_7_6_commitedOffsetSurvivesBrokerRestart(@TempDir Path dir) throws Exception {

        // Session 1: join + commit + verify in-memory fetch.
        String memberId;
        int memberEpoch;
        var node1 = TestBrokers.start((rp, bp) -> new Broker.Config(new NodeId(1), dir, rp, bp));
        var broker1 = node1.broker();
        int raftPort = node1.raftPort(), brokerPort = node1.brokerPort();
        var ch1 = NettyChannelBuilder.forAddress("127.0.0.1", brokerPort)
                .usePlaintext()
                .build();
        try (var client = new jbroker.broker.client.BrokerClient("127.0.0.1", brokerPort)) {
            waitForCoordinatorTopic(broker1);
            client.createTopic("orders", 1, 1);

            var stub1 = ConsumerGrpc.newBlockingStub(ch1).withDeadlineAfter(5, TimeUnit.SECONDS);
            var join = stub1.consumerGroupHeartbeat(ConsumerGroupHeartbeatRequest.newBuilder()
                    .setGroupId("g1")
                    .setMemberEpoch(0)
                    .addSubscribedTopics("orders")
                    .build());
            assertThat(join.getError()).isEqualTo(ErrorCode.OK);
            memberId = join.getMemberId();
            memberEpoch = join.getMemberEpoch();

            var commit = stub1.commitOffsets(CommitOffsetsRequest.newBuilder()
                    .setGroupId("g1")
                    .setMemberId(memberId)
                    .setGenerationIdOrMemberEpoch(memberEpoch)
                    .addCommits(OffsetCommit.newBuilder()
                            .setTp(TopicPartition.newBuilder()
                                    .setTopic("orders")
                                    .setPartition(0)
                                    .build())
                            .setOffset(42L)
                            .setLeaderEpoch(7)
                            .setMetadata("checkpoint-A")
                            .build())
                    .build());
            assertThat(commit.getResults(0).getError()).isEqualTo(ErrorCode.OK);

            var fetch = stub1.fetchOffsets(FetchOffsetsRequest.newBuilder()
                    .setGroupId("g1")
                    .addTps(TopicPartition.newBuilder()
                            .setTopic("orders")
                            .setPartition(0)
                            .build())
                    .build());
            assertThat(fetch.getResults(0).getError()).isEqualTo(ErrorCode.OK);
            assertThat(fetch.getResults(0).getOffset()).isEqualTo(42L);
            assertThat(fetch.getResults(0).getLeaderEpoch()).isEqualTo(7);
            assertThat(fetch.getResults(0).getMetadata()).isEqualTo("checkpoint-A");
        } finally {
            ch1.shutdown();
            ch1.awaitTermination(2, TimeUnit.SECONDS);
            broker1.close();
        }

        // Session 2: same dataDir + ports → recovery walk re-reads
        // __consumer_offsets log and FetchOffsets returns 42 again.
        var broker2 = Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort));
        var ch2 = NettyChannelBuilder.forAddress("127.0.0.1", brokerPort)
                .usePlaintext()
                .build();
        try {
            // Wait for the registration ticker to run the recovery walk.
            // (Topic exists from prior session via Raft replay; the ticker
            // runs every 1s so up to 2s for the post-startup tick.)
            waitForCoordinatorTopic(broker2);
            long deadline = System.currentTimeMillis() + 5_000;
            jbroker.proto.broker.OffsetFetchResult result = null;
            while (System.currentTimeMillis() < deadline) {
                var stub2 = ConsumerGrpc.newBlockingStub(ch2).withDeadlineAfter(5, TimeUnit.SECONDS);
                var fetch = stub2.fetchOffsets(FetchOffsetsRequest.newBuilder()
                        .setGroupId("g1")
                        .addTps(TopicPartition.newBuilder()
                                .setTopic("orders")
                                .setPartition(0)
                                .build())
                        .build());
                result = fetch.getResults(0);
                if (result.getError() == ErrorCode.OK && result.getOffset() == 42L) break;
                Thread.sleep(100);
            }
            assertThat(result).isNotNull();
            assertThat(result.getError())
                    .as("offset must be recovered from __consumer_offsets after restart")
                    .isEqualTo(ErrorCode.OK);
            assertThat(result.getOffset()).isEqualTo(42L);
            assertThat(result.getLeaderEpoch()).isEqualTo(7);
            assertThat(result.getMetadata()).isEqualTo("checkpoint-A");
        } finally {
            ch2.shutdown();
            ch2.awaitTermination(2, TimeUnit.SECONDS);
            broker2.close();
        }
    }

    @Test
    void commitWithStaleEpochReturnsFencedMemberEpoch(@TempDir Path dir) throws Exception {
        var broker = TestBrokers.startSingleNode(dir);
        int brokerPort = broker.brokerPort();
        var channel = NettyChannelBuilder.forAddress("127.0.0.1", brokerPort)
                .usePlaintext()
                .build();
        try (var client = new jbroker.broker.client.BrokerClient("127.0.0.1", brokerPort)) {
            waitForCoordinatorTopic(broker);
            client.createTopic("orders", 1, 1);
            var stub = ConsumerGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
            var join = stub.consumerGroupHeartbeat(ConsumerGroupHeartbeatRequest.newBuilder()
                    .setGroupId("g1")
                    .setMemberEpoch(0)
                    .addSubscribedTopics("orders")
                    .build());

            var commit = stub.commitOffsets(CommitOffsetsRequest.newBuilder()
                    .setGroupId("g1")
                    .setMemberId(join.getMemberId())
                    .setGenerationIdOrMemberEpoch(join.getMemberEpoch() + 99) // future epoch
                    .addCommits(OffsetCommit.newBuilder()
                            .setTp(TopicPartition.newBuilder()
                                    .setTopic("orders")
                                    .setPartition(0)
                                    .build())
                            .setOffset(1L)
                            .build())
                    .build());

            assertThat(commit.getResults(0).getError()).isEqualTo(ErrorCode.FENCED_MEMBER_EPOCH);
        } finally {
            channel.shutdown();
            channel.awaitTermination(2, TimeUnit.SECONDS);
            broker.close();
        }
    }

    @Test
    void fetchOffsetsForUncommittedTpReturnsOutOfRange(@TempDir Path dir) throws Exception {
        var broker = TestBrokers.startSingleNode(dir);
        int brokerPort = broker.brokerPort();
        var channel = NettyChannelBuilder.forAddress("127.0.0.1", brokerPort)
                .usePlaintext()
                .build();
        try {
            waitForCoordinatorTopic(broker);
            var stub = ConsumerGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
            var fetch = stub.fetchOffsets(FetchOffsetsRequest.newBuilder()
                    .setGroupId("g1")
                    .addTps(TopicPartition.newBuilder()
                            .setTopic("never-committed")
                            .setPartition(0)
                            .build())
                    .build());
            assertThat(fetch.getResults(0).getError()).isEqualTo(ErrorCode.OFFSET_OUT_OF_RANGE);
            assertThat(fetch.getResults(0).getOffset()).isEqualTo(-1L);
        } finally {
            channel.shutdown();
            channel.awaitTermination(2, TimeUnit.SECONDS);
            broker.close();
        }
    }

    private static void waitForCoordinatorTopic(Broker broker) throws InterruptedException {
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
}
