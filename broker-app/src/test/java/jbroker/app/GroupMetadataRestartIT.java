package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import jbroker.broker.ConsumerOffsetsTopic;
import jbroker.proto.broker.ConsumerGroupHeartbeatRequest;
import jbroker.proto.broker.ConsumerGrpc;
import jbroker.proto.common.ErrorCode;
import jbroker.raft.core.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * P7.8 — proves that group membership state survives broker restart on the
 * same {@code dataDir}: after restart, a member with the same
 * {@code member_id} + {@code member_epoch} can heartbeat without getting
 * UNKNOWN_MEMBER_ID. This is the single-broker analogue of E2E-7-9
 * (multi-broker coordinator failover); the multi-broker version follows
 * in P7.12 once the full E2E-7 suite lands.
 *
 * <p>The mechanism under test: each {@code join} fires a snapshot listener
 * which appends a Type-2 GroupMetadataValue record to
 * {@code __consumer_offsets}. On restart, the recovery walk replays those
 * records back into a fresh {@link jbroker.broker.group.GroupCoordinator}.
 */
class GroupMetadataRestartIT {

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void groupMembershipSurvivesBrokerRestart(@TempDir Path dir) throws Exception {
        int brokerPort = freePort();
        int raftPort = freePort();

        // Session 1: join group, capture id + epoch.
        String memberId;
        int memberEpoch;
        var broker1 = Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort));
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
                    .setInstanceId("static-A")
                    .addSubscribedTopics("orders")
                    .build());
            assertThat(join.getError()).isEqualTo(ErrorCode.OK);
            memberId = join.getMemberId();
            memberEpoch = join.getMemberEpoch();
            assertThat(memberId).isNotEmpty();
        } finally {
            ch1.shutdown();
            ch1.awaitTermination(2, TimeUnit.SECONDS);
            broker1.close();
        }

        // Session 2: same dataDir + ports → recovery walk replays the
        // group-metadata record and the member is heartbeatable.
        var broker2 = Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort));
        var ch2 = NettyChannelBuilder.forAddress("127.0.0.1", brokerPort)
                .usePlaintext()
                .build();
        try {
            waitForCoordinatorTopic(broker2);
            // Wait up to 5s for the registration ticker to run the recovery walk.
            long deadline = System.currentTimeMillis() + 5_000;
            var stub2 = ConsumerGrpc.newBlockingStub(ch2).withDeadlineAfter(5, TimeUnit.SECONDS);
            jbroker.proto.broker.ConsumerGroupHeartbeatResponse hb = null;
            while (System.currentTimeMillis() < deadline) {
                hb = stub2.consumerGroupHeartbeat(ConsumerGroupHeartbeatRequest.newBuilder()
                        .setGroupId("g1")
                        .setMemberId(memberId)
                        .setMemberEpoch(memberEpoch)
                        .build());
                if (hb.getError() == ErrorCode.OK) break;
                Thread.sleep(100);
            }
            assertThat(hb).isNotNull();
            assertThat(hb.getError())
                    .as("member must be recovered from __consumer_offsets after restart")
                    .isEqualTo(ErrorCode.OK);
        } finally {
            ch2.shutdown();
            ch2.awaitTermination(2, TimeUnit.SECONDS);
            broker2.close();
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
