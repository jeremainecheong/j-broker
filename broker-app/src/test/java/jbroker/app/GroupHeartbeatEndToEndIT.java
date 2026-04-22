package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import jbroker.broker.ConsumerOffsetsTopic;
import jbroker.proto.broker.ConsumerGroupHeartbeatRequest;
import jbroker.proto.broker.ConsumerGrpc;
import jbroker.proto.common.ErrorCode;
import jbroker.raft.core.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * covers (single consumer reads all partitions) and * (3 consumers, 6 partitions, evenly split) end-to-end through gRPC against
 * a single-broker cluster (which by default leads partition 0 of
 * {@code __consumer_offsets} → it IS the coordinator for every group).
 */
class GroupHeartbeatEndToEndIT {

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void singleConsumerGetsAllPartitions(@TempDir Path dir) throws Exception {
        int brokerPort = freePort();
        int raftPort = freePort();
        var broker = Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort));
        var channel = NettyChannelBuilder.forAddress("127.0.0.1", brokerPort)
                .usePlaintext()
                .build();
        try (var client = new jbroker.broker.client.BrokerClient("127.0.0.1", brokerPort)) {
            // Wait for __consumer_offsets to land.
            waitForCoordinatorTopic(broker);

            client.createTopic("orders", 6, 1);

            var stub = ConsumerGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
            var resp = stub.consumerGroupHeartbeat(ConsumerGroupHeartbeatRequest.newBuilder()
                    .setGroupId("g1")
                    .setMemberEpoch(0)
                    .addSubscribedTopics("orders")
                    .build());

            assertThat(resp.getError()).isEqualTo(ErrorCode.OK);
            assertThat(resp.getMemberId()).isNotEmpty();
            // Single consumer → all 6 partitions.
            assertThat(resp.getAssignment().getAssignedPartitionsCount()).isEqualTo(1);
            assertThat(resp.getAssignment().getAssignedPartitions(0).getTopic()).isEqualTo("orders");
            assertThat(resp.getAssignment().getAssignedPartitions(0).getPartitionsList())
                    .containsExactly(0, 1, 2, 3, 4, 5);
        } finally {
            channel.shutdown();
            channel.awaitTermination(2, TimeUnit.SECONDS);
            broker.close();
        }
    }

    @Test
    void threeConsumersSixPartitionsEachGetsTwo(@TempDir Path dir) throws Exception {
        int brokerPort = freePort();
        int raftPort = freePort();
        var broker = Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort));
        var channel = NettyChannelBuilder.forAddress("127.0.0.1", brokerPort)
                .usePlaintext()
                .build();
        try (var client = new jbroker.broker.client.BrokerClient("127.0.0.1", brokerPort)) {
            waitForCoordinatorTopic(broker);
            client.createTopic("orders", 6, 1);

            var stub = ConsumerGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);

            // Three sequential joins.
            var m1 = stub.consumerGroupHeartbeat(joinReq("g1", "orders"));
            var m2 = stub.consumerGroupHeartbeat(joinReq("g1", "orders"));
            var m3 = stub.consumerGroupHeartbeat(joinReq("g1", "orders"));

            assertThat(m1.getError()).isEqualTo(ErrorCode.OK);
            assertThat(m2.getError()).isEqualTo(ErrorCode.OK);
            assertThat(m3.getError()).isEqualTo(ErrorCode.OK);

            // cooperative incremental rebalance changed the
            // dynamics here. m1 lost partitions but had no additions →
            // single-stage advance to kept set [0,1]. m2 lost [4,5] AND
            // gained [2] → stage 1 returns [3] (kept), then after m2 acks
            // by sending owned_partitions=[3], stage 2 returns [2,3]. m3
            // got [4,5] immediately at join (brand-new member, no kept set).
            //
            // Drive m1 forward — single tick suffices.
            var m1Final = driveToSettled(stub, "g1", m1.getMemberId(), m1.getMemberEpoch(), m1.getAssignment());
            // Drive m2 forward — needs the stage-1→stage-2 dance.
            var m2Final = driveToSettled(stub, "g1", m2.getMemberId(), m2.getMemberEpoch(), m2.getAssignment());

            int m1Partitions = totalPartitionCount(m1Final);
            int m2Partitions = totalPartitionCount(m2Final);
            int m3Partitions = totalPartitionCount(m3.getAssignment());
            assertThat(m1Partitions).isEqualTo(2);
            assertThat(m2Partitions).isEqualTo(2);
            assertThat(m3Partitions).isEqualTo(2);

            // No partition assigned to two members.
            var allAssigned = new HashSet<Integer>();
            collectPartitions(m1Final, allAssigned);
            collectPartitions(m2Final, allAssigned);
            collectPartitions(m3.getAssignment(), allAssigned);
            assertThat(allAssigned).containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5);
        } finally {
            channel.shutdown();
            channel.awaitTermination(2, TimeUnit.SECONDS);
            broker.close();
        }
    }

    /**
     * cooperative-rebalance helper: heartbeat a member through any
     * staged kept-set → target transition and return its final assignment.
     * Caller passes the assignment the member already has (from the join
     * response, or empty for a member that was just created).
     */
    private static jbroker.proto.broker.Assignment driveToSettled(
            ConsumerGrpc.ConsumerBlockingStub stub,
            String group,
            String memberId,
            int startEpoch,
            jbroker.proto.broker.Assignment startAssignment) {
        var owned = startAssignment;
        int epoch = startEpoch;
        // Two ticks max: stage 1 (advertise kept) → stage 2 (advertise
        // target after ack). Idempotent past stage 2 — a third tick is a
        // no-op steady-state heartbeat.
        for (int i = 0; i < 3; i++) {
            var hb = stub.consumerGroupHeartbeat(ConsumerGroupHeartbeatRequest.newBuilder()
                    .setGroupId(group)
                    .setMemberId(memberId)
                    .setMemberEpoch(epoch)
                    .addAllOwnedPartitions(owned.getAssignedPartitionsList())
                    .build());
            if (hb.getAssignment().getAssignedPartitionsCount() == 0) return owned; // steady-state, no change
            owned = hb.getAssignment();
            epoch = hb.getMemberEpoch();
        }
        return owned;
    }

    private static ConsumerGroupHeartbeatRequest joinReq(String group, String topic) {
        return ConsumerGroupHeartbeatRequest.newBuilder()
                .setGroupId(group)
                .setMemberEpoch(0)
                .addSubscribedTopics(topic)
                .build();
    }

    private static int totalPartitionCount(jbroker.proto.broker.Assignment a) {
        int total = 0;
        for (var tp : a.getAssignedPartitionsList()) {
            total += tp.getPartitionsCount();
        }
        return total;
    }

    private static void collectPartitions(jbroker.proto.broker.Assignment a, java.util.Set<Integer> out) {
        for (var tp : a.getAssignedPartitionsList()) {
            out.addAll(tp.getPartitionsList());
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
