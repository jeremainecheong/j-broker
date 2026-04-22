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
 * P7.5 — covers E2E-7-1 (single consumer reads all partitions) and E2E-7-2
 * (3 consumers, 6 partitions, evenly split) end-to-end through gRPC against
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
    void e2e_7_1_singleConsumerGetsAllPartitions(@TempDir Path dir) throws Exception {
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
    void e2e_7_2_threeConsumersSixPartitionsEachGetsTwo(@TempDir Path dir) throws Exception {
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

            // m1's assignment was bumped by m2's join (and again by m3's),
            // so re-fetch its current state via a steady-state heartbeat.
            // Same for m2.
            var m1Updated = stub.consumerGroupHeartbeat(ConsumerGroupHeartbeatRequest.newBuilder()
                    .setGroupId("g1")
                    .setMemberId(m1.getMemberId())
                    .setMemberEpoch(m1.getMemberEpoch())
                    .build());
            var m2Updated = stub.consumerGroupHeartbeat(ConsumerGroupHeartbeatRequest.newBuilder()
                    .setGroupId("g1")
                    .setMemberId(m2.getMemberId())
                    .setMemberEpoch(m2.getMemberEpoch())
                    .build());

            // m3's epoch already reflects the latest assignment
            // (no further joins after it), so m3.assignment is current.
            // For m1/m2, the steady-state response carries the new
            // assignment + bumped epoch.
            int m1Partitions = totalPartitionCount(m1Updated.getAssignment());
            int m2Partitions = totalPartitionCount(m2Updated.getAssignment());
            int m3Partitions = totalPartitionCount(m3.getAssignment());
            assertThat(m1Partitions).isEqualTo(2);
            assertThat(m2Partitions).isEqualTo(2);
            assertThat(m3Partitions).isEqualTo(2);

            // No partition assigned to two members.
            var allAssigned = new HashSet<Integer>();
            collectPartitions(m1Updated.getAssignment(), allAssigned);
            collectPartitions(m2Updated.getAssignment(), allAssigned);
            collectPartitions(m3.getAssignment(), allAssigned);
            assertThat(allAssigned).containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5);
        } finally {
            channel.shutdown();
            channel.awaitTermination(2, TimeUnit.SECONDS);
            broker.close();
        }
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
