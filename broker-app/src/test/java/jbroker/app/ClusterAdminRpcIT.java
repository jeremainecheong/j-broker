package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import jbroker.app.testkit.BindRetry;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.broker.ErrorCodes;
import jbroker.broker.client.BrokerClient;
import jbroker.proto.broker.AddBrokerRequest;
import jbroker.proto.broker.AdminGrpc;
import jbroker.proto.broker.CancelReassignmentRequest;
import jbroker.proto.broker.DecommissionBrokerRequest;
import jbroker.proto.broker.DescribeMembershipRequest;
import jbroker.proto.broker.ListReassignmentsRequest;
import jbroker.proto.broker.ReassignPartitionRequest;
import jbroker.proto.broker.RebalanceLeadershipRequest;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The operator surface end to end, over real gRPC: join a broker, watch the
 * membership, reassign a partition, cancel a reassignment mid-flight,
 * rebalance leadership, and decommission a broker — plus the refusals
 * (follower answers NOT_LEADER with hints, unknown target broker, and
 * self-decommission).
 */
class ClusterAdminRpcIT {

    private static final int CI_MULT =
            "1".equals(System.getenv("JBROKER_CI")) || "true".equalsIgnoreCase(System.getenv("CI")) ? 4 : 1;

    @Test
    void adminRpcsDriveTheFullClusterLifecycle(
            @TempDir Path d1, @TempDir Path d2, @TempDir Path d3, @TempDir Path spareBase) throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        try (var cluster = TestBrokerCluster.start(3, 2, (i, voters, ports) -> new Broker.Config(
                        new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters)
                // Slow reassignment catch-up so a cancel has a window.
                .withReassignmentThrottleBytesPerSec(20_000))) {
            var brokers = cluster.brokers();
            awaitSingleLeader(brokers);
            awaitRegistryConvergence(brokers);

            var controller = leaderOf(brokers);
            int controllerId = brokers.indexOf(controller) + 1;
            int followerId = controllerId == 1 ? 2 : 1;

            try (var adminClient = new BrokerClient("127.0.0.1", controller.brokerPort())) {
                adminClient.createTopic("ops", 1, /*rf*/ 2);
            }
            awaitTopicSettled(controller);

            var leaderChannel = channel(controller.brokerPort());
            var followerChannel = channel(brokers.get(followerId - 1).brokerPort());
            Broker spare = null;
            try {
                var admin = AdminGrpc.newBlockingStub(leaderChannel);
                var followerAdmin = AdminGrpc.newBlockingStub(followerChannel);

                // Membership before: three voters, nothing in flight.
                var before = admin.withDeadlineAfter(5, TimeUnit.SECONDS)
                        .describeMembership(
                                DescribeMembershipRequest.newBuilder().build());
                assertThat(before.getVoterIdsList()).containsExactlyInAnyOrder(1, 2, 3);
                assertThat(before.getJoinPhase()).isEqualTo("IDLE");

                // A follower refuses mutations with NOT_LEADER.
                var notLeader = followerAdmin
                        .withDeadlineAfter(5, TimeUnit.SECONDS)
                        .rebalanceLeadership(
                                RebalanceLeadershipRequest.newBuilder().build());
                assertThat(notLeader.getError().getCode()).isEqualTo(ErrorCodes.NOT_LEADER);

                // Join broker 4 through the RPC.
                var incumbents = List.of(
                        new VoterAddress(new NodeId(1), "127.0.0.1", cluster.raftPort(0), cluster.brokerPort(0)),
                        new VoterAddress(new NodeId(2), "127.0.0.1", cluster.raftPort(1), cluster.brokerPort(1)),
                        new VoterAddress(new NodeId(3), "127.0.0.1", cluster.raftPort(2), cluster.brokerPort(2)));
                var sparePorts = new int[2];
                var spareDirHolder = new Path[1];
                spare = BindRetry.startWithBindRetry(() -> {
                    var p = BindRetry.freePorts(2);
                    spareDirHolder[0] = Files.createTempDirectory(spareBase, "spare-");
                    var b = Broker.start(new Broker.Config(new NodeId(4), spareDirHolder[0], p[0], p[1], incumbents));
                    sparePorts[0] = p[0];
                    sparePorts[1] = p[1];
                    return b;
                });
                var addResp = admin.withDeadlineAfter(10, TimeUnit.SECONDS)
                        .addBroker(AddBrokerRequest.newBuilder()
                                .setBrokerId(4)
                                .setHost("127.0.0.1")
                                .setRaftPort(sparePorts[0])
                                .setBrokerPort(sparePorts[1])
                                .build());
                assertThat(addResp.getError().getCode()).isZero();
                awaitJoinDone(admin, 60_000L * CI_MULT);

                // Reassign ops-0 onto {leader, 4}, throttled — then cancel
                // while the newcomer is still catching up.
                int p0Leader = controller
                        .topics()
                        .partitionState("ops", 0)
                        .orElseThrow()
                        .leader();
                try (var producer =
                        new BrokerClient("127.0.0.1", brokers.get(p0Leader - 1).brokerPort())) {
                    long pid = producer.initProducerId();
                    var pad = "x".repeat(2048);
                    for (int i = 0; i < 100; i++) {
                        producer.idempotentProduceAcksAll(
                                "ops", 0, (i + "-" + pad).getBytes(StandardCharsets.UTF_8), pid, 0, i);
                    }
                }
                var original = controller
                        .topics()
                        .partitionState("ops", 0)
                        .orElseThrow()
                        .replicas();
                var reassignResp = admin.withDeadlineAfter(10, TimeUnit.SECONDS)
                        .reassignPartition(ReassignPartitionRequest.newBuilder()
                                .setTopic("ops")
                                .setPartition(0)
                                .addAllReplicas(List.of(p0Leader, 4))
                                .build());
                assertThat(reassignResp.getError().getCode()).isZero();

                var listed = admin.withDeadlineAfter(5, TimeUnit.SECONDS)
                        .listReassignments(ListReassignmentsRequest.newBuilder().build());
                assertThat(listed.getReassignmentsList()).hasSize(1);
                assertThat(listed.getReassignments(0).getTargetReplicasList()).containsExactlyInAnyOrder(p0Leader, 4);

                // A second reassignment for the same partition is refused as in flight.
                var dup = admin.withDeadlineAfter(5, TimeUnit.SECONDS)
                        .reassignPartition(ReassignPartitionRequest.newBuilder()
                                .setTopic("ops")
                                .setPartition(0)
                                .addAllReplicas(List.of(p0Leader, 4))
                                .build());
                assertThat(dup.getError().getCode()).isEqualTo(ErrorCodes.REASSIGNMENT_IN_PROGRESS);

                var cancelResp = admin.withDeadlineAfter(10, TimeUnit.SECONDS)
                        .cancelReassignment(CancelReassignmentRequest.newBuilder()
                                .setTopic("ops")
                                .setPartition(0)
                                .build());
                assertThat(cancelResp.getError().getCode()).isZero();
                awaitCancelSettled(admin, controller, original, 30_000L * CI_MULT);

                // Validation refusals name the problem.
                var unknown = admin.withDeadlineAfter(5, TimeUnit.SECONDS)
                        .reassignPartition(ReassignPartitionRequest.newBuilder()
                                .setTopic("ops")
                                .setPartition(0)
                                .addAllReplicas(List.of(99))
                                .build());
                assertThat(unknown.getError().getCode()).isEqualTo(ErrorCodes.INVALID_CONFIG);
                var self = admin.withDeadlineAfter(5, TimeUnit.SECONDS)
                        .decommissionBroker(DecommissionBrokerRequest.newBuilder()
                                .setBrokerId(controllerId)
                                .build());
                assertThat(self.getError().getCode()).isEqualTo(ErrorCodes.INVALID_CONFIG);

                // Rebalance answers with a move count (possibly zero).
                var rebalance = admin.withDeadlineAfter(10, TimeUnit.SECONDS)
                        .rebalanceLeadership(
                                RebalanceLeadershipRequest.newBuilder().build());
                assertThat(rebalance.getError().getCode()).isZero();
                assertThat(rebalance.getMovesProposed()).isGreaterThanOrEqualTo(0);

                // Decommission the newcomer back out through the RPC.
                var dec = admin.withDeadlineAfter(10, TimeUnit.SECONDS)
                        .decommissionBroker(DecommissionBrokerRequest.newBuilder()
                                .setBrokerId(4)
                                .build());
                assertThat(dec.getError().getCode()).isZero();
                awaitDecommissionDone(admin, 4, 120_000L * CI_MULT);
            } finally {
                leaderChannel.shutdownNow();
                followerChannel.shutdownNow();
                if (spare != null) spare.close();
            }
        }
    }

    private static ManagedChannel channel(int port) {
        return NettyChannelBuilder.forAddress("127.0.0.1", port).usePlaintext().build();
    }

    private static void awaitJoinDone(AdminGrpc.AdminBlockingStub admin, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            var m = admin.withDeadlineAfter(5, TimeUnit.SECONDS)
                    .describeMembership(DescribeMembershipRequest.newBuilder().build());
            if ("DONE".equals(m.getJoinPhase()) && m.getVoterIdsList().contains(4)) return;
            Thread.sleep(200);
        }
        throw new AssertionError("join did not complete within " + timeoutMs + "ms");
    }

    private static void awaitDecommissionDone(AdminGrpc.AdminBlockingStub admin, int brokerId, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            var m = admin.withDeadlineAfter(5, TimeUnit.SECONDS)
                    .describeMembership(DescribeMembershipRequest.newBuilder().build());
            if ("DONE".equals(m.getDecommissionPhase()) && !m.getVoterIdsList().contains(brokerId)) {
                return;
            }
            if ("FAILED".equals(m.getDecommissionPhase())) {
                throw new AssertionError("decommission failed: " + m.getDecommissionDetail());
            }
            Thread.sleep(200);
        }
        throw new AssertionError("decommission did not complete within " + timeoutMs + "ms");
    }

    /** Cancel settles when the replica set is back to the original and no reassignment is pending. */
    private static void awaitCancelSettled(
            AdminGrpc.AdminBlockingStub admin, Broker b, List<Integer> expected, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            var s = b.topics().partitionState("ops", 0).orElse(null);
            var pending = admin.withDeadlineAfter(5, TimeUnit.SECONDS)
                    .listReassignments(ListReassignmentsRequest.newBuilder().build())
                    .getReassignmentsList();
            if (s != null
                    && s.replicas().size() == expected.size()
                    && s.replicas().containsAll(expected)
                    && pending.isEmpty()) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("cancel did not settle back to " + expected + " within " + timeoutMs + "ms");
    }

    private static void awaitTopicSettled(Broker b) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            var s = b.topics().partitionState("ops", 0).orElse(null);
            if (s != null && s.isr().size() == 2) return;
            Thread.sleep(50);
        }
        throw new AssertionError("ops topic did not settle within 10s");
    }

    private static void awaitSingleLeader(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (brokers.stream().filter(b -> b.role() == Role.LEADER).count() == 1) return;
            Thread.sleep(50);
        }
        throw new AssertionError("no single leader within 10s");
    }

    private static void awaitRegistryConvergence(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (brokers.stream()
                    .allMatch(b -> b.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3)))) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("broker registry did not converge within 5s");
    }

    private static Broker leaderOf(List<Broker> brokers) {
        return brokers.stream().filter(b -> b.role() == Role.LEADER).findFirst().orElseThrow();
    }
}
