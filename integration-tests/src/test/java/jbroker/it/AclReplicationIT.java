package jbroker.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import jbroker.app.Broker;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R3.2 round trip: ACL entries proposed on the controller commit
 * through Raft and land in every broker's replicated cache; deletes
 * propagate the same way; non-leaders answer NOT_LEADER with the usual
 * hint envelope rather than silently dropping the mutation.
 */
class AclReplicationIT {

    @Test
    void aclMutationsReplicateToEveryBroker(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3) throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        try (var cluster = TestBrokerCluster.start(
                3,
                3,
                (i, voters, ports) ->
                        new Broker.Config(new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters))) {
            int leader = awaitLeader(cluster);

            try (var client = new BrokerClient("127.0.0.1", cluster.brokerPort(leader))) {
                client.createAcl("alice", "topic", "orders", false, "produce", true);
                client.createAcl("svc", "group", "readers-", true, "consume", true);
            }

            for (int i = 0; i < 3; i++) {
                awaitAclCount(cluster.brokerPort(i), 2);
            }

            // Non-leader mutations fail fast with the leader hint instead
            // of burning a propose timeout.
            int follower = (leader + 1) % 3;
            try (var client = new BrokerClient("127.0.0.1", cluster.brokerPort(follower))) {
                assertThatThrownBy(() -> client.createAcl("bob", "topic", "orders", false, "produce", true))
                        .hasMessageContaining("leader");
            }

            try (var client = new BrokerClient("127.0.0.1", cluster.brokerPort(leader))) {
                client.deleteAcl("alice", "topic", "orders", false, "produce");
            }
            for (int i = 0; i < 3; i++) {
                awaitAclCount(cluster.brokerPort(i), 1);
            }
            try (var client = new BrokerClient("127.0.0.1", cluster.brokerPort(0))) {
                var survivor = client.listAcls().get(0);
                assertThat(survivor.getPrincipal()).isEqualTo("svc");
                assertThat(survivor.getPrefix()).isTrue();
            }
        }
    }

    private static int awaitLeader(TestBrokerCluster cluster) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            for (int i = 0; i < 3; i++) {
                if (cluster.broker(i).role() == Role.LEADER) return i;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("no Raft leader within 15s");
    }

    private static void awaitAclCount(int brokerPort, int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        try (var client = new BrokerClient("127.0.0.1", brokerPort)) {
            int seen = -1;
            while (System.currentTimeMillis() < deadline) {
                seen = client.listAcls().size();
                if (seen == expected) return;
                Thread.sleep(100);
            }
            throw new AssertionError("broker on port " + brokerPort + " has " + seen + " ACLs, wanted " + expected);
        }
    }
}
