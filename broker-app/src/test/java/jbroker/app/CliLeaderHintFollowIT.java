package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The CLI verbs that must land on a specific broker — {@code topics
 * create} on the controller, {@code produce} on the partition leader —
 * follow the broker's "leader is broker N" refusal once instead of
 * failing when aimed at the wrong broker. Real 3-broker cluster, real
 * gRPC: the wrong broker refuses, the CLI resolves the named broker via
 * the reached broker's cluster view, redials, and the operation lands.
 */
class CliLeaderHintFollowIT {

    @Test
    void createAndProduceAimedAtTheWrongBrokerFollowTheRefusalToTheRightOne(
            @TempDir Path d1, @TempDir Path d2, @TempDir Path d3) throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        try (var cluster = TestBrokerCluster.start(
                3,
                2,
                (i, voters, ports) ->
                        new Broker.Config(new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters))) {
            var brokers = cluster.brokers();
            awaitSingleLeader(brokers);
            awaitRegistryConvergence(brokers);

            int controllerIdx = controllerIndex(brokers);
            int wrongIdx = (controllerIdx + 1) % 3;

            // -- topics create aimed at a non-controller --
            String wrongAddr = "127.0.0.1:" + cluster.brokerPort(wrongIdx);
            BrokerApp.createTopicFollowingHint(wrongAddr, "hinted", 3, 3);

            try (var probe = new BrokerClient("127.0.0.1", cluster.brokerPort(controllerIdx))) {
                assertThat(probe.describeTopic("hinted").getPartitions())
                        .as("create aimed at a non-controller must land via the followed refusal")
                        .isEqualTo(3);
            }

            // -- produce aimed at a broker that does not lead the partition --
            // Every broker must have converged on the same leader first: a
            // broker without the topic's metadata answers UNKNOWN_TOPIC,
            // which carries no destination and must not trigger a redial.
            int leaderId = awaitAgreedPartitionLeader(brokers, "hinted", 0);
            int notLeaderIdx = leaderId % 3; // ids are 1-based: broker (id % 3) + 1 != id
            assertThat(notLeaderIdx + 1).isNotEqualTo(leaderId);

            var lines = new BufferedReader(new StringReader("h-1\nh-2\nh-3\n"));
            BrokerApp.produceLines("127.0.0.1:" + cluster.brokerPort(notLeaderIdx), "hinted", 0, lines);

            try (var reader = new BrokerClient("127.0.0.1", cluster.brokerPort(leaderId - 1))) {
                assertThat(reader.fetch("hinted", 0, 0, 1 << 20))
                        .as("all lines land on the real leader, including the one that was refused")
                        .containsExactly(
                                "h-1".getBytes(StandardCharsets.UTF_8),
                                "h-2".getBytes(StandardCharsets.UTF_8),
                                "h-3".getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    private static int controllerIndex(List<Broker> brokers) {
        for (int i = 0; i < brokers.size(); i++) {
            if (brokers.get(i).role() == Role.LEADER) return i;
        }
        throw new AssertionError("no controller among the brokers");
    }

    /**
     * Partition metadata propagates asynchronously after create; wait
     * until every broker reports the same live leader and return its id.
     */
    private static int awaitAgreedPartitionLeader(List<Broker> brokers, String topic, int partition)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            var leaders = brokers.stream()
                    .map(b -> b.topics().partitionState(topic, partition))
                    .map(s -> s.map(st -> st.leader()).orElse(-1))
                    .distinct()
                    .toList();
            if (leaders.size() == 1 && leaders.get(0) > 0) return leaders.get(0);
            Thread.sleep(50);
        }
        throw new AssertionError("brokers did not agree on a leader for " + topic + "-" + partition + " within 10s");
    }

    private static void awaitSingleLeader(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (brokers.stream().filter(b -> b.role() == Role.LEADER).count() == 1) return;
            Thread.sleep(50);
        }
        throw new AssertionError("no single Raft leader within 10s");
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
}
