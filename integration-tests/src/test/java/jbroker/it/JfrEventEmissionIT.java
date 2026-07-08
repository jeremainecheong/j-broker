package jbroker.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import jbroker.app.Broker;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordingFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * With a JFR recording active during produce load on
 * a 3-broker cluster, the recorded file contains at least one of each
 * custom event type.
 *
 * <p>We drive produce traffic through the broker client so ProduceLatency
 * and FetchLatency both get exercised (followers issue ReplicaFetch
 * against the leader, but ProduceLatency / FetchLatency cover the
 * client-facing paths). FsyncDuration fires on every segment force;
 * RaftTermChange fires during the initial election; PartitionLeaderChange
 * fires when the first partition is created; ReplicationLag fires when
 * a follower is behind by ≥ 10 records.
 */
class JfrEventEmissionIT {

    private static final Set<String> REQUIRED_EVENTS = Set.of(
            "jbroker.RaftTermChange",
            "jbroker.PartitionLeaderChange",
            "jbroker.FsyncDuration",
            "jbroker.ProduceLatency",
            "jbroker.FetchLatency",
            "jbroker.ReplicationLag");

    @Test
    void allSixJfrEventsEmittedUnderProduceLoad(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3) throws Exception {
        Path jfrPath = Files.createTempFile("jbroker-jfr-emission-", ".jfr");
        var recording = new Recording();
        recording.enable("jbroker.RaftTermChange");
        recording.enable("jbroker.PartitionLeaderChange");
        recording.enable("jbroker.FsyncDuration");
        recording.enable("jbroker.ProduceLatency");
        recording.enable("jbroker.FetchLatency");
        recording.enable("jbroker.ReplicationLag");
        recording.setDestination(jfrPath);
        recording.start();
        var dirs = new Path[] {d1, d2, d3};
        try (var cluster = TestBrokerCluster.start(
                3,
                2,
                (i, voters, ports) ->
                        new Broker.Config(new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters))) {
            var br1 = cluster.broker(0);
            var br2 = cluster.broker(1);
            var br3 = cluster.broker(2);
            int b1 = cluster.brokerPort(0), b2 = cluster.brokerPort(1), b3 = cluster.brokerPort(2);
            waitForClusterReady(br1, br2, br3);
            int leaderPort = br1.role() == Role.LEADER ? b1 : br2.role() == Role.LEADER ? b2 : b3;

            try (var client = new BrokerClient("127.0.0.1", leaderPort)) {
                client.createTopic("orders", 1, 3);
                // Small polling loop so the topic record is committed cluster-wide.
                long deadline = System.currentTimeMillis() + 5_000;
                while (!(br1.topics().exists("orders")
                                && br2.topics().exists("orders")
                                && br3.topics().exists("orders"))
                        && System.currentTimeMillis() < deadline) {
                    Thread.sleep(25);
                }
                // Produce enough records to trigger a segment flush + create
                // lag between leader and a follower that just came online.
                for (int i = 0; i < 200; i++) {
                    byte[] value = ("v" + i).getBytes(StandardCharsets.UTF_8);
                    client.produce("orders", 0, value);
                }
                // Drive a Fetch to hit FetchLatencyEvent.
                client.fetch("orders", 0, 0L, 16 * 1024);
            }
        } finally {
            recording.stop();
            recording.close();
        }

        var seen = new HashSet<String>();
        try (var rf = new RecordingFile(jfrPath)) {
            while (rf.hasMoreEvents()) {
                var ev = rf.readEvent();
                seen.add(ev.getEventType().getName());
            }
        }
        assertThat(seen)
                .as("expected at least one of each jbroker.* custom event")
                .containsAll(REQUIRED_EVENTS);

        Files.deleteIfExists(jfrPath);
    }

    private static void waitForClusterReady(Broker b1, Broker b2, Broker b3) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            int leaders = (b1.role() == Role.LEADER ? 1 : 0)
                    + (b2.role() == Role.LEADER ? 1 : 0)
                    + (b3.role() == Role.LEADER ? 1 : 0);
            boolean full = b1.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3))
                    && b2.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3))
                    && b3.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3));
            if (leaders == 1 && full) return;
            Thread.sleep(50);
        }
        throw new IllegalStateException("cluster did not converge in 15s");
    }
}
