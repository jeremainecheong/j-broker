package jbroker.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import jbroker.app.Broker;
import jbroker.app.VoterAddress;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordingFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * PRD §11.3 E2E-9-2 — with a JFR recording active during produce load on
 * a 3-broker cluster, the recorded file contains at least one of each
 * Phase 9 custom event type.
 *
 * <p>We drive produce traffic through the broker client so ProduceLatency
 * and FetchLatency both get exercised (followers issue ReplicaFetch
 * against the leader, but ProduceLatency / FetchLatency cover the
 * client-facing paths). FsyncDuration fires on every segment force;
 * RaftTermChange fires during the initial election; PartitionLeaderChange
 * fires when the first partition is created; ReplicationLag fires when
 * a follower is behind by ≥ 10 records.
 */
class E2E_9_2_JfrEventEmissionIT {

    private static final Set<String> REQUIRED_EVENTS = Set.of(
            "jbroker.RaftTermChange",
            "jbroker.PartitionLeaderChange",
            "jbroker.FsyncDuration",
            "jbroker.ProduceLatency",
            "jbroker.FetchLatency",
            "jbroker.ReplicationLag");

    @Test
    void allSixJfrEventsEmittedUnderProduceLoad(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3) throws Exception {
        int r1 = freePort(), r2 = freePort(), r3 = freePort();
        int b1 = freePort(), b2 = freePort(), b3 = freePort();
        var voters = List.of(
                new VoterAddress(new NodeId(1), "127.0.0.1", r1, b1),
                new VoterAddress(new NodeId(2), "127.0.0.1", r2, b2),
                new VoterAddress(new NodeId(3), "127.0.0.1", r3, b3));

        Path jfrPath = Files.createTempFile("jbroker-e2e-9-2-", ".jfr");
        var recording = new Recording();
        recording.enable("jbroker.RaftTermChange");
        recording.enable("jbroker.PartitionLeaderChange");
        recording.enable("jbroker.FsyncDuration");
        recording.enable("jbroker.ProduceLatency");
        recording.enable("jbroker.FetchLatency");
        recording.enable("jbroker.ReplicationLag");
        recording.setDestination(jfrPath);
        recording.start();
        try (var br1 = Broker.start(new Broker.Config(new NodeId(1), d1, r1, b1, voters));
                var br2 = Broker.start(new Broker.Config(new NodeId(2), d2, r2, b2, voters));
                var br3 = Broker.start(new Broker.Config(new NodeId(3), d3, r3, b3, voters))) {
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

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
