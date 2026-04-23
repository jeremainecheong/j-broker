package jbroker.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
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
 * PRD §11.3 E2E-10-2 — with a JFR recording capturing
 * {@code jdk.VirtualThreadPinned} during concurrent produce load, the
 * emitted event count on hot paths is zero.
 *
 * <p>The P10.2 ReentrantLock migration on {@link jbroker.storage.Log} +
 * {@link jbroker.storage.LogSegment} is the mechanism; this test is the
 * acceptance gate.
 */
class E2E_10_2_VirtualThreadPinningIT {

    @Test
    void producePathEmitsZeroVirtualThreadPinnedEvents(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
        int r1 = freePort(), r2 = freePort(), r3 = freePort();
        int b1 = freePort(), b2 = freePort(), b3 = freePort();
        var voters = List.of(
                new VoterAddress(new NodeId(1), "127.0.0.1", r1, b1),
                new VoterAddress(new NodeId(2), "127.0.0.1", r2, b2),
                new VoterAddress(new NodeId(3), "127.0.0.1", r3, b3));

        Path jfrPath = Files.createTempFile("jbroker-e2e-10-2-", ".jfr");
        var recording = new Recording();
        // jdk.VirtualThreadPinned is emitted when a virtual thread parks
        // while holding a monitor — the exact condition the P10.2 change
        // eliminates. Threshold 0ms so every pinning event is captured.
        recording.enable("jdk.VirtualThreadPinned").withThreshold(java.time.Duration.ZERO);
        recording.setDestination(jfrPath);
        recording.start();
        try (var br1 = Broker.start(new Broker.Config(new NodeId(1), d1, r1, b1, voters));
                var br2 = Broker.start(new Broker.Config(new NodeId(2), d2, r2, b2, voters));
                var br3 = Broker.start(new Broker.Config(new NodeId(3), d3, r3, b3, voters))) {
            waitForClusterReady(br1, br2, br3);
            int leaderPort = br1.role() == Role.LEADER ? b1 : br2.role() == Role.LEADER ? b2 : b3;

            try (var client = new BrokerClient("127.0.0.1", leaderPort)) {
                client.createTopic("pinning", 1, 3);
                long deadline = System.currentTimeMillis() + 5_000;
                while (!(br1.topics().exists("pinning")
                                && br2.topics().exists("pinning")
                                && br3.topics().exists("pinning"))
                        && System.currentTimeMillis() < deadline) {
                    Thread.sleep(25);
                }
            }
            // Drive 200 concurrent produces on virtual threads so the
            // Log.append path is on virtual-thread carriers the whole time.
            try (var exec = Executors.newVirtualThreadPerTaskExecutor()) {
                var tasks = new java.util.ArrayList<Callable<Long>>();
                for (int i = 0; i < 200; i++) {
                    final int idx = i;
                    tasks.add(() -> {
                        try (var c = new BrokerClient("127.0.0.1", leaderPort)) {
                            return c.produce("pinning", 0, ("v" + idx).getBytes(StandardCharsets.UTF_8));
                        }
                    });
                }
                for (var f : exec.invokeAll(tasks)) f.get();
            }
        } finally {
            recording.stop();
            recording.close();
        }

        int pinnedCount = 0;
        try (var rf = new RecordingFile(jfrPath)) {
            while (rf.hasMoreEvents()) {
                var ev = rf.readEvent();
                if ("jdk.VirtualThreadPinned".equals(ev.getEventType().getName())) {
                    pinnedCount++;
                }
            }
        }
        Files.deleteIfExists(jfrPath);

        // The DoD is zero pinning events on the produce hot path. gRPC
        // server internals may emit rare pinning events outside our
        // control (e.g. native netty code during channel accept) — the
        // threshold here is intentionally strict; if Netty adds a
        // known-benign pin we can widen it to e.g. ≤ 2 with an inline
        // comment explaining the waiver.
        assertThat(pinnedCount)
                .as("jdk.VirtualThreadPinned events captured during 200 concurrent produces")
                .isZero();
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
