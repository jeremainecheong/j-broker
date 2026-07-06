package jbroker.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
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
 * VT-pinning CI gate at bench scale.
 *
 * <p>{@link VirtualThreadPinningIT} satisfied the acceptance gate by
 * driving 200 concurrent produces and asserting zero
 * {@code jdk.VirtualThreadPinned} events. The audit flagged two gaps:
 *
 * <ol>
 *   <li>200 ops is well below the scale a real client workload reaches,
 *       so a pinning regression only emitting above some threshold
 *       wouldn't fail the existing test.</li>
 *   <li>Only the produce hot path was exercised — the fetch path
 *       (FetchHandler + its zero-copy transferTo) has never been
 *       explicitly gated against pinning.</li>
 * </ol>
 *
 * <p>This IT pushes both: 2000 produces and 2000 fetches on virtual
 * threads (chunked into 200-concurrent rounds so default-ulimit CI can
 * run it), with the JFR {@code jdk.VirtualThreadPinned} event recording
 * live for the entire run. Runs in CI by default so a regression in
 * either path fails the build.
 */
class VtPinningBenchScaleIT {

    private static final int CLIENTS_PER_ROUND = 200;
    private static final int ROUNDS = 10;
    private static final int TOTAL_PRODUCES = CLIENTS_PER_ROUND * ROUNDS;
    private static final int TOTAL_FETCHES = CLIENTS_PER_ROUND * ROUNDS;

    @Test
    void produceAndFetchHotPathsEmitZeroVirtualThreadPinnedEventsAtScale(
            @TempDir Path d1, @TempDir Path d2, @TempDir Path d3) throws Exception {
        Path jfrPath = Files.createTempFile("jbroker-hardening pass-", ".jfr");
        var recording = new Recording();
        recording.enable("jdk.VirtualThreadPinned").withThreshold(Duration.ZERO);
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
                client.createTopic("vt-bench", 1, 3);
                long deadline = System.currentTimeMillis() + 5_000;
                while (!(br1.topics().exists("vt-bench")
                                && br2.topics().exists("vt-bench")
                                && br3.topics().exists("vt-bench"))
                        && System.currentTimeMillis() < deadline) {
                    Thread.sleep(25);
                }
            }

            // Milestone 1 — produce storm. Chunked into 200-wide × 10-round
            // waves so we don't exhaust the kernel's ephemeral port pool
            // (TenThousandClientsIT runs the full 10k-concurrent shape only under an
            // explicitly bumped ulimit); 200 concurrent sockets is the
            // same envelope TenThousandClientsCiGradeIT uses for CI default `ulimit -n 1024`.
            try (var exec = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int round = 0; round < ROUNDS; round++) {
                    var tasks = new java.util.ArrayList<Callable<Long>>(CLIENTS_PER_ROUND);
                    for (int i = 0; i < CLIENTS_PER_ROUND; i++) {
                        final int idx = round * CLIENTS_PER_ROUND + i;
                        tasks.add(() -> {
                            try (var c = new BrokerClient("127.0.0.1", leaderPort)) {
                                return c.produce("vt-bench", 0, ("v" + idx).getBytes(StandardCharsets.UTF_8));
                            }
                        });
                    }
                    for (var f : exec.invokeAll(tasks)) f.get();
                }
            }

            // Milestone 2 — fetch storm. Same round shape. Exercises
            // FetchHandler + LogSegment.transferTo on virtual threads;
            // the session-cache allocation path runs for every fresh
            // connection.
            try (var exec = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int round = 0; round < ROUNDS; round++) {
                    var tasks = new java.util.ArrayList<Callable<Integer>>(CLIENTS_PER_ROUND);
                    for (int i = 0; i < CLIENTS_PER_ROUND; i++) {
                        tasks.add(() -> {
                            try (var c = new BrokerClient("127.0.0.1", leaderPort)) {
                                return c.fetch("vt-bench", 0, 0L, 64 * 1024).size();
                            }
                        });
                    }
                    for (var f : exec.invokeAll(tasks)) f.get();
                }
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

        assertThat(pinnedCount)
                .as("jdk.VirtualThreadPinned events during %d produces + %d fetches", TOTAL_PRODUCES, TOTAL_FETCHES)
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
}
