package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Latency gate for the event-driven acks=all path. On loopback, a produce
 * that must replicate to all three brokers completes in single-digit
 * milliseconds: the leader's append wakes the long-polled replica
 * fetches, the followers' next fetches advance the HWM, and the HWM
 * advance wakes the parked produce — no fixed poll cadence anywhere on
 * the path. The polling design this replaced (25 ms follower tick
 * stacked on a 10 ms leader-side HWM poll) had a median around 60 ms,
 * so the 30 ms ceiling fails on any regression back to polling while
 * staying far above signal-path noise.
 */
class AcksAllSignalLatencyIT {

    // Shared CI runners schedule and context-switch several times slower
    // than a laptop; scale the ceiling like the sibling lifecycle ITs do.
    private static final int CI_MULT =
            "1".equals(System.getenv("JBROKER_CI")) || "true".equalsIgnoreCase(System.getenv("CI")) ? 4 : 1;

    private static final int WARMUP = 20;
    private static final int SAMPLES = 200;
    private static final long MEDIAN_MAX_MS = 30;

    @Test
    void acksAllMedianCompletesWithinTheSignalBudget(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        try (var cluster = TestBrokerCluster.start(
                3,
                2,
                (i, voters, ports) ->
                        new Broker.Config(new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters))) {
            var brokers = List.of(cluster.broker(0), cluster.broker(1), cluster.broker(2));
            awaitSingleLeader(brokers);
            awaitRegistryConvergence(brokers);

            var raftLeader = brokers.stream()
                    .filter(b -> b.role() == Role.LEADER)
                    .findFirst()
                    .orElseThrow();
            try (var client = new BrokerClient("127.0.0.1", raftLeader.brokerPort())) {
                client.createTopic("latency", 1, 3);

                // Warmup: settles the replica fetch loops into their parked
                // steady state and pages the produce path hot.
                for (int i = 0; i < WARMUP; i++) {
                    client.produceAcksAll("latency", 0, ("warm-" + i).getBytes(StandardCharsets.UTF_8));
                }

                long[] latenciesNanos = new long[SAMPLES];
                for (int i = 0; i < SAMPLES; i++) {
                    byte[] value = ("sample-" + i).getBytes(StandardCharsets.UTF_8);
                    long start = System.nanoTime();
                    client.produceAcksAll("latency", 0, value);
                    latenciesNanos[i] = System.nanoTime() - start;
                }

                Arrays.sort(latenciesNanos);
                long medianMs = latenciesNanos[SAMPLES / 2] / 1_000_000L;
                long p99Ms = latenciesNanos[(int) (SAMPLES * 0.99)] / 1_000_000L;
                System.out.printf(
                        "acks=all latency over %d serial produces: median=%d ms p99=%d ms (ceiling %d ms)%n",
                        SAMPLES, medianMs, p99Ms, MEDIAN_MAX_MS * CI_MULT);
                assertThat(medianMs)
                        .as("acks=all median over %d serial produces (p99 %d ms)", SAMPLES, p99Ms)
                        .isLessThan(MEDIAN_MAX_MS * CI_MULT);
            }
        }
    }

    private static void awaitSingleLeader(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000L * CI_MULT;
        while (System.currentTimeMillis() < deadline) {
            long leaders = brokers.stream().filter(b -> b.role() == Role.LEADER).count();
            if (leaders == 1) return;
            Thread.sleep(50);
        }
        throw new AssertionError("no single leader within the deadline");
    }

    private static void awaitRegistryConvergence(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000L * CI_MULT;
        while (System.currentTimeMillis() < deadline) {
            boolean allKnow = brokers.stream()
                    .allMatch(b -> b.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3)));
            if (allKnow) return;
            Thread.sleep(50);
        }
        throw new AssertionError("broker registry did not converge within the deadline");
    }
}
