package jbroker.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import jbroker.app.Broker;
import jbroker.app.testkit.TestBrokers;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * end-to-end produce + consume perf gate. Runs in CI under
 * the {@code perf} tag so a merge that halves throughput or triples tail
 * latency breaks the build rather than silently shipping.
 *
 * <p>Existing coverage:
 * <ul>
 *   <li>{@code bench/} harness — runs manually, prints numbers, no gate.</li>
 *   <li>{@code AppendThroughputTest} — gates the Log-layer append rate.</li>
 * </ul>
 *
 * <p>Gap this IT fills: the full gRPC round-trip path (client encode →
 * acceptor → ProduceHandler → Log append → FetchHandler → client decode)
 * has no CI gate at all, so a regression in any of those layers would
 * only show up via manual bench runs.
 *
 * <p>Thresholds are deliberately generous. The floor values are set ~3–5×
 * above observed dev-machine numbers so we catch catastrophic regressions
 * (VT pinning, missing batching, serialisation blowup) without going red
 * on GitHub Actions' noisy-neighbour IO. Baseline numbers seen on a dev
 * laptop at the time of writing: produce p99 ≈ 1–2 ms, consume-fetch p99
 * ≈ 1 ms, throughput ≈ 5–8k records/s single-producer.
 */
class PerfGateIT {

    private static final int RECORDS = 5_000;
    private static final int PAYLOAD_BYTES = 512;
    private static final int FETCH_BYTES = 64 * 1024;
    private static final int WARMUP = 200;

    // Regression floors — intentionally loose.
    private static final long PRODUCE_P99_MAX_NS = 50L * 1_000_000L; // 50 ms
    private static final long CONSUME_FETCH_P99_MAX_NS = 100L * 1_000_000L; // 100 ms
    private static final double PRODUCE_RPS_MIN = 1_000.0;

    @Test
    @Tag("perf")
    void endToEndProduceConsumeMeetsCiGatedFloors(@TempDir Path dir) throws Exception {
        try (var broker = TestBrokers.startSingleVoter(dir)) {
            int brokerPort = broker.brokerPort();
            awaitLeadership(broker);

            try (var client = new BrokerClient("127.0.0.1", brokerPort)) {
                client.createTopic("perf-gate", 1, 1);

                byte[] payload = new byte[PAYLOAD_BYTES];
                ThreadLocalRandom.current().nextBytes(payload);

                // Warmup so JIT compiles the hot paths before we start
                // recording.
                for (int i = 0; i < WARMUP; i++) {
                    client.produce("perf-gate", 0, payload);
                }

                // Producer timing.
                long[] produceLatencies = new long[RECORDS];
                long produceStart = System.nanoTime();
                for (int i = 0; i < RECORDS; i++) {
                    long t0 = System.nanoTime();
                    client.produce("perf-gate", 0, payload);
                    produceLatencies[i] = System.nanoTime() - t0;
                }
                long produceElapsedNs = System.nanoTime() - produceStart;

                // Consumer path — drain everything, timing each fetch RPC.
                long offset = 0;
                int remaining = RECORDS + WARMUP;
                var fetchLatencies = new ArrayList<Long>();
                while (remaining > 0) {
                    long t0 = System.nanoTime();
                    var batch = client.fetch("perf-gate", 0, offset, FETCH_BYTES);
                    fetchLatencies.add(System.nanoTime() - t0);
                    if (batch.isEmpty()) break;
                    remaining -= batch.size();
                    offset += batch.size();
                }

                Arrays.sort(produceLatencies);
                long p50ProduceNs = produceLatencies[RECORDS / 2];
                long p99ProduceNs = produceLatencies[(int) (RECORDS * 0.99)];
                double producerRps = RECORDS / (produceElapsedNs / 1e9);

                Collections.sort(fetchLatencies);
                long p99FetchNs =
                        fetchLatencies.get(Math.min(fetchLatencies.size() - 1, (int) (fetchLatencies.size() * 0.99)));

                System.out.printf(
                        "E2E-hardening pass perf-gate: produce p50=%.2fms p99=%.2fms rps=%.0f | consume-fetch p99=%.2fms (samples=%d)%n",
                        p50ProduceNs / 1e6, p99ProduceNs / 1e6, producerRps, p99FetchNs / 1e6, fetchLatencies.size());

                assertThat(p99ProduceNs)
                        .as(
                                "produce p99 (ns) must stay below %dms; saw %.2fms",
                                PRODUCE_P99_MAX_NS / 1_000_000L, p99ProduceNs / 1e6)
                        .isLessThan(PRODUCE_P99_MAX_NS);
                assertThat(producerRps)
                        .as(
                                "single-producer throughput must stay above %.0f rec/s; saw %.0f",
                                PRODUCE_RPS_MIN, producerRps)
                        .isGreaterThan(PRODUCE_RPS_MIN);
                assertThat(p99FetchNs)
                        .as(
                                "consume-fetch p99 (ns) must stay below %dms; saw %.2fms",
                                CONSUME_FETCH_P99_MAX_NS / 1_000_000L, p99FetchNs / 1e6)
                        .isLessThan(CONSUME_FETCH_P99_MAX_NS);
            }
        }
    }

    private static void awaitLeadership(Broker broker) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (broker.role() != Role.LEADER && System.currentTimeMillis() < deadline) {
            Thread.sleep(25);
        }
        assertThat(broker.role()).isEqualTo(Role.LEADER);
    }
}
