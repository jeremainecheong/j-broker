package jbroker.bench;

import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import jbroker.broker.client.BrokerClient;
import org.HdrHistogram.Histogram;

/**
 * multi-broker acks=all producer bench. Spins up an
 * in-process 3-broker cluster with RF=3, times produce calls that block
 * until every ISR member has replicated. The single-broker {@code
 * producer --acks all} mode only tests the handler's happy path; this
 * mode additionally exercises the leader's wait-for-HWM loop against a
 * real replication pipeline.
 *
 * <pre>
 *   j-broker-bench acks-all [--records N] [--payload-size B] [--csv FILE]
 * </pre>
 */
public final class AcksAllPerfTest {

    private static final long ONE_MINUTE_NANOS = 60L * 1_000_000_000L;

    private AcksAllPerfTest() {}

    static void run(String[] args) throws Exception {
        int records = BenchArgs.getInt(args, "--records", 2_000);
        int payloadSize = BenchArgs.getInt(args, "--payload-size", 512);
        String csvStr = BenchArgs.get(args, "--csv", null);
        Path csv = csvStr == null ? null : Path.of(csvStr);

        byte[] payload = new byte[payloadSize];
        ThreadLocalRandom.current().nextBytes(payload);

        try (var cluster = BenchCluster.start(3)) {
            try (var client = new BrokerClient("127.0.0.1", cluster.leaderPort)) {
                client.createTopic("bench-acks-all", 1, 3);
                // Wait for the partition leader to settle — otherwise the
                // first few produces race topic-creation propagation and
                // skew the histogram.
                Thread.sleep(500);

                // Warmup so HWM plumbing has flushed its first pass.
                int warmup = Math.max(50, records / 50);
                for (int i = 0; i < warmup; i++) {
                    client.produceAcksAll("bench-acks-all", 0, payload);
                }

                var histogram = new Histogram(ONE_MINUTE_NANOS, 3);
                long totalBytes = 0;
                long start = System.nanoTime();
                for (int i = 0; i < records; i++) {
                    long t0 = System.nanoTime();
                    client.produceAcksAll("bench-acks-all", 0, payload);
                    histogram.recordValue(System.nanoTime() - t0);
                    totalBytes += payload.length;
                }
                long elapsed = System.nanoTime() - start;

                PerfReport.emit("producer-acks-all", histogram, records, totalBytes, elapsed, csv);
            }
        }
    }
}
