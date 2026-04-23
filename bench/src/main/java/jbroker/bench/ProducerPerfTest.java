package jbroker.bench;

import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import jbroker.broker.client.BrokerClient;
import org.HdrHistogram.Histogram;

/**
 * produce-path benchmark.
 *
 * <pre>
 *   j-broker-bench producer --broker HOST:PORT --topic T --partition N
 *                           --records N --payload-size BYTES
 *                           [--acks all] [--csv FILE]
 * </pre>
 *
 * <p>Runs single-producer, single-partition. Each record is a random
 * payload of {@code --payload-size} bytes; the timed region is {@code
 * BrokerClient.produce()} / {@code produceAcksAll()}. Latency samples
 * feed into an {@link Histogram} with 3-significant-digit precision; the
 * final report prints p50 / p99 / p999 / max + throughput.
 *
 * <p>For multi-producer throughput numbers, invoke this in parallel via
 * the shell (one JVM per producer) and have each write to the same
 * {@code --csv FILE}. Multi-threaded in-process producers are a future
 * enhancement.
 */
public final class ProducerPerfTest {

    private ProducerPerfTest() {}

    static void run(String[] args) throws Exception {
        String broker = BenchArgs.get(args, "--broker", "127.0.0.1:9092");
        String topic = BenchArgs.get(args, "--topic", null);
        int partition = BenchArgs.getInt(args, "--partition", 0);
        int records = BenchArgs.getInt(args, "--records", 10_000);
        int payloadSize = BenchArgs.getInt(args, "--payload-size", 256);
        boolean acksAll = "all".equals(BenchArgs.get(args, "--acks", null));
        String csvStr = BenchArgs.get(args, "--csv", null);
        Path csv = csvStr == null ? null : Path.of(csvStr);

        if (topic == null) {
            System.err.println("--topic required");
            System.exit(2);
            return;
        }

        // Pre-fill a pool of random payloads so payload allocation doesn't
        // dominate the measured region. Reuse round-robin across the run.
        byte[][] pool = new byte[64][];
        for (int i = 0; i < pool.length; i++) {
            pool[i] = new byte[payloadSize];
            ThreadLocalRandom.current().nextBytes(pool[i]);
        }

        var histogram = new Histogram(TimeUnit.SECONDS_IN_NANOS * 60, 3); // up to 60s per-op
        int colon = broker.indexOf(':');
        try (var client = new BrokerClient(broker.substring(0, colon), Integer.parseInt(broker.substring(colon + 1)))) {
            // Quick warmup — 2% of the total, with a floor of 100.
            int warmup = Math.max(100, records / 50);
            for (int i = 0; i < warmup; i++) {
                var payload = pool[i % pool.length];
                if (acksAll) client.produceAcksAll(topic, partition, payload);
                else client.produce(topic, partition, payload);
            }

            long totalBytes = 0;
            long start = System.nanoTime();
            for (int i = 0; i < records; i++) {
                var payload = pool[i % pool.length];
                long t0 = System.nanoTime();
                if (acksAll) client.produceAcksAll(topic, partition, payload);
                else client.produce(topic, partition, payload);
                histogram.recordValue(System.nanoTime() - t0);
                totalBytes += payload.length;
            }
            long elapsed = System.nanoTime() - start;

            PerfReport.emit("producer", histogram, records, totalBytes, elapsed, csv);
        }
    }

    /** Nanos in a minute — covers the widest plausible per-op latency we'd ever want to record. */
    private static final class TimeUnit {
        static final long SECONDS_IN_NANOS = 60L * 1_000_000_000L;
    }
}
