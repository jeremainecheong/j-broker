package jbroker.bench;

import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import jbroker.broker.client.BrokerClient;
import org.HdrHistogram.Histogram;

/**
 * produce-path benchmark.
 *
 * <pre>
 *   j-broker-bench producer --broker HOST:PORT --topic T --partition N
 *                           --records N --payload-size BYTES
 *                           [--concurrency N] [--acks all] [--csv FILE]
 *                           [--tls-trust CA.crt --tls-cert C.crt --tls-key C.key]
 * </pre>
 *
 * <p>Default is single-producer, single-partition (concurrency=1). Each
 * record is a random payload of {@code --payload-size} bytes; the timed
 * region is {@code BrokerClient.produce()} / {@code produceAcksAll()}.
 * Latency samples feed into an {@link Histogram} with 3-significant-digit
 * precision; the final report prints p50 / p99 / p999 / max + throughput.
 *
 * <p>{@code --concurrency N} spins N virtual-thread workers,
 * each with its own {@link BrokerClient} (distinct TCP socket so the
 * broker's acceptor is actually under fan-in), cooperatively dividing
 * the record budget. Single-threaded RTT bench numbers are bounded by
 * {@code 1 / p50}; the broker's throughput capacity only shows up under
 * fan-in. Histograms are merged across workers so the final percentile
 * table reflects the full distribution.
 */
public final class ProducerPerfTest {

    private ProducerPerfTest() {}

    static void run(String[] args) throws Exception {
        String broker = BenchArgs.get(args, "--broker", "127.0.0.1:9092");
        String topic = BenchArgs.get(args, "--topic", null);
        int partition = BenchArgs.getInt(args, "--partition", 0);
        // optional fan-out across partitions. Worker w produces
        // to (basePartition + w) % partitionsSpan. Defaults to 1 so the
        // single-partition bench shape is preserved for existing callers.
        int partitionsSpan = Math.max(1, BenchArgs.getInt(args, "--partitions", 1));
        int records = BenchArgs.getInt(args, "--records", 10_000);
        int payloadSize = BenchArgs.getInt(args, "--payload-size", 256);
        int concurrency = Math.max(1, BenchArgs.getInt(args, "--concurrency", 1));
        boolean acksAll = "all".equals(BenchArgs.get(args, "--acks", null));
        String csvStr = BenchArgs.get(args, "--csv", null);
        Path csv = csvStr == null ? null : Path.of(csvStr);
        var tls = BenchArgs.tlsFromArgs(args);

        if (topic == null) {
            System.err.println("--topic required");
            System.exit(2);
            return;
        }

        int colon = broker.indexOf(':');
        String host = broker.substring(0, colon);
        int port = Integer.parseInt(broker.substring(colon + 1));

        // Pre-fill a pool of random payloads so payload allocation doesn't
        // dominate the measured region. Reuse round-robin across the run.
        byte[][] pool = new byte[64][];
        for (int i = 0; i < pool.length; i++) {
            pool[i] = new byte[payloadSize];
            ThreadLocalRandom.current().nextBytes(pool[i]);
        }

        // Fast single-threaded path — preserved for existing callers that
        // measure pure per-call RTT under the default --concurrency=1.
        if (concurrency == 1) {
            var histogram = new Histogram(TimeUnitNanos.ONE_MINUTE, 3);
            try (var client = new BrokerClient(host, port, tls)) {
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
            return;
        }

        // Multi-worker path. Each worker owns its own BrokerClient so the
        // broker's acceptor is under real TCP fan-in (one connection per
        // concurrent producer). Histograms merge at the end.
        int perWorker = records / concurrency;
        int spillover = records - perWorker * concurrency; // hand to worker 0
        var merged = new Histogram(TimeUnitNanos.ONE_MINUTE, 3);
        var produced = new AtomicInteger(0);
        long start = System.nanoTime();

        try (var exec = Executors.newVirtualThreadPerTaskExecutor()) {
            var tasks = new java.util.ArrayList<Callable<Histogram>>();
            for (int w = 0; w < concurrency; w++) {
                final int myBudget = perWorker + (w == 0 ? spillover : 0);
                final int myPartition = (partition + w) % partitionsSpan;
                tasks.add(() -> {
                    var local = new Histogram(TimeUnitNanos.ONE_MINUTE, 3);
                    try (var client = new BrokerClient(host, port, tls)) {
                        // Per-worker warmup: 2% of its own budget, floored.
                        int warmup = Math.max(20, myBudget / 50);
                        for (int i = 0; i < warmup; i++) {
                            var payload = pool[i % pool.length];
                            if (acksAll) client.produceAcksAll(topic, myPartition, payload);
                            else client.produce(topic, myPartition, payload);
                        }
                        for (int i = 0; i < myBudget; i++) {
                            var payload = pool[i % pool.length];
                            long t0 = System.nanoTime();
                            if (acksAll) client.produceAcksAll(topic, myPartition, payload);
                            else client.produce(topic, myPartition, payload);
                            local.recordValue(System.nanoTime() - t0);
                            produced.incrementAndGet();
                        }
                    }
                    return local;
                });
            }
            for (var f : exec.invokeAll(tasks)) merged.add(f.get());
        }
        long elapsed = System.nanoTime() - start;
        long totalBytes = (long) produced.get() * payloadSize;
        PerfReport.emit("producer(c=" + concurrency + ")", merged, produced.get(), totalBytes, elapsed, csv);
    }

    /** Nanos in a minute — covers the widest plausible per-op latency we'd ever want to record. */
    private static final class TimeUnitNanos {
        static final long ONE_MINUTE = 60L * 1_000_000_000L;
    }
}
