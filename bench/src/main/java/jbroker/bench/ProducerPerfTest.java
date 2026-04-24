package jbroker.bench;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import jbroker.broker.client.BrokerClient;
import org.HdrHistogram.Histogram;

/**
 * P12.1 — produce-path benchmark.
 *
 * <pre>
 *   j-broker-bench producer --broker HOST:PORT --topic T --partition N
 *                           --records N --payload-size BYTES
 *                           [--concurrency N] [--partitions N]
 *                           [--batch-size N] [--acks all] [--csv FILE]
 *                           [--tls-trust CA --tls-cert C --tls-key K]
 * </pre>
 *
 * <p>Throughput knobs, in order of impact:
 *
 * <ol>
 *   <li>{@code --batch-size N} — records per RPC. Amortizes fixed
 *       per-call cost (encode, gRPC, handler dispatch, Log-append lock
 *       acquire) across N records. Single-record RPCs are RTT-bound;
 *       a batch size of 100 routinely unlocks 20–50× throughput.</li>
 *   <li>{@code --concurrency N} — N virtual-thread producers each with
 *       its own BrokerClient (TCP socket). Breaks the serial-RTT cap.</li>
 *   <li>{@code --partitions N} — fan workers across N partitions so
 *       per-partition Log.append locks don't serialize.</li>
 * </ol>
 *
 * <p>Latency samples reflect per-RPC duration; when {@code --batch-size}
 * is set, that's N records' worth of work per sample. Throughput
 * reported is records/s and bytes/s.
 */
public final class ProducerPerfTest {

    private ProducerPerfTest() {}

    static void run(String[] args) throws Exception {
        String broker = BenchArgs.get(args, "--broker", "127.0.0.1:9092");
        String topic = BenchArgs.get(args, "--topic", null);
        int partition = BenchArgs.getInt(args, "--partition", 0);
        int partitionsSpan = Math.max(1, BenchArgs.getInt(args, "--partitions", 1));
        int records = BenchArgs.getInt(args, "--records", 10_000);
        int payloadSize = BenchArgs.getInt(args, "--payload-size", 256);
        int concurrency = Math.max(1, BenchArgs.getInt(args, "--concurrency", 1));
        int batchSize = Math.max(1, BenchArgs.getInt(args, "--batch-size", 1));
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

        // Single-threaded path preserved for pure-RTT baseline runs.
        if (concurrency == 1) {
            var histogram = new Histogram(TimeUnitNanos.ONE_MINUTE, 3);
            try (var client = new BrokerClient(host, port, tls)) {
                int warmupBatches = Math.max(5, records / 50 / batchSize);
                for (int b = 0; b < warmupBatches; b++) {
                    sendOne(client, topic, partition, pool, b * batchSize, batchSize, acksAll);
                }

                long totalBytes = 0;
                long totalRecords = 0;
                int batches = records / batchSize;
                long start = System.nanoTime();
                for (int b = 0; b < batches; b++) {
                    long t0 = System.nanoTime();
                    sendOne(client, topic, partition, pool, b * batchSize, batchSize, acksAll);
                    histogram.recordValue(System.nanoTime() - t0);
                    totalBytes += (long) batchSize * payloadSize;
                    totalRecords += batchSize;
                }
                long elapsed = System.nanoTime() - start;
                String tag = batchSize == 1 ? "producer" : "producer(bs=" + batchSize + ")";
                PerfReport.emit(tag, histogram, totalRecords, totalBytes, elapsed, csv);
            }
            return;
        }

        // Multi-worker path.
        int perWorker = records / concurrency;
        int spillover = records - perWorker * concurrency;
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
                        int warmupBatches = Math.max(5, myBudget / 50 / batchSize);
                        for (int b = 0; b < warmupBatches; b++) {
                            sendOne(client, topic, myPartition, pool, b * batchSize, batchSize, acksAll);
                        }
                        int batches = myBudget / batchSize;
                        for (int b = 0; b < batches; b++) {
                            long t0 = System.nanoTime();
                            sendOne(client, topic, myPartition, pool, b * batchSize, batchSize, acksAll);
                            local.recordValue(System.nanoTime() - t0);
                            produced.addAndGet(batchSize);
                        }
                    }
                    return local;
                });
            }
            for (var f : exec.invokeAll(tasks)) merged.add(f.get());
        }
        long elapsed = System.nanoTime() - start;
        long totalBytes = (long) produced.get() * payloadSize;
        String tag = "producer(c=" + concurrency + (batchSize > 1 ? " bs=" + batchSize : "") + ")";
        PerfReport.emit(tag, merged, produced.get(), totalBytes, elapsed, csv);
    }

    /**
     * Send one RPC. For {@code batchSize==1} routes through the single-record
     * path so the pre-batching bench baseline is preserved byte-for-byte;
     * otherwise builds a batch of {@code batchSize} records and calls
     * {@link BrokerClient#produceBatch}.
     */
    private static void sendOne(
            BrokerClient client, String topic, int partition, byte[][] pool, int seed, int batchSize, boolean acksAll) {
        if (batchSize == 1) {
            var payload = pool[seed % pool.length];
            if (acksAll) client.produceAcksAll(topic, partition, payload);
            else client.produce(topic, partition, payload);
            return;
        }
        var batch = new ArrayList<byte[]>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            batch.add(pool[(seed + i) % pool.length]);
        }
        if (acksAll) client.produceBatchAcksAll(topic, partition, batch);
        else client.produceBatch(topic, partition, batch);
    }

    /** Nanos in a minute — covers the widest plausible per-op latency we'd ever want to record. */
    private static final class TimeUnitNanos {
        static final long ONE_MINUTE = 60L * 1_000_000_000L;
    }
}
