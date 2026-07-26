package jbroker.bench;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import jbroker.broker.client.BrokerClient;
import org.HdrHistogram.Histogram;

/**
 * Produce-path benchmark against an already-running broker. Emits one CSV
 * row: mode=producer, latency_kind=per_rpc — each histogram sample is one
 * produce RPC round trip, so with {@code --batch-size N} a sample covers
 * N records' worth of work.
 *
 * <pre>
 *   j-broker-bench producer --broker HOST:PORT --topic T --partition N
 *                           [--duration-s S | --records N] [--warmup-s S]
 *                           [--payload-size BYTES] [--concurrency N]
 *                           [--partitions N] [--batch-size N] [--acks all]
 *                           [--csv FILE]
 *                           [--tls-trust CA --tls-cert C --tls-key K]
 * </pre>
 *
 * <p>Throughput knobs, in order of impact:
 *
 * <ol>
 *   <li>{@code --batch-size N} — records per RPC. Amortizes fixed
 *       per-call cost (encode, gRPC, handler dispatch, Log-append lock
 *       acquire) across N records. Single-record RPCs are RTT-bound.</li>
 *   <li>{@code --concurrency N} — N virtual-thread producers each with
 *       its own BrokerClient (TCP socket). Breaks the serial-RTT cap.</li>
 *   <li>{@code --partitions N} — fan workers across N partitions so
 *       per-partition Log.append locks don't serialize.</li>
 * </ol>
 */
public final class ProducerPerfTest {

    private static final long MAX_LATENCY_NANOS = 60L * 1_000_000_000L;

    private ProducerPerfTest() {}

    private record WorkerResult(Histogram histogram, long records, long warmupRecords) {}

    static void run(String[] args) throws Exception {
        String broker = BenchArgs.get(args, "--broker", "127.0.0.1:9092");
        String topic = BenchArgs.get(args, "--topic", null);
        int partition = BenchArgs.getInt(args, "--partition", 0);
        int partitionsSpan = Math.max(1, BenchArgs.getInt(args, "--partitions", 1));
        int payloadSize = BenchArgs.getInt(args, "--payload-size", 256);
        int concurrency = Math.max(1, BenchArgs.getInt(args, "--concurrency", 1));
        int batchSize = Math.max(1, BenchArgs.getInt(args, "--batch-size", 1));
        boolean acksAll = "all".equals(BenchArgs.get(args, "--acks", null));
        var bounds = RunBounds.parse(args);
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

        TopicLabels labels;
        try (var client = new BrokerClient(host, port, tls)) {
            labels = TopicLabels.resolve(client, topic);
        }

        // Pre-fill a pool of random payloads so payload allocation doesn't
        // dominate the measured region. Reuse round-robin across the run.
        byte[][] pool = new byte[64][];
        for (int i = 0; i < pool.length; i++) {
            pool[i] = new byte[payloadSize];
            ThreadLocalRandom.current().nextBytes(pool[i]);
        }

        // Absolute deadlines shared by all workers: everyone warms until
        // the same instant, so the measured windows line up.
        long warmupDeadline = System.nanoTime() + bounds.warmupNanos();
        long measureDeadline = bounds.durationBounded() ? warmupDeadline + bounds.durationNanos() : Long.MAX_VALUE;
        long perWorker = bounds.durationBounded() ? -1 : bounds.recordBudget() / concurrency;
        long spillover = bounds.durationBounded() ? 0 : bounds.recordBudget() - perWorker * concurrency;

        var merged = new Histogram(MAX_LATENCY_NANOS, 3);
        long totalRecords = 0;
        long totalWarmup = 0;

        try (var exec = Executors.newVirtualThreadPerTaskExecutor()) {
            var tasks = new ArrayList<Callable<WorkerResult>>();
            for (int w = 0; w < concurrency; w++) {
                final long myBudget = perWorker < 0 ? -1 : perWorker + (w == 0 ? spillover : 0);
                final int myPartition = (partition + w) % partitionsSpan;
                tasks.add(() -> {
                    var local = new Histogram(MAX_LATENCY_NANOS, 3);
                    long warm = 0;
                    long sent = 0;
                    try (var client = new BrokerClient(host, port, tls)) {
                        while (System.nanoTime() < warmupDeadline) {
                            sendOne(client, topic, myPartition, pool, (int) warm * batchSize, batchSize, acksAll);
                            warm += batchSize;
                        }
                        long batchBudget = myBudget < 0 ? Long.MAX_VALUE : myBudget / batchSize;
                        for (long b = 0; b < batchBudget && System.nanoTime() < measureDeadline; b++) {
                            long t0 = System.nanoTime();
                            sendOne(client, topic, myPartition, pool, (int) (b % 64) * batchSize, batchSize, acksAll);
                            local.recordValue(Math.min(System.nanoTime() - t0, MAX_LATENCY_NANOS));
                            sent += batchSize;
                        }
                    }
                    return new WorkerResult(local, sent, warm);
                });
            }
            for (var f : exec.invokeAll(tasks)) {
                var r = f.get();
                merged.add(r.histogram());
                totalRecords += r.records();
                totalWarmup += r.warmupRecords();
            }
        }
        long elapsed = System.nanoTime() - warmupDeadline;

        PerfReport.row("producer")
                .latencyKind("per_rpc")
                .acks(acksAll ? "all" : "1")
                .partitions(labels.partitions())
                .replicationFactor(labels.replicationFactor())
                .minInsyncReplicas(labels.minInsyncReplicas())
                .payloadSize(payloadSize)
                .batchSize((long) batchSize)
                .warmupRecords(totalWarmup)
                .records(totalRecords)
                .bytes(totalRecords * payloadSize)
                .elapsedNanos(elapsed)
                .histogram(merged)
                .emit(csv);
    }

    /**
     * Send one RPC. For {@code batchSize==1} routes through the single-record
     * path so the no-batching baseline is preserved byte-for-byte; otherwise
     * builds a batch of {@code batchSize} records and calls
     * {@link BrokerClient#produceBatch}.
     */
    private static void sendOne(
            BrokerClient client, String topic, int partition, byte[][] pool, int seed, int batchSize, boolean acksAll) {
        if (batchSize == 1) {
            var payload = pool[Math.floorMod(seed, pool.length)];
            if (acksAll) client.produceAcksAll(topic, partition, payload);
            else client.produce(topic, partition, payload);
            return;
        }
        var batch = new ArrayList<byte[]>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            batch.add(pool[Math.floorMod(seed + i, pool.length)]);
        }
        if (acksAll) client.produceBatchAcksAll(topic, partition, batch);
        else client.produceBatch(topic, partition, batch);
    }
}
