package jbroker.bench;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import jbroker.broker.client.BatchingProducer;
import jbroker.broker.client.BrokerClient;
import jbroker.broker.client.ClusterClient;
import jbroker.storage.Compression;
import org.HdrHistogram.SynchronizedHistogram;

/**
 * Client-library batching bench: drives {@link BatchingProducer}
 * (idempotent, acks=all) the way an application would. Emits one CSV row:
 * mode=batch-producer, latency_kind=per_record — each histogram sample is
 * {@code send()} return → future completion for ONE record, so it
 * INCLUDES the linger wait and queueing behind earlier batches. Never
 * compare these samples against a per_rpc row.
 *
 * <pre>
 *   j-broker-bench batch-producer --broker HOST:PORT --topic T --partition N
 *                                 [--duration-s S | --records N] [--warmup-s S]
 *                                 [--payload-size BYTES]
 *                                 [--batch-bytes N] [--linger-ms MS]
 *                                 [--compression none|zstd]
 *                                 [--max-outstanding N] [--csv FILE]
 *                                 [--tls-trust CA --tls-cert C --tls-key K]
 *   j-broker-bench batch-producer --rf N [--partitions N] [--min-insync N]
 *                                 [--bootstrap H:P,H:P,...] ...
 * </pre>
 *
 * <p>With {@code --rf} (or {@code --bootstrap}/{@code BENCH_BOOTSTRAP})
 * the bench runs the replicated path instead: an in-process loopback
 * 3-broker cluster (or the listed external endpoints, refused unless at
 * least {@code --rf} are reachable), a fresh RF={@code --rf} topic, and
 * sends spread round-robin over {@code --partitions}. Rows from the two
 * paths share mode=batch-producer and are told apart by the
 * replication_factor column.
 *
 * <p>{@code --max-outstanding} bounds unacked records so an unbounded
 * duration run cannot queue millions of futures; the semaphore is the
 * only backpressure — {@code send()} itself never blocks on the network.
 */
public final class BatchProducerPerfTest {

    private static final long MAX_LATENCY_NANOS = 60L * 1_000_000_000L;

    private BatchProducerPerfTest() {}

    private record MeasuredRun(
            SynchronizedHistogram histogram, long warmupRecords, long delivered, long elapsedNanos) {}

    static void run(String[] args) throws Exception {
        String broker = BenchArgs.get(args, "--broker", "127.0.0.1:9092");
        String topic = BenchArgs.get(args, "--topic", null);
        int partition = BenchArgs.getInt(args, "--partition", 0);
        int payloadSize = BenchArgs.getInt(args, "--payload-size", 256);
        int batchBytes = BenchArgs.getInt(args, "--batch-bytes", 64 * 1024);
        long lingerMs = BenchArgs.getLong(args, "--linger-ms", 5);
        // Bounds the closed loop: per_record latency under saturation is
        // queueing-dominated and scales with this window, so publishable
        // rows must state it (it rides in the summary via warmup/records
        // and in the flag defaults documented in bench/README.md).
        int maxOutstanding = BenchArgs.getInt(args, "--max-outstanding", 10_000);
        var compression =
                Compression.valueOf(BenchArgs.get(args, "--compression", "none").toUpperCase(Locale.ROOT));
        var bounds = RunBounds.parse(args);
        String csvStr = BenchArgs.get(args, "--csv", null);
        Path csv = csvStr == null ? null : Path.of(csvStr);
        var tls = BenchArgs.tlsFromArgs(args);
        int rf = BenchArgs.getInt(args, "--rf", -1);
        String bootstrap = BenchArgs.get(args, "--bootstrap", System.getenv("BENCH_BOOTSTRAP"));

        var config = new BatchingProducer.Config(batchBytes, lingerMs, 120_000, 100, compression);
        byte[][] pool = new byte[64][];
        for (int i = 0; i < pool.length; i++) {
            pool[i] = new byte[payloadSize];
            ThreadLocalRandom.current().nextBytes(pool[i]);
        }

        boolean clusterMode = rf > 0 || (bootstrap != null && !bootstrap.isBlank());
        if (clusterMode) {
            runReplicated(
                    args,
                    rf > 0 ? rf : 3,
                    bootstrap,
                    config,
                    pool,
                    payloadSize,
                    maxOutstanding,
                    bounds,
                    csv,
                    batchBytes,
                    lingerMs,
                    compression);
            return;
        }

        if (topic == null) {
            System.err.println("--topic required");
            System.exit(2);
            return;
        }

        int colon = broker.indexOf(':');
        String host = broker.substring(0, colon);
        int port = Integer.parseInt(broker.substring(colon + 1));

        try (var client = new BrokerClient(host, port, tls)) {
            var labels = TopicLabels.resolve(client, topic);
            try (var producer = BatchingProducer.create(client, config)) {
                var run = measure(producer, topic, partition, 1, pool, maxOutstanding, bounds);
                emit(
                        run,
                        labels.partitions(),
                        labels.replicationFactor(),
                        labels.minInsyncReplicas(),
                        payloadSize,
                        batchBytes,
                        lingerMs,
                        compression,
                        csv);
            }
        }
    }

    private static void runReplicated(
            String[] args,
            int rf,
            String bootstrap,
            BatchingProducer.Config config,
            byte[][] pool,
            int payloadSize,
            int maxOutstanding,
            RunBounds bounds,
            Path csv,
            int batchBytes,
            long lingerMs,
            Compression compression)
            throws Exception {
        int partitions = Math.max(1, BenchArgs.getInt(args, "--partitions", 1));
        int minInsync = BenchArgs.getInt(args, "--min-insync", -1);
        // Unique per run: external clusters keep topics across invocations.
        String topic = "bench-cluster-batch-" + System.currentTimeMillis();

        BenchCluster cluster = null;
        List<String> endpoints;
        try {
            if (bootstrap != null && !bootstrap.isBlank()) {
                endpoints = java.util.Arrays.stream(bootstrap.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .toList();
                AcksAllPerfTest.requireReachable(endpoints, rf);
            } else {
                cluster = BenchCluster.start(3);
                endpoints = new ArrayList<>();
                for (int p : cluster.brokerPorts) endpoints.add("127.0.0.1:" + p);
            }
            AcksAllPerfTest.createTopic(endpoints, topic, partitions, rf, minInsync);
            try (var client = new ClusterClient(endpoints);
                    var producer = BatchingProducer.create(client, config)) {
                var run = measure(producer, topic, 0, partitions, pool, maxOutstanding, bounds);
                emit(
                        run,
                        partitions,
                        rf,
                        minInsync > 0 ? minInsync : null,
                        payloadSize,
                        batchBytes,
                        lingerMs,
                        compression,
                        csv);
            }
        } finally {
            if (cluster != null) cluster.close();
        }
    }

    private static MeasuredRun measure(
            BatchingProducer producer,
            String topic,
            int fixedPartition,
            int partitionSpan,
            byte[][] pool,
            int maxOutstanding,
            RunBounds bounds)
            throws Exception {
        var semaphore = new Semaphore(maxOutstanding);
        // Completions run on the producer's sender thread while a
        // late-attached callback can run on this thread — the
        // histogram must tolerate both writers.
        var histogram = new SynchronizedHistogram(MAX_LATENCY_NANOS, 3);
        var completed = new AtomicLong();
        var failed = new AtomicLong();

        long warm = 0;
        long warmupDeadline = System.nanoTime() + bounds.warmupNanos();
        while (System.nanoTime() < warmupDeadline) {
            semaphore.acquire();
            int p = partitionSpan > 1 ? (int) (warm % partitionSpan) : fixedPartition;
            producer.send(topic, p, pool[(int) (warm % pool.length)])
                    .whenComplete((offset, error) -> semaphore.release());
            warm++;
        }
        // Drain warmup batches so no warmup completion can land in
        // the measured window. The drain can take a while with a
        // deep outstanding window, so the measured window is
        // anchored AFTER it — otherwise the drain would eat the
        // measured duration.
        producer.flush();

        long sent = 0;
        long budget = bounds.durationBounded() ? Long.MAX_VALUE : bounds.recordBudget();
        long start = System.nanoTime();
        long measureDeadline = bounds.durationBounded() ? start + bounds.durationNanos() : Long.MAX_VALUE;
        while (sent < budget && System.nanoTime() < measureDeadline) {
            semaphore.acquire();
            int p = partitionSpan > 1 ? (int) (sent % partitionSpan) : fixedPartition;
            final long t0 = System.nanoTime();
            producer.send(topic, p, pool[(int) (sent % pool.length)]).whenComplete((offset, error) -> {
                if (error == null) {
                    histogram.recordValue(Math.min(System.nanoTime() - t0, MAX_LATENCY_NANOS));
                } else {
                    failed.incrementAndGet();
                }
                completed.incrementAndGet();
                semaphore.release();
            });
            sent++;
        }
        producer.flush();
        // flush() joins the record futures; the whenComplete stages
        // recording into the histogram may still be a few
        // instructions behind, so wait for the counter to agree.
        long drainDeadline = System.currentTimeMillis() + 60_000;
        while (completed.get() < sent && System.currentTimeMillis() < drainDeadline) {
            Thread.sleep(1);
        }
        long elapsed = System.nanoTime() - start;
        if (failed.get() > 0) {
            System.err.printf("warning: %d records failed delivery (excluded from counts)%n", failed.get());
        }
        return new MeasuredRun(histogram, warm, sent - failed.get(), elapsed);
    }

    private static void emit(
            MeasuredRun run,
            Integer partitions,
            Integer replicationFactor,
            Integer minInsyncReplicas,
            int payloadSize,
            int batchBytes,
            long lingerMs,
            Compression compression,
            Path csv)
            throws Exception {
        PerfReport.row("batch-producer")
                .latencyKind("per_record")
                .acks("all")
                .partitions(partitions)
                .replicationFactor(replicationFactor)
                .minInsyncReplicas(minInsyncReplicas)
                .payloadSize(payloadSize)
                .batchSize((long) batchBytes)
                .lingerMs(lingerMs)
                .compression(compression.name().toLowerCase(Locale.ROOT))
                .warmupRecords(run.warmupRecords())
                .records(run.delivered())
                .bytes(run.delivered() * payloadSize)
                .elapsedNanos(run.elapsedNanos())
                .histogram(run.histogram())
                .emit(csv);
    }
}
