package jbroker.bench;

import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import jbroker.broker.client.BatchingProducer;
import jbroker.broker.client.BrokerClient;
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
 * </pre>
 *
 * <p>{@code --max-outstanding} bounds unacked records so an unbounded
 * duration run cannot queue millions of futures; the semaphore is the
 * only backpressure — {@code send()} itself never blocks on the network.
 */
public final class BatchProducerPerfTest {

    private static final long MAX_LATENCY_NANOS = 60L * 1_000_000_000L;

    private BatchProducerPerfTest() {}

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

        if (topic == null) {
            System.err.println("--topic required");
            System.exit(2);
            return;
        }

        int colon = broker.indexOf(':');
        String host = broker.substring(0, colon);
        int port = Integer.parseInt(broker.substring(colon + 1));

        byte[][] pool = new byte[64][];
        for (int i = 0; i < pool.length; i++) {
            pool[i] = new byte[payloadSize];
            ThreadLocalRandom.current().nextBytes(pool[i]);
        }

        var config = new BatchingProducer.Config(batchBytes, lingerMs, 120_000, 100, compression);
        try (var client = new BrokerClient(host, port, tls)) {
            var labels = TopicLabels.resolve(client, topic);
            var semaphore = new Semaphore(maxOutstanding);
            // Completions run on the producer's sender thread while a
            // late-attached callback can run on this thread — the
            // histogram must tolerate both writers.
            var histogram = new SynchronizedHistogram(MAX_LATENCY_NANOS, 3);
            var completed = new AtomicLong();
            var failed = new AtomicLong();

            try (var producer = BatchingProducer.create(client, config)) {
                long warm = 0;
                long warmupDeadline = System.nanoTime() + bounds.warmupNanos();
                while (System.nanoTime() < warmupDeadline) {
                    semaphore.acquire();
                    producer.send(topic, partition, pool[(int) (warm % pool.length)])
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
                    final long t0 = System.nanoTime();
                    producer.send(topic, partition, pool[(int) (sent % pool.length)])
                            .whenComplete((offset, error) -> {
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
                long delivered = sent - failed.get();
                if (failed.get() > 0) {
                    System.err.printf("warning: %d records failed delivery (excluded from counts)%n", failed.get());
                }

                PerfReport.row("batch-producer")
                        .latencyKind("per_record")
                        .acks("all")
                        .partitions(labels.partitions())
                        .replicationFactor(labels.replicationFactor())
                        .minInsyncReplicas(labels.minInsyncReplicas())
                        .payloadSize(payloadSize)
                        .batchSize((long) batchBytes)
                        .lingerMs(lingerMs)
                        .compression(compression.name().toLowerCase(Locale.ROOT))
                        .warmupRecords(warm)
                        .records(delivered)
                        .bytes(delivered * payloadSize)
                        .elapsedNanos(elapsed)
                        .histogram(histogram)
                        .emit(csv);
            }
        }
    }
}
