package jbroker.bench;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import jbroker.broker.client.BrokerClient;
import org.HdrHistogram.Histogram;

/**
 * Batched-RPC produce bench: drives
 * {@link BrokerClient#produceBatch(String, int, List)} with N records per
 * RPC. Emits one CSV row: mode=produce-batch, latency_kind=per_rpc —
 * each histogram sample is one round trip carrying
 * {@code --batch-records} records, so per-RPC latency here is NOT
 * per-record latency and must never be compared against a per_record row.
 *
 * <pre>
 *   j-broker-bench produce-batch --broker HOST:PORT --topic T --partition N
 *                                [--duration-s S | --records N] [--warmup-s S]
 *                                [--payload-size BYTES]
 *                                [--batch-records N] [--acks all]
 *                                [--csv FILE]
 *                                [--tls-trust CA --tls-cert C --tls-key K]
 * </pre>
 */
public final class ProduceBatchPerfTest {

    private static final long MAX_LATENCY_NANOS = 60L * 1_000_000_000L;

    private ProduceBatchPerfTest() {}

    static void run(String[] args) throws Exception {
        String broker = BenchArgs.get(args, "--broker", "127.0.0.1:9092");
        String topic = BenchArgs.get(args, "--topic", null);
        int partition = BenchArgs.getInt(args, "--partition", 0);
        int payloadSize = BenchArgs.getInt(args, "--payload-size", 256);
        int batchRecords = Math.max(1, BenchArgs.getInt(args, "--batch-records", 500));
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

        // The broker rejects encoded batches above the topic's
        // max.message.bytes (cluster default 1 MiB); cap the record count
        // so the requested payload size always ships, and report the
        // EFFECTIVE per-RPC count in the batch_size column.
        int maxBatchBytes = BenchArgs.getInt(args, "--max-batch-bytes", 1024 * 1024);
        int effectiveBatchRecords = capToEncodedSize(batchRecords, payloadSize, maxBatchBytes);
        if (effectiveBatchRecords < batchRecords) {
            System.err.printf(
                    "warning: --batch-records %d exceeds --max-batch-bytes %d at payload %dB; capped to %d records/RPC%n",
                    batchRecords, maxBatchBytes, payloadSize, effectiveBatchRecords);
            batchRecords = effectiveBatchRecords;
        }

        // One reusable value list: contents are random but identical per
        // RPC, so the measured region is the RPC, not list assembly.
        var values = new ArrayList<byte[]>(batchRecords);
        for (int i = 0; i < batchRecords; i++) {
            var payload = new byte[payloadSize];
            ThreadLocalRandom.current().nextBytes(payload);
            values.add(payload);
        }

        try (var client = new BrokerClient(host, port, tls)) {
            var labels = TopicLabels.resolve(client, topic);

            long warm = 0;
            long warmupDeadline = System.nanoTime() + bounds.warmupNanos();
            while (System.nanoTime() < warmupDeadline) {
                sendOne(client, topic, partition, values, acksAll);
                warm += batchRecords;
            }

            var histogram = new Histogram(MAX_LATENCY_NANOS, 3);
            long sent = 0;
            long measureDeadline = bounds.durationBounded() ? warmupDeadline + bounds.durationNanos() : Long.MAX_VALUE;
            long budget = bounds.durationBounded() ? Long.MAX_VALUE : bounds.recordBudget();
            long start = System.nanoTime();
            while (sent < budget && System.nanoTime() < measureDeadline) {
                long t0 = System.nanoTime();
                sendOne(client, topic, partition, values, acksAll);
                histogram.recordValue(Math.min(System.nanoTime() - t0, MAX_LATENCY_NANOS));
                sent += batchRecords;
            }
            long elapsed = System.nanoTime() - start;

            PerfReport.row("produce-batch")
                    .latencyKind("per_rpc")
                    .acks(acksAll ? "all" : "1")
                    .partitions(labels.partitions())
                    .replicationFactor(labels.replicationFactor())
                    .minInsyncReplicas(labels.minInsyncReplicas())
                    .payloadSize(payloadSize)
                    .batchSize((long) batchRecords)
                    .warmupRecords(warm)
                    .records(sent)
                    .bytes(sent * payloadSize)
                    .elapsedNanos(elapsed)
                    .histogram(histogram)
                    .emit(csv);
        }
    }

    private static void sendOne(
            BrokerClient client, String topic, int partition, List<byte[]> values, boolean acksAll) {
        if (acksAll) client.produceBatchAcksAll(topic, partition, values);
        else client.produceBatch(topic, partition, values);
    }

    /** Largest record count whose encoded batch fits maxBatchBytes (floor 1). */
    private static int capToEncodedSize(int batchRecords, int payloadSize, int maxBatchBytes) {
        var probe = new byte[payloadSize];
        int n = batchRecords;
        while (n > 1) {
            var records = new ArrayList<jbroker.storage.Record>(n);
            for (int i = 0; i < n; i++) {
                records.add(new jbroker.storage.Record(i, 0L, null, probe));
            }
            long encoded = jbroker.storage.RecordBatch.estimatedSize(records);
            if (encoded <= maxBatchBytes) break;
            n = Math.max(1, (int) ((long) n * maxBatchBytes / encoded));
        }
        return n;
    }
}
