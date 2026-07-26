package jbroker.bench;

import java.nio.file.Path;
import jbroker.broker.client.BrokerClient;
import org.HdrHistogram.Histogram;

/**
 * Fetch-path benchmark against an already-running broker. Emits ONE CSV
 * row: mode=consumer, latency_kind=per_fetch_rpc — the histogram times
 * fetch RPC round trips that returned records. Attributing a fetch's
 * latency to each record it carried would be meaningless, so there is no
 * per-record latency row; per-record throughput lives in the records /
 * elapsed_s columns of the same row.
 *
 * <pre>
 *   j-broker-bench consumer --broker HOST:PORT --topic T --partition N
 *                           [--duration-s S | --records N] [--warmup-s S]
 *                           [--max-bytes BYTES] [--csv FILE]
 * </pre>
 *
 * <p>Duration-bounded runs wrap back to offset 0 on reaching the log
 * tail, so a long run keeps fetching real data and accumulates enough
 * fetch samples for a valid p999 (the PerfReport gate suppresses the
 * percentile honestly when it doesn't). Records-bounded runs stop at
 * {@code --records} consumed or after a stall budget of empty polls.
 */
public final class ConsumerPerfTest {

    private static final long MAX_LATENCY_NANOS = 60L * 1_000_000_000L;

    private ConsumerPerfTest() {}

    static void run(String[] args) throws Exception {
        String broker = BenchArgs.get(args, "--broker", "127.0.0.1:9092");
        String topic = BenchArgs.get(args, "--topic", null);
        int partition = BenchArgs.getInt(args, "--partition", 0);
        int maxBytes = BenchArgs.getInt(args, "--max-bytes", 1024 * 1024);
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
        try (var client =
                new BrokerClient(broker.substring(0, colon), Integer.parseInt(broker.substring(colon + 1)), tls)) {
            var labels = TopicLabels.resolve(client, topic);

            long offset = 0;
            long warmRecords = 0;
            long warmupDeadline = System.nanoTime() + bounds.warmupNanos();
            while (System.nanoTime() < warmupDeadline) {
                var batch = client.fetch(topic, partition, offset, maxBytes);
                if (batch.isEmpty()) {
                    if (offset == 0) {
                        Thread.sleep(1);
                    } else {
                        offset = 0;
                    }
                    continue;
                }
                warmRecords += batch.size();
                offset += batch.size();
            }

            var histogram = new Histogram(MAX_LATENCY_NANOS, 3);
            long received = 0;
            long totalBytes = 0;
            long emptyFetches = 0;
            long measureDeadline = bounds.durationBounded() ? warmupDeadline + bounds.durationNanos() : Long.MAX_VALUE;
            long budget = bounds.durationBounded() ? Long.MAX_VALUE : bounds.recordBudget();
            int stalePolls = 0;
            int stallBudget = bounds.durationBounded() ? Integer.MAX_VALUE : (int) Math.max(100, budget * 2);

            long start = System.nanoTime();
            while (received < budget && stalePolls < stallBudget && System.nanoTime() < measureDeadline) {
                long t0 = System.nanoTime();
                var batch = client.fetch(topic, partition, offset, maxBytes);
                long dt = System.nanoTime() - t0;
                if (batch.isEmpty()) {
                    emptyFetches++;
                    if (bounds.durationBounded()) {
                        // Tail reached: wrap and keep the fetch path loaded.
                        if (offset == 0) Thread.sleep(1);
                        else offset = 0;
                    } else {
                        stalePolls++;
                        Thread.onSpinWait();
                    }
                    continue;
                }
                stalePolls = 0;
                histogram.recordValue(Math.min(dt, MAX_LATENCY_NANOS));
                for (var rec : batch) totalBytes += rec.length;
                received += batch.size();
                offset += batch.size();
            }
            long elapsed = System.nanoTime() - start;

            PerfReport.row("consumer")
                    .latencyKind("per_fetch_rpc")
                    .partitions(labels.partitions())
                    .replicationFactor(labels.replicationFactor())
                    .minInsyncReplicas(labels.minInsyncReplicas())
                    .warmupRecords(warmRecords)
                    .records(received)
                    .bytes(totalBytes)
                    .elapsedNanos(elapsed)
                    .histogram(histogram)
                    .emit(csv);

            if (emptyFetches > 0) {
                System.out.printf("empty fetches: %d (excluded from the latency histogram)%n", emptyFetches);
            }
            if (!bounds.durationBounded() && received < budget) {
                System.err.printf(
                        "warning: only received %d/%d records before stall (check topic has data)%n", received, budget);
            }
        }
    }
}
