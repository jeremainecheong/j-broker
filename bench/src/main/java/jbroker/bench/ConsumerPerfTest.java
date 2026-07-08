package jbroker.bench;

import java.nio.file.Path;
import jbroker.broker.client.BrokerClient;
import org.HdrHistogram.Histogram;

/**
 * Fetch-path benchmark.
 *
 * <pre>
 *   j-broker-bench consumer --broker HOST:PORT --topic T --partition N
 *                           --records N [--max-bytes BYTES] [--csv FILE]
 * </pre>
 *
 * <p>Polls {@link BrokerClient#fetch} in a tight loop until at least
 * {@code --records} records have arrived (or the loop reaches
 * {@code records * 2} polls with no progress, at which point it gives up
 * and reports what it saw). Latency is per-fetch-call, not per-record —
 * so the histogram reflects how long each RPC round-trip takes, which is
 * the metric that matters for consumer-lag SLOs.
 */
public final class ConsumerPerfTest {

    private static final long ONE_MINUTE_NANOS = 60L * 1_000_000_000L;

    private ConsumerPerfTest() {}

    static void run(String[] args) throws Exception {
        String broker = BenchArgs.get(args, "--broker", "127.0.0.1:9092");
        String topic = BenchArgs.get(args, "--topic", null);
        int partition = BenchArgs.getInt(args, "--partition", 0);
        int records = BenchArgs.getInt(args, "--records", 10_000);
        int maxBytes = BenchArgs.getInt(args, "--max-bytes", 1024 * 1024);
        String csvStr = BenchArgs.get(args, "--csv", null);
        Path csv = csvStr == null ? null : Path.of(csvStr);

        if (topic == null) {
            System.err.println("--topic required");
            System.exit(2);
            return;
        }

        var histogram = new Histogram(ONE_MINUTE_NANOS, 3);
        int colon = broker.indexOf(':');
        var tls = BenchArgs.tlsFromArgs(args);
        try (var client =
                new BrokerClient(broker.substring(0, colon), Integer.parseInt(broker.substring(colon + 1)), tls)) {
            long offset = 0;
            long received = 0;
            long totalBytes = 0;
            int stalePolls = 0;
            int stallBudget = Math.max(100, records * 2);

            long start = System.nanoTime();
            while (received < records && stalePolls < stallBudget) {
                long t0 = System.nanoTime();
                var batch = client.fetch(topic, partition, offset, maxBytes);
                histogram.recordValue(System.nanoTime() - t0);
                if (batch.isEmpty()) {
                    stalePolls++;
                    Thread.onSpinWait();
                    continue;
                }
                stalePolls = 0;
                for (var rec : batch) totalBytes += rec.length;
                received += batch.size();
                offset += batch.size();
            }
            long elapsed = System.nanoTime() - start;

            PerfReport.emit("consumer", histogram, received, totalBytes, elapsed, csv);
            if (received < records) {
                System.err.printf(
                        "warning: only received %d/%d records before stall (check topic has data)%n",
                        received, records);
            }
        }
    }
}
