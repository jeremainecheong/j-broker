package jbroker.bench;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import jbroker.broker.client.BrokerClient;
import jbroker.storage.Record;

/**
 * compaction pause-duration bench. The compaction
 * implementation is stop-the-world (Log-level ReentrantLock held for the
 * full rewrite); the v1.4 bench suite only measured produce/consume
 * latency under steady state and never surfaced how long a compaction
 * blocks the hot path. This harness:
 *
 * <ol>
 *   <li>Spins up a single broker.</li>
 *   <li>Creates a compact-policy topic.</li>
 *   <li>Appends N records across K keys directly at the Log layer.</li>
 *   <li>Calls {@code forceCompactPartition} and times the RPC.</li>
 * </ol>
 *
 * <p>Because the call blocks until compaction finishes, the RPC
 * wall-clock is a reasonable upper bound on the pause a concurrent
 * writer would observe. The bench surfaces records/keys/resulting-
 * survivors along with the pause so compaction tuning has a baseline.
 *
 * <pre>
 *   j-broker-bench compaction [--records N] [--keys K] [--csv FILE]
 * </pre>
 */
public final class CompactionPausePerfTest {

    private CompactionPausePerfTest() {}

    static void run(String[] args) throws Exception {
        int records = BenchArgs.getInt(args, "--records", 100_000);
        int keys = BenchArgs.getInt(args, "--keys", 1_000);
        String csvStr = BenchArgs.get(args, "--csv", null);
        Path csv = csvStr == null ? null : Path.of(csvStr);

        try (var cluster = BenchCluster.start(1)) {
            try (var client = new BrokerClient("127.0.0.1", cluster.leaderPort)) {
                client.createTopicWithConfig("bench-compaction", 1, 1, Map.of("cleanup.policy", "compact"));

                // Append records directly on the leader log so the timing
                // isolates compaction, not produce.
                var leaderLog = cluster.brokers.get(0).logManager().logFor("bench-compaction", 0);
                long seedStart = System.nanoTime();
                for (int i = 0; i < records; i++) {
                    String key = "k" + (i % keys);
                    String value = "v" + i;
                    leaderLog.append(
                            List.of(new Record(
                                    0,
                                    0L,
                                    key.getBytes(StandardCharsets.UTF_8),
                                    value.getBytes(StandardCharsets.UTF_8))),
                            System.currentTimeMillis());
                }
                long seedElapsed = System.nanoTime() - seedStart;

                // Force compaction and time the pause.
                long compactStart = System.nanoTime();
                int kept = client.forceCompactPartition("bench-compaction", 0);
                long compactElapsed = System.nanoTime() - compactStart;

                double compactSec = compactElapsed / 1e9;
                double seedSec = seedElapsed / 1e9;
                System.out.printf(
                        Locale.ROOT,
                        "=== compaction perf summary ===%n"
                                + "records      : %,d%n"
                                + "distinct keys: %,d%n"
                                + "survivors    : %,d%n"
                                + "seed elapsed : %.3fs (leader-local log append)%n"
                                + "compact pause: %.3fs  (records/pause = %,.0f rec/s)%n",
                        records,
                        keys,
                        kept,
                        seedSec,
                        compactSec,
                        records / compactSec);
                if (csv != null) {
                    appendCsv(csv, records, keys, kept, compactSec);
                }
            }
        }
    }

    private static void appendCsv(Path csv, int records, int keys, int kept, double compactSec) throws IOException {
        Files.createDirectories(csv.toAbsolutePath().getParent());
        boolean fresh = !Files.exists(csv);
        try (var w = Files.newBufferedWriter(csv, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            if (fresh) {
                w.write("mode,records,keys,survivors,compact_s\n");
            }
            w.write(String.format(Locale.ROOT, "compaction,%d,%d,%d,%.6f%n", records, keys, kept, compactSec));
        }
    }
}
