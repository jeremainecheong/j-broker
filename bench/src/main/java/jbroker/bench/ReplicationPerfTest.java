package jbroker.bench;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;
import jbroker.broker.client.BrokerClient;
import jbroker.storage.Record;

/**
 * Follower replication throughput bench. Spins up an
 * in-process 3-broker RF=3 cluster, bulk-appends to the leader's log
 * directly (so the measurement isolates the replica-fetcher path rather
 * than the client→leader path), then waits for every follower's LEO to
 * catch up. Reports:
 *
 * <ul>
 *   <li>catch-up duration (wall clock from "seeding done" → "all
 *       followers at LEO=N")</li>
 *   <li>records/s and MiB/s the follower fetchers sustained</li>
 * </ul>
 *
 * <p>Flags: {@code --records N --payload-size B [--csv FILE]}. Defaults
 * produce ~25 MiB so the run finishes in a few seconds on a laptop but
 * still crosses the ReplicaFetcher's fetch-batching threshold.
 */
public final class ReplicationPerfTest {

    private ReplicationPerfTest() {}

    static void run(String[] args) throws Exception {
        int records = BenchArgs.getInt(args, "--records", 50_000);
        int payloadSize = BenchArgs.getInt(args, "--payload-size", 512);
        String csvStr = BenchArgs.get(args, "--csv", null);
        Path csv = csvStr == null ? null : Path.of(csvStr);

        byte[] payload = new byte[payloadSize];
        ThreadLocalRandom.current().nextBytes(payload);

        try (var cluster = BenchCluster.start(3)) {
            try (var client = new BrokerClient("127.0.0.1", cluster.leaderPort)) {
                client.createTopic("bench-replication", 1, 3);
            }
            // Resolve partition leader broker id by polling any broker.
            int leaderId = -1;
            long pollDeadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < pollDeadline && leaderId < 0) {
                for (int i = 0; i < cluster.brokers.size(); i++) {
                    var state = cluster.brokers
                            .get(i)
                            .topics()
                            .partitionState("bench-replication", 0)
                            .orElse(null);
                    if (state != null && state.leader() > 0) {
                        leaderId = state.leader();
                        break;
                    }
                }
                if (leaderId < 0) Thread.sleep(50);
            }
            if (leaderId < 0) throw new IllegalStateException("no partition leader within 5s");
            var leaderBroker = cluster.brokers.get(leaderId - 1);
            var leaderLog = leaderBroker.logManager().logFor("bench-replication", 0);

            // Seed: append N records to the leader's log directly. Use
            // appendRaw via Log.append(records, now) so ReplicaFetchers
            // copy the raw batches byte-for-byte.
            long seedStart = System.nanoTime();
            for (int i = 0; i < records; i++) {
                leaderLog.append(List.of(new Record(0, 0L, null, payload)), System.currentTimeMillis());
            }
            long seedElapsed = System.nanoTime() - seedStart;

            // Catch-up: wait for every follower's LEO to hit `records`.
            long targetLeo = leaderLog.nextOffset();
            long catchupStart = System.nanoTime();
            long catchupDeadline = System.currentTimeMillis() + 120_000;
            while (System.currentTimeMillis() < catchupDeadline) {
                boolean allCaughtUp = true;
                for (int i = 0; i < cluster.brokers.size(); i++) {
                    if ((i + 1) == leaderId) continue;
                    long leo = cluster.brokers
                            .get(i)
                            .logManager()
                            .logFor("bench-replication", 0)
                            .nextOffset();
                    if (leo < targetLeo) {
                        allCaughtUp = false;
                        break;
                    }
                }
                if (allCaughtUp) break;
                Thread.sleep(10);
            }
            long catchupElapsed = System.nanoTime() - catchupStart;

            double catchupSec = catchupElapsed / 1e9;
            double seedSec = seedElapsed / 1e9;
            long bytes = (long) records * payloadSize;
            double rps = records / catchupSec;
            double mibPerSec = bytes / catchupSec / (1024.0 * 1024.0);

            System.out.printf(
                    Locale.ROOT,
                    "=== replication perf summary ===%n"
                            + "records       : %,d%n"
                            + "bytes         : %,d%n"
                            + "seed elapsed  : %.3fs (leader log append only)%n"
                            + "catch-up      : %.3fs (last-produce → all-followers@LEO)%n"
                            + "replication   : %,.0f records/s  (%.2f MiB/s)%n",
                    records,
                    bytes,
                    seedSec,
                    catchupSec,
                    rps,
                    mibPerSec);
            if (csv != null) {
                appendCsv(csv, records, bytes, catchupSec, rps, mibPerSec);
            }
        }
    }

    private static void appendCsv(Path csv, int records, long bytes, double catchupSec, double rps, double mibPerSec)
            throws IOException {
        Files.createDirectories(csv.toAbsolutePath().getParent());
        boolean fresh = !Files.exists(csv);
        try (var w = Files.newBufferedWriter(csv, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            if (fresh) {
                w.write("mode,records,bytes,catchup_s,records_per_s,mib_per_s\n");
            }
            w.write(String.format(
                    Locale.ROOT, "replication,%d,%d,%.6f,%.2f,%.2f%n", records, bytes, catchupSec, rps, mibPerSec));
        }
    }
}
