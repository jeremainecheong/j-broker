package jbroker.bench;

import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;
import jbroker.broker.client.BrokerClient;
import jbroker.storage.Record;

/**
 * Follower replication throughput bench. Spins up an in-process 3-broker
 * RF=3 cluster, bulk-appends to the leader's log directly (so the
 * measurement isolates the replica-fetcher path rather than the
 * client→leader path), then waits for every follower's LEO to catch up.
 * The CSV row (mode=replication) carries catch-up throughput only — there
 * is no per-request latency histogram on this path, so the latency cells
 * stay empty and samples is 0.
 *
 * <pre>
 *   j-broker-bench replication [--records N | --duration-s S]
 *                              [--payload-size B] [--csv FILE]
 * </pre>
 *
 * <p>Records-bounded by default (50k) — seeding is a direct log append
 * with no backpressure, so duration-bounding the seed phase writes at
 * append speed and is only for deliberate large-volume runs. The warmup
 * is a smaller seed + full catch-up pass excluded from the measurement.
 */
public final class ReplicationPerfTest {

    private ReplicationPerfTest() {}

    static void run(String[] args) throws Exception {
        int payloadSize = BenchArgs.getInt(args, "--payload-size", 512);
        var bounds = RunBounds.parse(args, 30, 50_000);
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

            // Warmup pass: a smaller seed + full follower catch-up, excluded
            // from the measurement, so fetcher connections and first-fetch
            // setup costs never land in the measured window. 20% of the
            // measured records (floor 1000) when records-bounded; the
            // configured warmup window when duration-bounded.
            long warmRecords = 0;
            if (bounds.durationBounded()) {
                long warmDeadline = System.nanoTime() + bounds.warmupNanos();
                while (System.nanoTime() < warmDeadline) {
                    leaderLog.append(List.of(new Record(0, 0L, null, payload)), System.currentTimeMillis());
                    warmRecords++;
                }
            } else {
                long target = Math.max(1_000, bounds.recordBudget() / 5);
                for (long i = 0; i < target; i++) {
                    leaderLog.append(List.of(new Record(0, 0L, null, payload)), System.currentTimeMillis());
                }
                warmRecords = target;
            }
            awaitFollowers(cluster, leaderId, leaderLog.nextOffset(), 120_000);

            // Seed: append the measured records to the leader's log directly
            // so ReplicaFetchers copy the raw batches byte-for-byte.
            long records = 0;
            long seedStart = System.nanoTime();
            if (bounds.durationBounded()) {
                long seedDeadline = seedStart + bounds.durationNanos();
                while (System.nanoTime() < seedDeadline) {
                    leaderLog.append(List.of(new Record(0, 0L, null, payload)), System.currentTimeMillis());
                    records++;
                }
            } else {
                for (long i = 0; i < bounds.recordBudget(); i++) {
                    leaderLog.append(List.of(new Record(0, 0L, null, payload)), System.currentTimeMillis());
                }
                records = bounds.recordBudget();
            }
            long seedElapsed = System.nanoTime() - seedStart;

            // Catch-up: wall clock from "seeding done" to "all followers at LEO".
            long targetLeo = leaderLog.nextOffset();
            long catchupStart = System.nanoTime();
            awaitFollowers(cluster, leaderId, targetLeo, 120_000);
            long catchupElapsed = System.nanoTime() - catchupStart;

            System.out.printf(
                    Locale.ROOT,
                    "seed elapsed  : %.3fs (leader log append only)%n"
                            + "catch-up      : %.3fs (last-produce → all-followers@LEO)%n",
                    seedElapsed / 1e9,
                    catchupElapsed / 1e9);

            PerfReport.row("replication")
                    .partitions(1)
                    .replicationFactor(3)
                    .payloadSize(payloadSize)
                    .warmupRecords(warmRecords)
                    .records(records)
                    .bytes(records * payloadSize)
                    .elapsedNanos(catchupElapsed)
                    .emit(csv);
        }
    }

    private static void awaitFollowers(BenchCluster cluster, int leaderId, long targetLeo, long timeoutMs)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
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
            if (allCaughtUp) return;
            Thread.sleep(10);
        }
        throw new IllegalStateException("followers did not reach LEO " + targetLeo + " within " + timeoutMs + "ms");
    }
}
