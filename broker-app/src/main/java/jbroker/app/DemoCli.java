package jbroker.app;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import jbroker.broker.client.BatchingProducer;
import jbroker.broker.client.ClusterClient;
import jbroker.broker.client.TransactionalProducer;
import jbroker.broker.client.consumer.Consumer;
import jbroker.broker.client.consumer.ConsumerConfig;
import jbroker.broker.client.consumer.StringDeserializer;
import jbroker.proto.broker.FetchOffsetsRequest;
import jbroker.proto.common.ErrorCode;
import jbroker.proto.common.TopicPartition;

/**
 * Workload drivers behind {@code scripts/demo/full-demo.sh}. Four modes, all
 * built on {@link ClusterClient} (multi-endpoint bootstrap, leader routing,
 * bounded retries), so each keeps working while any single broker is down:
 *
 * <pre>
 *   j-broker demo feed     --bootstrap H:P,... --topic T --count N [--rate R] [--partitions K] [--prefix S]
 *   j-broker demo drain    --bootstrap H:P,... --group G --topic T [--topic T2 ...]
 *   j-broker demo pipeline --bootstrap H:P,... --source S --sink D --group G --txn-id ID --expected N
 *   j-broker demo verify   --bootstrap H:P,... --source S --sink D --group G --expected N
 * </pre>
 *
 * <p>{@code feed} produces {@code PREFIX-0 .. PREFIX-(N-1)} round-robin
 * across partitions with the idempotent acks=all batching producer, paced at
 * {@code --rate} records/second. {@code drain} runs a consumer group and
 * prints a running total about once a second. {@code pipeline} is the
 * transactional consume-transform-produce loop: each polled batch is
 * rewritten to the sink and the source offsets are committed in the same
 * transaction, with {@link TransactionalProducer#transact} supplying the
 * abort-and-retry behaviour — the loop body itself contains no error
 * handling. {@code verify} re-reads both topics and checks the exactly-once
 * contract: the sink's read_committed records equal the transformed source
 * exactly once, in order, and the pipeline group's committed offset equals
 * the source record count. Exit code 0 means the audit passed.
 */
final class DemoCli {

    /** Transform applied by the pipeline; verify recomputes it on the source side. */
    private static String transform(String value) {
        return "processed:" + value;
    }

    static void run(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            System.exit(2);
        }
        var rest = Arrays.copyOfRange(args, 1, args.length);
        switch (args[0]) {
            case "feed" -> feed(rest);
            case "drain" -> drain(rest);
            case "pipeline" -> pipeline(rest);
            case "verify" -> verify(rest);
            default -> {
                usage();
                System.exit(2);
            }
        }
    }

    private static void usage() {
        System.err.println(
                """
                Usage: j-broker demo feed     --bootstrap H:P,... --topic T --count N [--rate R] [--partitions K] [--prefix S]
                       j-broker demo drain    --bootstrap H:P,... --group G --topic T [--topic T2 ...]
                       j-broker demo pipeline --bootstrap H:P,... --source S --sink D --group G --txn-id ID --expected N
                       j-broker demo verify   --bootstrap H:P,... --source S --sink D --group G --expected N""");
    }

    // ---- feed ----

    private static void feed(String[] args) throws Exception {
        var endpoints = bootstrap(args);
        String topic = required(args, "--topic");
        int count = Integer.parseInt(required(args, "--count"));
        int rate = Integer.parseInt(flag(args, "--rate", "0"));
        int partitions = Integer.parseInt(flag(args, "--partitions", "1"));
        String prefix = flag(args, "--prefix", "record");

        var cluster = new ClusterClient(endpoints);
        try (cluster;
                var producer = BatchingProducer.create(cluster)) {
            var futures = new ArrayList<CompletableFuture<Long>>(count);
            var acked = new AtomicLong();
            long start = System.nanoTime();
            long lastReport = start;
            for (int i = 0; i < count; i++) {
                var f = producer.send(topic, i % partitions, (prefix + "-" + i).getBytes(StandardCharsets.UTF_8));
                f.thenRun(acked::incrementAndGet);
                futures.add(f);
                if (rate > 0) {
                    long due = start + (long) ((i + 1) * 1_000_000_000.0 / rate);
                    long aheadMs = (due - System.nanoTime()) / 1_000_000;
                    if (aheadMs > 0) Thread.sleep(aheadMs);
                }
                if (System.nanoTime() - lastReport >= 1_000_000_000L) {
                    System.out.printf("feed %s: sent %d/%d, acked %d%n", topic, i + 1, count, acked.get());
                    lastReport = System.nanoTime();
                }
            }
            producer.flush();
            long failed = futures.stream()
                    .filter(CompletableFuture::isCompletedExceptionally)
                    .count();
            if (failed > 0) {
                System.err.printf("feed %s: %d of %d records failed delivery%n", topic, failed, count);
                System.exit(1);
            }
            System.out.printf("feed %s: complete, %d records acked (acks=all, idempotent)%n", topic, count);
        }
    }

    // ---- drain ----

    private static void drain(String[] args) throws Exception {
        var endpoints = bootstrap(args);
        String group = required(args, "--group");
        var topics = repeated(args, "--topic");
        if (topics.isEmpty()) {
            System.err.println("at least one --topic is required");
            System.exit(2);
        }

        // On SIGTERM the hook flips the flag, then waits for the main loop
        // to break and close the consumer — a clean group leave instead of a
        // phantom member lingering until the session timeout.
        var running = new AtomicBoolean(true);
        var mainThread = Thread.currentThread();
        Runtime.getRuntime()
                .addShutdownHook(new Thread(
                        () -> {
                            running.set(false);
                            try {
                                mainThread.join(10_000);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        },
                        "demo-drain-shutdown"));

        var cluster = new ClusterClient(endpoints);
        try (cluster;
                var consumer = new Consumer<>(
                        ConsumerConfig.builder(group).build(),
                        new StringDeserializer(),
                        new StringDeserializer(),
                        cluster)) {
            consumer.subscribe(topics, null);
            System.out.printf("drain %s: consuming %s%n", group, topics);
            long total = 0;
            long lastTotal = 0;
            long lastReport = System.nanoTime();
            while (running.get()) {
                var records = consumer.poll(Duration.ofMillis(500));
                total += records.count();
                if (!records.isEmpty()) {
                    try {
                        consumer.commitSync();
                    } catch (RuntimeException e) {
                        System.err.println("drain " + group + ": commit failed, retrying next tick: " + e.getMessage());
                    }
                }
                if (System.nanoTime() - lastReport >= 1_000_000_000L) {
                    System.out.printf("drain %s: total %d records (+%d)%n", group, total, total - lastTotal);
                    lastTotal = total;
                    lastReport = System.nanoTime();
                }
            }
        }
    }

    // ---- pipeline ----

    private static void pipeline(String[] args) throws Exception {
        var endpoints = bootstrap(args);
        String source = required(args, "--source");
        String sink = required(args, "--sink");
        String group = required(args, "--group");
        String txnId = required(args, "--txn-id");
        int expected = Integer.parseInt(required(args, "--expected"));

        var cluster = new ClusterClient(endpoints);
        try (cluster;
                var consumer = new Consumer<>(
                        ConsumerConfig.builder(group)
                                .isolationLevel(ConsumerConfig.IsolationLevel.READ_COMMITTED)
                                .maxPollRecords(50)
                                .build(),
                        new StringDeserializer(),
                        new StringDeserializer(),
                        cluster);
                var producer = new TransactionalProducer(
                        cluster,
                        txnId,
                        // A transaction stalled on a leader failover must not be
                        // timeout-aborted by the coordinator sweep, so the
                        // transaction timeout is far above any failover window.
                        new TransactionalProducer.Config(
                                /*transactionTimeoutMs*/ 1_800_000,
                                /*retryBackoffMs*/ 50,
                                /*opDeadlineMs*/ 120_000,
                                /*transactDeadlineMs*/ 600_000))) {
            consumer.subscribe(List.of(source), null);
            producer.initTransactions();
            System.out.printf(
                    "pipeline: %s -> %s, group=%s, txn-id=%s, expecting %d records%n",
                    source, sink, group, txnId, expected);
            long processed = 0;
            long txns = 0;
            while (processed < expected) {
                var records = consumer.poll(Duration.ofMillis(250));
                if (records.isEmpty()) continue;
                var batch = new ArrayList<>(records.all());
                long nextOffset = batch.get(batch.size() - 1).offset() + 1;
                producer.transact(() -> {
                    for (var record : batch) {
                        producer.send(sink, 0, transform(record.value()).getBytes(StandardCharsets.UTF_8));
                    }
                    producer.sendOffsetsToTransaction(group, Map.of(tp(source, 0), nextOffset));
                });
                processed += batch.size();
                txns++;
                System.out.printf(
                        "pipeline: txn %d committed, %d records (total %d/%d)%n",
                        txns, batch.size(), processed, expected);
            }
            System.out.printf("pipeline: complete, %d records moved in %d transactions%n", processed, txns);
        }
    }

    // ---- verify ----

    private static void verify(String[] args) throws Exception {
        var endpoints = bootstrap(args);
        String source = required(args, "--source");
        String sink = required(args, "--sink");
        String group = required(args, "--group");
        int expected = Integer.parseInt(required(args, "--expected"));

        var cluster = new ClusterClient(endpoints);
        try (cluster) {
            var sourceValues = drainOnce(
                    cluster, source, "demo-audit-src", ConsumerConfig.IsolationLevel.READ_UNCOMMITTED, expected);
            System.out.printf("verify: source %s-0 records: %d%n", source, sourceValues.size());

            var sinkValues =
                    drainOnce(cluster, sink, "demo-audit-sink", ConsumerConfig.IsolationLevel.READ_COMMITTED, expected);
            System.out.printf("verify: sink %s-0 records under read_committed: %d%n", sink, sinkValues.size());

            var expectedSink = new ArrayList<String>(sourceValues.size());
            for (var v : sourceValues) expectedSink.add(transform(v));

            boolean pass = true;
            if (sourceValues.size() != expected) {
                System.out.printf(
                        "verify: FAIL, source holds %d records, expected %d%n", sourceValues.size(), expected);
                pass = false;
            }
            if (sinkValues.equals(expectedSink)) {
                System.out.println("verify: sink equals transformed source exactly once, in order: OK");
            } else {
                pass = false;
                System.out.printf(
                        "verify: FAIL, sink does not match transformed source (source=%d, sink=%d)%n",
                        expectedSink.size(), sinkValues.size());
                int limit = Math.min(expectedSink.size(), sinkValues.size());
                for (int i = 0; i < limit; i++) {
                    if (!expectedSink.get(i).equals(sinkValues.get(i))) {
                        System.out.printf(
                                "verify: first divergence at sink offset %d: expected %s, saw %s%n",
                                i, expectedSink.get(i), sinkValues.get(i));
                        break;
                    }
                }
            }

            long committed = awaitCommittedOffset(cluster, group, source, sourceValues.size());
            if (committed == sourceValues.size()) {
                System.out.printf(
                        "verify: group %s committed offset on %s-0: %d, equals source count: OK%n",
                        group, source, committed);
            } else {
                pass = false;
                System.out.printf(
                        "verify: FAIL, group %s committed offset on %s-0 is %d, expected %d%n",
                        group, source, committed, sourceValues.size());
            }

            System.out.println(pass ? "verify: PASS, exactly-once contract holds" : "verify: FAIL");
            if (!pass) System.exit(1);
        }
    }

    /**
     * Read the whole single-partition topic through a throwaway consumer
     * group (nothing is committed): poll until {@code atLeast} records
     * arrived or 60s passed, then a few extra polls so duplicates or
     * aborted-data leakage would surface as extras.
     */
    private static List<String> drainOnce(
            ClusterClient cluster, String topic, String group, ConsumerConfig.IsolationLevel isolation, int atLeast)
            throws Exception {
        try (var consumer = new Consumer<>(
                ConsumerConfig.builder(group).isolationLevel(isolation).build(),
                new StringDeserializer(),
                new StringDeserializer(),
                cluster)) {
            consumer.subscribe(List.of(topic), null);
            var out = new ArrayList<String>();
            long deadline = System.currentTimeMillis() + 60_000;
            while (out.size() < atLeast && System.currentTimeMillis() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    out.add(record.value());
                }
            }
            for (int i = 0; i < 3; i++) {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    out.add(record.value());
                }
            }
            return out;
        }
    }

    /**
     * The last transaction's offset commit becomes visible when its marker
     * folds into the group coordinator, which can lag the pipeline's exit by
     * a beat — poll up to 30s for the expected value, then report whatever
     * the final answer was.
     */
    private static long awaitCommittedOffset(ClusterClient cluster, String group, String topic, long expected)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        long last = -1;
        while (System.currentTimeMillis() < deadline) {
            var resp = cluster.fetchOffsets(
                    FetchOffsetsRequest.newBuilder()
                            .setGroupId(group)
                            .addTps(tp(topic, 0))
                            .build(),
                    10_000);
            var result = resp.getResults(0);
            last = result.getError() == ErrorCode.OK ? result.getOffset() : -1;
            if (last == expected) return last;
            Thread.sleep(200);
        }
        return last;
    }

    // ---- shared plumbing ----

    private static TopicPartition tp(String topic, int partition) {
        return TopicPartition.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .build();
    }

    private static List<String> bootstrap(String[] args) {
        var raw = required(args, "--bootstrap");
        var endpoints = new ArrayList<String>();
        for (var part : raw.split(",")) {
            if (!part.isBlank()) endpoints.add(part.trim());
        }
        if (endpoints.isEmpty()) {
            System.err.println("--bootstrap must list at least one HOST:PORT");
            System.exit(2);
        }
        return endpoints;
    }

    private static String required(String[] args, String name) {
        var value = flag(args, name, null);
        if (value == null) {
            System.err.println(name + " is required");
            usage();
            System.exit(2);
        }
        return value;
    }

    private static String flag(String[] args, String name, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) return args[i + 1];
        }
        return defaultValue;
    }

    private static List<String> repeated(String[] args, String name) {
        var out = new ArrayList<String>();
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) out.add(args[i + 1]);
        }
        return out;
    }

    private DemoCli() {}
}
