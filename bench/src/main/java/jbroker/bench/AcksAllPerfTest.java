package jbroker.bench;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import jbroker.broker.client.BrokerClient;
import jbroker.broker.client.ClusterClient;
import jbroker.proto.broker.ProduceRequest;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;
import org.HdrHistogram.Histogram;

/**
 * Replicated-path producer bench (verb {@code acks-all}, CSV mode
 * {@code cluster-producer}). Times single-record produce RPCs against an
 * RF=3 topic, routed to each partition leader by {@link ClusterClient};
 * with {@code --acks all} every sample includes the leader's
 * wait-for-HWM loop against a real replication pipeline.
 *
 * <pre>
 *   j-broker-bench acks-all [--acks all|1] [--partitions N] [--rf N]
 *                           [--min-insync N] [--payload-size B]
 *                           [--duration-s S | --records N] [--warmup-s S]
 *                           [--bootstrap H:P,H:P,...] [--csv FILE]
 * </pre>
 *
 * <p>Topology: an in-process loopback 3-broker cluster by default; when
 * {@code --bootstrap} (or env {@code BENCH_BOOTSTRAP}) lists external
 * endpoints, the bench runs against those instead and refuses to start
 * unless at least {@code --rf} of them are reachable — a silently
 * under-replicated "acks=all" number would be a lie.
 */
public final class AcksAllPerfTest {

    private static final long MAX_LATENCY_NANOS = 60L * 1_000_000_000L;

    private AcksAllPerfTest() {}

    static void run(String[] args) throws Exception {
        int payloadSize = BenchArgs.getInt(args, "--payload-size", 512);
        int partitions = Math.max(1, BenchArgs.getInt(args, "--partitions", 1));
        int rf = Math.max(1, BenchArgs.getInt(args, "--rf", 3));
        int minInsync = BenchArgs.getInt(args, "--min-insync", -1);
        boolean acksAll = !"1".equals(BenchArgs.get(args, "--acks", "all"));
        var bounds = RunBounds.parse(args);
        String csvStr = BenchArgs.get(args, "--csv", null);
        Path csv = csvStr == null ? null : Path.of(csvStr);
        String bootstrap = BenchArgs.get(args, "--bootstrap", System.getenv("BENCH_BOOTSTRAP"));

        byte[] payload = new byte[payloadSize];
        ThreadLocalRandom.current().nextBytes(payload);
        // Unique per run: external clusters keep topics across invocations.
        String topic = "bench-cluster-" + System.currentTimeMillis();

        BenchCluster cluster = null;
        List<String> endpoints;
        try {
            if (bootstrap != null && !bootstrap.isBlank()) {
                endpoints = java.util.Arrays.stream(bootstrap.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .toList();
                requireReachable(endpoints, rf);
            } else {
                cluster = BenchCluster.start(3);
                endpoints = new ArrayList<>();
                for (int p : cluster.brokerPorts) endpoints.add("127.0.0.1:" + p);
            }
            runAgainst(endpoints, topic, partitions, rf, minInsync, acksAll, payload, bounds, csv);
        } finally {
            if (cluster != null) cluster.close();
        }
    }

    private static void requireReachable(List<String> endpoints, int rf) {
        int reachable = 0;
        for (var ep : endpoints) {
            try (var c = client(ep)) {
                c.describeCluster();
                reachable++;
            } catch (RuntimeException e) {
                System.err.println("broker unreachable: " + ep + " (" + e.getMessage() + ")");
            }
        }
        if (reachable < rf) {
            System.err.printf(
                    "%d of %d brokers reachable, replication factor %d requires %d%n",
                    reachable, endpoints.size(), rf, rf);
            System.exit(2);
        }
    }

    private static void runAgainst(
            List<String> endpoints,
            String topic,
            int partitions,
            int rf,
            int minInsync,
            boolean acksAll,
            byte[] payload,
            RunBounds bounds,
            Path csv)
            throws Exception {
        createTopic(endpoints, topic, partitions, rf, minInsync);

        // Pre-encode one single-record batch and one request per partition
        // so the measured region is the RPC round trip, not client-side
        // encoding.
        var records = List.of(new Record(0, 0L, null, payload));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        long now = System.currentTimeMillis();
        RecordBatch.encode(buf, 0L, 0, now, now, -1L, (short) -1, -1, records);
        buf.flip();
        var batchBytes = ByteString.copyFrom(buf);
        var requests = new ProduceRequest[partitions];
        for (int p = 0; p < partitions; p++) {
            requests[p] = ProduceRequest.newBuilder()
                    .setTopic(topic)
                    .setPartition(p)
                    .setBatch(batchBytes)
                    .setProducerId(-1L)
                    .setBaseSequence(-1)
                    .setAcks(acksAll ? -1 : 1)
                    .build();
        }

        try (var client = new ClusterClient(endpoints)) {
            long warm = 0;
            long warmupDeadline = System.nanoTime() + bounds.warmupNanos();
            while (System.nanoTime() < warmupDeadline) {
                client.produce(requests[(int) (warm % partitions)], 10_000);
                warm++;
            }

            var histogram = new Histogram(MAX_LATENCY_NANOS, 3);
            long sent = 0;
            long measureDeadline = bounds.durationBounded() ? warmupDeadline + bounds.durationNanos() : Long.MAX_VALUE;
            long budget = bounds.durationBounded() ? Long.MAX_VALUE : bounds.recordBudget();
            long start = System.nanoTime();
            while (sent < budget && System.nanoTime() < measureDeadline) {
                var req = requests[(int) (sent % partitions)];
                long t0 = System.nanoTime();
                client.produce(req, 10_000);
                histogram.recordValue(Math.min(System.nanoTime() - t0, MAX_LATENCY_NANOS));
                sent++;
            }
            long elapsed = System.nanoTime() - start;

            PerfReport.row("cluster-producer")
                    .latencyKind("per_rpc")
                    .acks(acksAll ? "all" : "1")
                    .partitions(partitions)
                    .replicationFactor(rf)
                    .minInsyncReplicas(minInsync > 0 ? minInsync : null)
                    .payloadSize(payload.length)
                    .batchSize(1L)
                    .warmupRecords(warm)
                    .records(sent)
                    .bytes(sent * payload.length)
                    .elapsedNanos(elapsed)
                    .histogram(histogram)
                    .emit(csv);
        }
    }

    /**
     * Create the bench topic via whichever endpoint currently hosts the
     * controller — non-controllers reject with NOT_LEADER, so walk the
     * list until one accepts.
     */
    private static void createTopic(List<String> endpoints, String topic, int partitions, int rf, int minInsync) {
        var config = new LinkedHashMap<String, String>();
        if (minInsync > 0) config.put("min.insync.replicas", Integer.toString(minInsync));
        RuntimeException last = null;
        for (var ep : endpoints) {
            try (var c = client(ep)) {
                c.createTopicWithConfig(topic, partitions, rf, config);
                return;
            } catch (RuntimeException e) {
                last = e;
            }
        }
        throw new IllegalStateException("createTopic failed on every endpoint", last);
    }

    private static BrokerClient client(String endpoint) {
        int colon = endpoint.indexOf(':');
        return new BrokerClient(
                endpoint.substring(0, colon).trim(),
                Integer.parseInt(endpoint.substring(colon + 1).trim()));
    }
}
