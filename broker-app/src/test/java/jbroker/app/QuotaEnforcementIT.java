package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.broker.ErrorCodes;
import jbroker.broker.client.BrokerClient;
import jbroker.proto.broker.ConsumerGrpc;
import jbroker.proto.broker.FetchRequest;
import jbroker.proto.broker.FetchResponse;
import jbroker.proto.broker.ProduceRequest;
import jbroker.proto.broker.ProduceResponse;
import jbroker.proto.broker.ProducerGrpc;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end byte-rate quota enforcement through a live broker:
 *
 * <ul>
 *   <li>A hot producer against a small produce quota is refused with
 *       retriable {@code QUOTA_VIOLATED} carrying a back-off hint, and the
 *       same produce succeeds after honoring the hint.</li>
 *   <li>A fetch quota throttles a client consumer while follower
 *       replication — which rides the separate ReplicaFetch RPC — stays
 *       healthy: log tails converge and the ISR remains full.</li>
 *   <li>A default-config broker (no quota settings) behaves exactly as
 *       before: bursts far beyond any quota-sized budget never see a
 *       {@code QUOTA_VIOLATED}.</li>
 * </ul>
 */
class QuotaEnforcementIT {

    // CI runners fsync + schedule 2–3× slower; scale deadlines so the gate
    // validates "it happens", not laptop-specific timing.
    private static final int CI_MULT =
            "1".equals(System.getenv("JBROKER_CI")) || "true".equalsIgnoreCase(System.getenv("CI")) ? 4 : 1;

    private static final String TOPIC = "quota-e2e";
    private static final Pattern RETRY_HINT = Pattern.compile("retry in (\\d+)ms");

    /**
     * Produce quota vs batch size leaves a wide margin against slow
     * runners: a 6 KiB batch against an 8 KiB/s bucket means back-to-back
     * produces are denied unless the loop somehow slows below one send
     * per 500ms — two orders of magnitude above a loopback RPC.
     */
    private static final long PRODUCE_QUOTA_BYTES_PER_SEC = 8 * 1024;

    private static final int PRODUCE_PAYLOAD_BYTES = 6 * 1024;

    /** Same margin on fetch: 16 KiB reads against a 32 KiB/s bucket. */
    private static final long FETCH_QUOTA_BYTES_PER_SEC = 32 * 1024;

    private static final int FETCH_MAX_BYTES = 16 * 1024;

    @Test
    void hotProducerDeniedWithHintThenSucceedsAfterBackoff(@TempDir Path dir) throws Exception {
        try (var cluster = TestBrokerCluster.start(
                1, 2, (i, voters, ports) -> new Broker.Config(new NodeId(1), dir, ports[0][0], ports[0][1], voters)
                        .withQuotaBytesPerSec(PRODUCE_QUOTA_BYTES_PER_SEC, 0))) {
            awaitSingleRaftLeader(cluster.brokers());
            try (var admin = new BrokerClient("127.0.0.1", cluster.brokerPort(0))) {
                admin.createTopic(TOPIC, 1, 1);
            }
            awaitPartitionLeaderIsrSize(cluster, 1, 15_000L * CI_MULT);

            try (var producer = new RawProducer(cluster.brokerPort(0))) {
                byte[] payload = new byte[PRODUCE_PAYLOAD_BYTES];

                // Hot loop: unpaced sends must run into the quota. The
                // bucket admits at most one 6 KiB batch per 8 KiB budget,
                // so the second-or-so back-to-back send is refused.
                ProduceResponse denied = null;
                long deadline = System.currentTimeMillis() + 20_000L * CI_MULT;
                while (System.currentTimeMillis() < deadline) {
                    var resp = producer.produce(TOPIC, payload);
                    if (resp.hasError()) {
                        denied = resp;
                        break;
                    }
                }
                assertThat(denied)
                        .as("an unpaced producer runs into the produce quota")
                        .isNotNull();
                assertThat(denied.getError().getCode()).isEqualTo(ErrorCodes.QUOTA_VIOLATED);
                assertThat(denied.getError().getMessage())
                        .contains("produce quota exceeded")
                        .contains(PRODUCE_QUOTA_BYTES_PER_SEC + " B/s");
                long hintMillis = parseRetryHint(denied.getError().getMessage());
                assertThat(hintMillis).isPositive();

                // Backing off per the hint refills enough budget for the
                // same batch. Loop for robustness on noisy runners, but
                // the first post-hint retry is the expected success.
                boolean acked = false;
                long retryDeadline = System.currentTimeMillis() + 20_000L * CI_MULT;
                long sleepMillis = hintMillis;
                while (System.currentTimeMillis() < retryDeadline) {
                    Thread.sleep(sleepMillis + 50L);
                    var retry = producer.produce(TOPIC, payload);
                    if (!retry.hasError()) {
                        acked = true;
                        break;
                    }
                    assertThat(retry.getError().getCode()).isEqualTo(ErrorCodes.QUOTA_VIOLATED);
                    sleepMillis = parseRetryHint(retry.getError().getMessage());
                }
                assertThat(acked)
                        .as("the denied producer succeeds after honoring the back-off hint")
                        .isTrue();
            }
        }
    }

    @Test
    void fetchQuotaThrottlesClientWhileReplicationStaysHealthy(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        try (var cluster = TestBrokerCluster.start(3, 2, (i, voters, ports) -> new Broker.Config(
                        new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters)
                .withQuotaBytesPerSec(0, FETCH_QUOTA_BYTES_PER_SEC))) {
            awaitSingleRaftLeader(cluster.brokers());
            awaitRegistryConvergence(cluster.brokers());
            try (var admin = new BrokerClient(
                    "127.0.0.1", raftLeaderOf(cluster.brokers()).brokerPort())) {
                admin.createTopic(TOPIC, 1, 3);
            }
            int leaderId = awaitPartitionLeaderIsrSize(cluster, 3, 20_000L * CI_MULT);
            int leaderPort = cluster.brokerPort(leaderId - 1);

            // Seed well past the fetch bucket. Produce quota is disabled,
            // so this is full speed, and acks=all forces replication.
            var batch = new ArrayList<byte[]>();
            for (int i = 0; i < 10; i++) batch.add(new byte[4 * 1024]);
            try (var client = new BrokerClient("127.0.0.1", leaderPort)) {
                for (int i = 0; i < 3; i++) client.produceBatchAcksAll(TOPIC, 0, batch);
            }

            // Replication is exempt from the fetch quota: 120 KiB of tail
            // converges without ever being charged, and the ISR holds.
            awaitLogTailConvergence(cluster, 30_000L * CI_MULT);

            try (var consumer = new RawConsumer(leaderPort)) {
                // Hot client loop: 16 KiB reads against a 32 KiB/s bucket
                // must run into the quota within a few fetches.
                FetchResponse denied = null;
                long deadline = System.currentTimeMillis() + 20_000L * CI_MULT;
                while (System.currentTimeMillis() < deadline) {
                    var resp = consumer.fetch(TOPIC, 0, FETCH_MAX_BYTES);
                    if (resp.hasError()) {
                        denied = resp;
                        break;
                    }
                    assertThat(resp.getRecords().size())
                            .as("seeded log serves non-empty fetches until the quota bites")
                            .isPositive();
                }
                assertThat(denied)
                        .as("an unpaced consumer runs into the fetch quota")
                        .isNotNull();
                assertThat(denied.getError().getCode()).isEqualTo(ErrorCodes.QUOTA_VIOLATED);
                assertThat(denied.getError().getMessage())
                        .contains("fetch quota exceeded")
                        .contains(FETCH_QUOTA_BYTES_PER_SEC + " B/s");
                long hintMillis = parseRetryHint(denied.getError().getMessage());
                assertThat(hintMillis).isPositive();

                // Backing off restores service for the same read.
                boolean served = false;
                long retryDeadline = System.currentTimeMillis() + 20_000L * CI_MULT;
                long sleepMillis = hintMillis;
                while (System.currentTimeMillis() < retryDeadline) {
                    Thread.sleep(sleepMillis + 50L);
                    var retry = consumer.fetch(TOPIC, 0, FETCH_MAX_BYTES);
                    if (!retry.hasError()) {
                        assertThat(retry.getRecords().size()).isPositive();
                        served = true;
                        break;
                    }
                    assertThat(retry.getError().getCode()).isEqualTo(ErrorCodes.QUOTA_VIOLATED);
                    sleepMillis = parseRetryHint(retry.getError().getMessage());
                }
                assertThat(served)
                        .as("the throttled consumer is served after honoring the back-off hint")
                        .isTrue();
            }

            // The whole time the client was being throttled, replication
            // stayed untouched: full ISR, identical tails.
            var leader = cluster.broker(leaderId - 1);
            assertThat(leader.topics()
                            .partitionState(TOPIC, 0)
                            .map(s -> s.isr().size())
                            .orElse(-1))
                    .as("fetch quota never shrinks the ISR")
                    .isEqualTo(3);
            awaitLogTailConvergence(cluster, 10_000L * CI_MULT);
        }
    }

    @Test
    void defaultConfigHasNoQuotaSideEffects(@TempDir Path dir) throws Exception {
        try (var cluster = TestBrokerCluster.start(
                1, 2, (i, voters, ports) -> new Broker.Config(new NodeId(1), dir, ports[0][0], ports[0][1], voters))) {
            awaitSingleRaftLeader(cluster.brokers());
            try (var admin = new BrokerClient("127.0.0.1", cluster.brokerPort(0))) {
                admin.createTopic(TOPIC, 1, 1);
            }
            awaitPartitionLeaderIsrSize(cluster, 1, 15_000L * CI_MULT);

            // Bursts far past both quota budgets used above: with no quota
            // configured, nothing is ever refused.
            try (var producer = new RawProducer(cluster.brokerPort(0));
                    var consumer = new RawConsumer(cluster.brokerPort(0))) {
                byte[] payload = new byte[PRODUCE_PAYLOAD_BYTES];
                for (int i = 0; i < 50; i++) {
                    var resp = producer.produce(TOPIC, payload);
                    assertThat(resp.hasError())
                            .as("unquotaed produce burst #%d, got %s", i, resp.getError())
                            .isFalse();
                }
                for (int i = 0; i < 50; i++) {
                    var resp = consumer.fetch(TOPIC, 0, 1 << 20);
                    assertThat(resp.hasError())
                            .as("unquotaed fetch burst #%d, got %s", i, resp.getError())
                            .isFalse();
                    assertThat(resp.getRecords().size()).isPositive();
                }
            }
        }
    }

    // ---- raw stubs (broker error codes, not client exception text) ----

    private static final class RawProducer implements AutoCloseable {
        private final ManagedChannel channel;
        private final ProducerGrpc.ProducerBlockingStub stub;

        RawProducer(int port) {
            this.channel = NettyChannelBuilder.forAddress("127.0.0.1", port)
                    .usePlaintext()
                    .build();
            this.stub = ProducerGrpc.newBlockingStub(channel);
        }

        /** Non-idempotent acks=0 produce of a single record. */
        ProduceResponse produce(String topic, byte[] value) {
            var records = List.of(new Record(0, 0L, null, value));
            var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
            long now = System.currentTimeMillis();
            RecordBatch.encode(buf, 0L, 0, now, now, -1L, (short) 0, -1, records);
            buf.flip();
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            return stub.withDeadlineAfter(10, TimeUnit.SECONDS)
                    .produce(ProduceRequest.newBuilder()
                            .setTopic(topic)
                            .setPartition(0)
                            .setBatch(ByteString.copyFrom(bytes))
                            .setProducerId(-1L)
                            .setBaseSequence(-1)
                            .build());
        }

        @Override
        public void close() {
            shutdown(channel);
        }
    }

    private static final class RawConsumer implements AutoCloseable {
        private final ManagedChannel channel;
        private final ConsumerGrpc.ConsumerBlockingStub stub;

        RawConsumer(int port) {
            this.channel = NettyChannelBuilder.forAddress("127.0.0.1", port)
                    .usePlaintext()
                    .build();
            this.stub = ConsumerGrpc.newBlockingStub(channel);
        }

        FetchResponse fetch(String topic, long offset, int maxBytes) {
            return stub.withDeadlineAfter(10, TimeUnit.SECONDS)
                    .fetch(FetchRequest.newBuilder()
                            .setTopic(topic)
                            .setPartition(0)
                            .setOffset(offset)
                            .setMaxBytes(maxBytes)
                            .build());
        }

        @Override
        public void close() {
            shutdown(channel);
        }
    }

    private static void shutdown(ManagedChannel channel) {
        channel.shutdownNow();
        try {
            channel.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static long parseRetryHint(String message) {
        var m = RETRY_HINT.matcher(message);
        assertThat(m.find())
                .as("deny message carries a retry hint: %s", message)
                .isTrue();
        return Long.parseLong(m.group(1));
    }

    // ---- cluster plumbing ----

    private static void awaitSingleRaftLeader(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000L * CI_MULT;
        while (System.currentTimeMillis() < deadline) {
            if (brokers.stream().filter(b -> b.role() == Role.LEADER).count() == 1) return;
            Thread.sleep(50);
        }
        throw new AssertionError("no single Raft leader within deadline");
    }

    private static void awaitRegistryConvergence(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000L * CI_MULT;
        while (System.currentTimeMillis() < deadline) {
            if (brokers.stream()
                    .allMatch(b -> b.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3)))) return;
            Thread.sleep(50);
        }
        throw new AssertionError("broker registry did not converge within deadline");
    }

    private static Broker raftLeaderOf(List<Broker> brokers) {
        return brokers.stream().filter(b -> b.role() == Role.LEADER).findFirst().orElseThrow();
    }

    /** Every broker agrees on a live leader whose ISR has the expected size; returns the leader id. */
    private static int awaitPartitionLeaderIsrSize(TestBrokerCluster cluster, int isrSize, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            Integer agreed = null;
            boolean ok = true;
            for (var b : cluster.brokers()) {
                var state = b.topics().partitionState(TOPIC, 0).orElse(null);
                if (state == null || state.leader() <= 0 || state.isr().size() != isrSize) {
                    ok = false;
                    break;
                }
                if (agreed == null) agreed = state.leader();
                else if (agreed != state.leader()) {
                    ok = false;
                    break;
                }
            }
            if (ok && agreed != null) return agreed;
            Thread.sleep(50);
        }
        throw new AssertionError("no agreed partition leader with ISR size " + isrSize + " within " + timeoutMs + "ms");
    }

    private static void awaitLogTailConvergence(TestBrokerCluster cluster, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        int count = cluster.brokers().size();
        long[] leos = new long[count];
        while (System.currentTimeMillis() < deadline) {
            boolean same = true;
            for (int i = 0; i < count; i++) {
                leos[i] = cluster.broker(i).logManager().logFor(TOPIC, 0).nextOffset();
                if (leos[i] != leos[0]) same = false;
            }
            if (same && leos[0] > 0) return;
            Thread.sleep(100);
        }
        throw new AssertionError("log tails did not converge: " + java.util.Arrays.toString(leos));
    }
}
