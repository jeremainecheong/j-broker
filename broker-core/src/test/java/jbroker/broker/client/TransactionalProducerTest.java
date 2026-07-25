package jbroker.broker.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import jbroker.broker.ProtocolVersion;
import jbroker.broker.client.ClusterClient.Endpoint;
import jbroker.proto.broker.ApiVersionsResponse;
import jbroker.proto.broker.BrokerInfo;
import jbroker.proto.broker.CommitOffsetsRequest;
import jbroker.proto.broker.CommitOffsetsResponse;
import jbroker.proto.broker.ConsumerGroupHeartbeatRequest;
import jbroker.proto.broker.ConsumerGroupHeartbeatResponse;
import jbroker.proto.broker.CreateTopicRequest;
import jbroker.proto.broker.CreateTopicResponse;
import jbroker.proto.broker.DescribeClusterResponse;
import jbroker.proto.broker.DescribeTopicPartitionsResponse;
import jbroker.proto.broker.Error;
import jbroker.proto.broker.FetchOffsetsRequest;
import jbroker.proto.broker.FetchOffsetsResponse;
import jbroker.proto.broker.FetchRequest;
import jbroker.proto.broker.FetchResponse;
import jbroker.proto.broker.FindCoordinatorRequest;
import jbroker.proto.broker.FindCoordinatorResponse;
import jbroker.proto.broker.InitProducerIdResponse;
import jbroker.proto.broker.ListOffsetsRequest;
import jbroker.proto.broker.ListOffsetsResponse;
import jbroker.proto.broker.PartitionStateInfo;
import jbroker.proto.broker.ProduceRequest;
import jbroker.proto.broker.ProduceResponse;
import jbroker.proto.common.BrokerEndpoint;
import jbroker.proto.common.ErrorCode;
import jbroker.proto.common.TopicPartition;
import jbroker.proto.txn.AddOffsetsToTxnRequest;
import jbroker.proto.txn.AddOffsetsToTxnResponse;
import jbroker.proto.txn.AddPartitionsToTxnRequest;
import jbroker.proto.txn.AddPartitionsToTxnResponse;
import jbroker.proto.txn.EndTxnRequest;
import jbroker.proto.txn.EndTxnResponse;
import jbroker.proto.txn.InitTransactionsRequest;
import jbroker.proto.txn.InitTransactionsResponse;
import jbroker.proto.txn.TxnOffsetCommitRequest;
import jbroker.proto.txn.TxnOffsetCommitResponse;
import jbroker.proto.txn.TxnOffsetCommitResult;
import jbroker.storage.RecordBatch;
import org.junit.jupiter.api.Test;

/**
 * {@link TransactionalProducer} state machine against a scripted
 * single-broker transport: init grant + CONCURRENT_TRANSACTIONS retry,
 * partition registration exactly once per transaction, transactional
 * batch shape and sequence progression, offset-commit registration,
 * commit/abort, fencing fatality, and the {@code transact}
 * abort-and-retry loop's epoch bump.
 */
class TransactionalProducerTest {

    private static final Endpoint B1 = new Endpoint("h1", 1001);
    private static final String TXN_ID = "app-txn";

    /** Scripted single-broker world: broker 1 leads everything and coordinates everything. */
    private static final class World {
        final AtomicInteger epochCounter = new AtomicInteger(-1);
        long nextOffset;
        final List<InitTransactionsRequest> inits = new ArrayList<>();
        final List<AddPartitionsToTxnRequest> addPartitions = new ArrayList<>();
        final List<AddOffsetsToTxnRequest> addOffsets = new ArrayList<>();
        final List<EndTxnRequest> endTxns = new ArrayList<>();
        final List<ProduceRequest> produces = new ArrayList<>();
        final List<TxnOffsetCommitRequest> offsetCommits = new ArrayList<>();
        final Map<String, ArrayDeque<Object>> scripts = new HashMap<>();

        void script(String op, Object outcome) {
            scripts.computeIfAbsent(op, k -> new ArrayDeque<>()).add(outcome);
        }

        @SuppressWarnings("unchecked")
        <T> T scripted(String op, java.util.function.Supplier<T> fallback) {
            var queue = scripts.get(op);
            if (queue != null && !queue.isEmpty()) {
                var outcome = queue.poll();
                if (outcome instanceof RuntimeException e) throw e;
                return (T) outcome;
            }
            return fallback.get();
        }
    }

    private static final class FakeTransport implements ClusterClient.Transport {
        final World world;

        FakeTransport(World world) {
            this.world = world;
        }

        @Override
        public ApiVersionsResponse apiVersions(long timeoutMs) {
            return ApiVersionsResponse.newBuilder()
                    .setError(ErrorCode.OK)
                    .setMinProtocolVersion(ProtocolVersion.MIN_SUPPORTED)
                    .setMaxProtocolVersion(ProtocolVersion.CURRENT)
                    .build();
        }

        @Override
        public DescribeClusterResponse describeCluster(long timeoutMs) {
            return DescribeClusterResponse.newBuilder()
                    .setError(ErrorCode.OK)
                    .setControllerId(1)
                    .addNodes(BrokerInfo.newBuilder()
                            .setBrokerId(1)
                            .setHost(B1.host())
                            .setPort(B1.port())
                            .setAlive(true))
                    .build();
        }

        @Override
        public DescribeTopicPartitionsResponse describeTopicPartitions(String topic, long timeoutMs) {
            return DescribeTopicPartitionsResponse.newBuilder()
                    .setError(ErrorCode.OK)
                    .setTopic(topic)
                    .setPartitions(1)
                    .addPartitionStates(
                            PartitionStateInfo.newBuilder().setPartition(0).setLeader(1))
                    .build();
        }

        @Override
        public InitProducerIdResponse initProducerId(long timeoutMs) {
            return InitProducerIdResponse.newBuilder().setProducerId(7L).build();
        }

        @Override
        public ProduceResponse produce(ProduceRequest req, long timeoutMs) {
            world.produces.add(req);
            return world.scripted("produce", () -> {
                long last = world.nextOffset++;
                return ProduceResponse.newBuilder().setLastOffset(last).build();
            });
        }

        @Override
        public FetchResponse fetch(FetchRequest req, long timeoutMs) {
            return FetchResponse.getDefaultInstance();
        }

        @Override
        public ListOffsetsResponse listOffsets(ListOffsetsRequest req, long timeoutMs) {
            return ListOffsetsResponse.getDefaultInstance();
        }

        @Override
        public FindCoordinatorResponse findCoordinator(FindCoordinatorRequest req, long timeoutMs) {
            return FindCoordinatorResponse.newBuilder()
                    .setError(ErrorCode.OK)
                    .setCoordinator(BrokerEndpoint.newBuilder()
                            .setNodeId(1)
                            .setHost(B1.host())
                            .setPort(B1.port()))
                    .build();
        }

        @Override
        public ConsumerGroupHeartbeatResponse consumerGroupHeartbeat(
                ConsumerGroupHeartbeatRequest req, long timeoutMs) {
            return ConsumerGroupHeartbeatResponse.getDefaultInstance();
        }

        @Override
        public CommitOffsetsResponse commitOffsets(CommitOffsetsRequest req, long timeoutMs) {
            return CommitOffsetsResponse.getDefaultInstance();
        }

        @Override
        public FetchOffsetsResponse fetchOffsets(FetchOffsetsRequest req, long timeoutMs) {
            return FetchOffsetsResponse.getDefaultInstance();
        }

        @Override
        public CreateTopicResponse createTopic(CreateTopicRequest req, long timeoutMs) {
            return CreateTopicResponse.getDefaultInstance();
        }

        @Override
        public InitTransactionsResponse initTransactions(InitTransactionsRequest req, long timeoutMs) {
            world.inits.add(req);
            return world.scripted("initTransactions", () -> InitTransactionsResponse.newBuilder()
                    .setError(ErrorCode.OK)
                    .setProducerId(7L)
                    .setProducerEpoch(world.epochCounter.incrementAndGet())
                    .build());
        }

        @Override
        public AddPartitionsToTxnResponse addPartitionsToTxn(AddPartitionsToTxnRequest req, long timeoutMs) {
            world.addPartitions.add(req);
            return world.scripted("addPartitionsToTxn", () -> AddPartitionsToTxnResponse.newBuilder()
                    .setError(ErrorCode.OK)
                    .build());
        }

        @Override
        public EndTxnResponse endTxn(EndTxnRequest req, long timeoutMs) {
            world.endTxns.add(req);
            return world.scripted(
                    "endTxn",
                    () -> EndTxnResponse.newBuilder().setError(ErrorCode.OK).build());
        }

        @Override
        public AddOffsetsToTxnResponse addOffsetsToTxn(AddOffsetsToTxnRequest req, long timeoutMs) {
            world.addOffsets.add(req);
            return world.scripted("addOffsetsToTxn", () -> AddOffsetsToTxnResponse.newBuilder()
                    .setError(ErrorCode.OK)
                    .build());
        }

        @Override
        public TxnOffsetCommitResponse txnOffsetCommit(TxnOffsetCommitRequest req, long timeoutMs) {
            world.offsetCommits.add(req);
            return world.scripted("txnOffsetCommit", () -> {
                var b = TxnOffsetCommitResponse.newBuilder();
                for (var o : req.getOffsetsList()) {
                    b.addResults(
                            TxnOffsetCommitResult.newBuilder().setTp(o.getTp()).setError(ErrorCode.OK));
                }
                return b.build();
            });
        }

        @Override
        public void close() {}
    }

    private static final class Harness {
        final World world = new World();
        final List<Long> sleeps = new ArrayList<>();
        final ClusterClient cluster;
        final TransactionalProducer producer;

        Harness() {
            this(TransactionalProducer.Config.defaults());
        }

        Harness(TransactionalProducer.Config config) {
            cluster = new ClusterClient(
                    List.of(B1),
                    new ClusterClient.Config(1, 1.0, 1, 30_000, 1_000),
                    ep -> new FakeTransport(world),
                    ClusterClient.Ticker.SYSTEM,
                    new Random(42));
            // Record AND actually sleep: deadline math runs on the real
            // clock, so a zero-cost sleep would spin the retry loops far
            // past their scripted responses.
            producer = new TransactionalProducer(cluster, TXN_ID, config, ms -> {
                sleeps.add(ms);
                Thread.sleep(ms);
            });
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // ---- init ----

    @Test
    void initAdoptsTheGrantAndRetriesConcurrentTransactionsWithBackoff() {
        var h = new Harness();
        h.world.script(
                "initTransactions",
                InitTransactionsResponse.newBuilder()
                        .setError(ErrorCode.CONCURRENT_TRANSACTIONS)
                        .build());

        h.producer.initTransactions();

        assertThat(h.producer.producerId()).isEqualTo(7L);
        assertThat(h.producer.producerEpoch()).isZero();
        assertThat(h.world.inits).hasSize(2);
        assertThat(h.sleeps)
                .as("CONCURRENT_TRANSACTIONS retried after a backoff")
                .hasSize(1);
    }

    @Test
    void lifecycleMisuseFailsCoherently() {
        var h = new Harness();
        assertThatThrownBy(h.producer::beginTransaction)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("initTransactions");
        h.producer.initTransactions();
        assertThatThrownBy(() -> h.producer.send("t", 0, bytes("v")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("beginTransaction");
        h.producer.beginTransaction();
        assertThatThrownBy(h.producer::beginTransaction)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already in flight");
    }

    // ---- send ----

    @Test
    void sendRegistersThePartitionOnceAndProducesTransactionalBatches() {
        var h = new Harness();
        h.producer.initTransactions();
        h.producer.beginTransaction();

        h.producer.send("orders", 0, bytes("a"));
        h.producer.send("orders", 0, bytes("b"));

        assertThat(h.world.addPartitions)
                .as("one registration per partition per transaction")
                .hasSize(1);
        assertThat(h.world.addPartitions.get(0).getPartitionsList()).anySatisfy(tp -> {
            assertThat(tp.getTopic()).isEqualTo("orders");
            assertThat(tp.getPartition()).isZero();
        });
        assertThat(h.world.produces).hasSize(2);
        var first = RecordBatch.decode(
                ByteBuffer.wrap(h.world.produces.get(0).getBatch().toByteArray()));
        var second = RecordBatch.decode(
                ByteBuffer.wrap(h.world.produces.get(1).getBatch().toByteArray()));
        assertThat(first.transactional()).isTrue();
        assertThat(first.producerId()).isEqualTo(7L);
        assertThat(first.producerEpoch()).isZero();
        assertThat(first.baseSequence()).isZero();
        assertThat(second.baseSequence()).as("sequences progress per partition").isEqualTo(1);
    }

    @Test
    void sequencesSurviveCommitButResetOnReinit() {
        var h = new Harness();
        h.producer.initTransactions();
        h.producer.beginTransaction();
        h.producer.send("orders", 0, bytes("a"));
        h.producer.commitTransaction();

        h.producer.beginTransaction();
        h.producer.send("orders", 0, bytes("b"));
        var afterCommit = RecordBatch.decode(
                ByteBuffer.wrap(h.world.produces.get(1).getBatch().toByteArray()));
        assertThat(afterCommit.baseSequence())
                .as("same epoch continues the sequence across transactions")
                .isEqualTo(1);
        h.producer.commitTransaction();

        h.producer.initTransactions();
        h.producer.beginTransaction();
        h.producer.send("orders", 0, bytes("c"));
        var afterReinit = RecordBatch.decode(
                ByteBuffer.wrap(h.world.produces.get(2).getBatch().toByteArray()));
        assertThat(afterReinit.producerEpoch()).isEqualTo((short) 1);
        assertThat(afterReinit.baseSequence()).as("epoch bump resets sequences").isZero();
    }

    @Test
    void producerFencedOnProduceIsFatal() {
        var h = new Harness();
        h.producer.initTransactions();
        h.producer.beginTransaction();
        h.world.script(
                "produce",
                ProduceResponse.newBuilder()
                        .setError(Error.newBuilder()
                                .setCode(jbroker.broker.ErrorCodes.PRODUCER_FENCED)
                                .setMessage("fenced"))
                        .build());

        assertThatThrownBy(() -> h.producer.send("orders", 0, bytes("a")))
                .isInstanceOf(TransactionalProducer.ProducerFencedException.class);
    }

    // ---- offsets ----

    @Test
    void sendOffsetsRegistersTheGroupOncePerTransaction() {
        var h = new Harness();
        h.producer.initTransactions();
        h.producer.beginTransaction();
        var tp = TopicPartition.newBuilder().setTopic("src").setPartition(0).build();

        h.producer.sendOffsetsToTransaction("g", Map.of(tp, 5L));
        h.producer.sendOffsetsToTransaction("g", Map.of(tp, 9L));

        assertThat(h.world.addOffsets)
                .as("one AddOffsetsToTxn per group per transaction")
                .hasSize(1);
        assertThat(h.world.addOffsets.get(0).getGroupId()).isEqualTo("g");
        assertThat(h.world.offsetCommits).hasSize(2);
        assertThat(h.world.offsetCommits.get(1).getOffsets(0).getOffset()).isEqualTo(9L);
        assertThat(h.world.offsetCommits.get(0).getProducerId()).isEqualTo(7L);

        h.producer.commitTransaction();
        h.producer.beginTransaction();
        h.producer.sendOffsetsToTransaction("g", Map.of(tp, 11L));
        assertThat(h.world.addOffsets)
                .as("a new transaction re-registers the group")
                .hasSize(2);
    }

    @Test
    void fencedOffsetCommitIsFatal() {
        var h = new Harness();
        h.producer.initTransactions();
        h.producer.beginTransaction();
        var tp = TopicPartition.newBuilder().setTopic("src").setPartition(0).build();
        h.world.script(
                "txnOffsetCommit",
                TxnOffsetCommitResponse.newBuilder()
                        .addResults(TxnOffsetCommitResult.newBuilder().setTp(tp).setError(ErrorCode.PRODUCER_FENCED))
                        .build());

        assertThatThrownBy(() -> h.producer.sendOffsetsToTransaction("g", Map.of(tp, 5L)))
                .isInstanceOf(TransactionalProducer.ProducerFencedException.class);
    }

    // ---- commit / abort ----

    @Test
    void commitSendsEndTxnAndRetriesConcurrentTransactions() {
        var h = new Harness();
        h.producer.initTransactions();
        h.producer.beginTransaction();
        h.producer.send("orders", 0, bytes("a"));
        h.world.script(
                "endTxn",
                EndTxnResponse.newBuilder()
                        .setError(ErrorCode.CONCURRENT_TRANSACTIONS)
                        .build());

        h.producer.commitTransaction();

        assertThat(h.world.endTxns).hasSize(2);
        assertThat(h.world.endTxns).allSatisfy(e -> assertThat(e.getCommit()).isTrue());
        assertThat(h.sleeps).hasSize(1);
    }

    @Test
    void abortSendsEndTxnWithCommitFalse() {
        var h = new Harness();
        h.producer.initTransactions();
        h.producer.beginTransaction();
        h.producer.send("orders", 0, bytes("a"));

        h.producer.abortTransaction();

        assertThat(h.world.endTxns).hasSize(1);
        assertThat(h.world.endTxns.get(0).getCommit()).isFalse();
    }

    @Test
    void untouchedTransactionCommitsWithoutARoundTrip() {
        var h = new Harness();
        h.producer.initTransactions();
        h.producer.beginTransaction();
        h.producer.commitTransaction();
        assertThat(h.world.endTxns).isEmpty();

        // And the producer is immediately usable again.
        h.producer.beginTransaction();
        h.producer.send("orders", 0, bytes("a"));
        h.producer.commitTransaction();
        assertThat(h.world.endTxns).hasSize(1);
    }

    @Test
    void fencedEndTxnIsFatal() {
        var h = new Harness();
        h.producer.initTransactions();
        h.producer.beginTransaction();
        h.producer.send("orders", 0, bytes("a"));
        h.world.script(
                "endTxn",
                EndTxnResponse.newBuilder().setError(ErrorCode.PRODUCER_FENCED).build());

        assertThatThrownBy(h.producer::commitTransaction)
                .isInstanceOf(TransactionalProducer.ProducerFencedException.class);
    }

    // ---- transact: the abort-and-retry loop ----

    @Test
    void transactAbortsReinitsAndRetriesAnAbortableFailure() {
        var h = new Harness();
        h.producer.initTransactions();
        // First attempt's produce fails with a non-retriable, non-fenced
        // envelope; the loop must abort, re-init (epoch bump), and rerun.
        h.world.script(
                "produce",
                ProduceResponse.newBuilder()
                        .setError(Error.newBuilder()
                                .setCode(jbroker.broker.ErrorCodes.INVALID_TXN_STATE)
                                .setMessage("unregistered"))
                        .build());

        var attempts = new AtomicInteger();
        h.producer.transact(() -> {
            attempts.incrementAndGet();
            h.producer.send("orders", 0, bytes("a"));
        });

        assertThat(attempts.get()).isEqualTo(2);
        // Attempt 1: abort (endTxn commit=false); then re-init; attempt 2 commits.
        assertThat(h.world.endTxns).hasSize(2);
        assertThat(h.world.endTxns.get(0).getCommit()).isFalse();
        assertThat(h.world.endTxns.get(1).getCommit()).isTrue();
        assertThat(h.world.inits).as("initial init + the retry's epoch bump").hasSize(2);
        assertThat(h.producer.producerEpoch()).isEqualTo(1);
        var retried = RecordBatch.decode(ByteBuffer.wrap(
                h.world.produces.get(h.world.produces.size() - 1).getBatch().toByteArray()));
        assertThat(retried.producerEpoch())
                .as("retry runs under the bumped epoch")
                .isEqualTo((short) 1);
        assertThat(retried.baseSequence()).as("bumped epoch restarts sequences").isZero();
    }

    @Test
    void transactPropagatesFencingWithoutRetry() {
        var h = new Harness();
        h.producer.initTransactions();
        h.world.script(
                "produce",
                ProduceResponse.newBuilder()
                        .setError(Error.newBuilder()
                                .setCode(jbroker.broker.ErrorCodes.PRODUCER_FENCED)
                                .setMessage("fenced"))
                        .build());

        var attempts = new AtomicInteger();
        assertThatThrownBy(() -> h.producer.transact(() -> {
                    attempts.incrementAndGet();
                    h.producer.send("orders", 0, bytes("a"));
                }))
                .isInstanceOf(TransactionalProducer.ProducerFencedException.class);
        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test
    void transactNeverTurnsACommitFailureIntoAnAbort() {
        var h = new Harness(new TransactionalProducer.Config(0, 1, 50, 200));
        h.producer.initTransactions();
        // Every EndTxn answers CONCURRENT_TRANSACTIONS until the op
        // deadline: the commit's outcome is unknown, so transact must
        // propagate rather than abort-and-rerun the body.
        for (int i = 0; i < 1_000; i++) {
            h.world.script(
                    "endTxn",
                    EndTxnResponse.newBuilder()
                            .setError(ErrorCode.CONCURRENT_TRANSACTIONS)
                            .build());
        }

        var attempts = new AtomicInteger();
        assertThatThrownBy(() -> h.producer.transact(() -> {
                    attempts.incrementAndGet();
                    h.producer.send("orders", 0, bytes("a"));
                }))
                .hasMessageContaining("commitTransaction");
        assertThat(attempts.get())
                .as("the body never reruns after commit was attempted")
                .isEqualTo(1);
        assertThat(h.world.endTxns).allSatisfy(e -> assertThat(e.getCommit()).isTrue());
    }
}
