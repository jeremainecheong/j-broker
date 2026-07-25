package jbroker.broker.txn;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import jbroker.broker.BrokerRegistry;
import jbroker.broker.TopicManager;
import jbroker.broker.replication.FollowerStateTracker;
import jbroker.proto.common.ErrorCode;
import jbroker.proto.common.TopicPartition;
import jbroker.proto.txn.AddPartitionsToTxnRequest;
import jbroker.proto.txn.EndTxnRequest;
import jbroker.proto.txn.InitTransactionsRequest;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Routing for the client-facing Txn RPCs: coordinator-as-partition-leader
 * over {@code __transaction_state}, NOT_COORDINATOR with suggested hints,
 * COORDINATOR_NOT_AVAILABLE while leaderless, and the full happy path
 * through a real runtime when self coordinates.
 */
class TxnHandlerTest {

    private static final int SELF = 1;

    @Test
    void missingTopicAnswersCoordinatorNotAvailable(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var tm = new TopicManager();
            var handler = handler(lm, tm, new BrokerRegistry());
            var resp = handler.initTransactions(init("t1"));
            assertThat(resp.getError()).isEqualTo(ErrorCode.COORDINATOR_NOT_AVAILABLE);
            assertThat(resp.getProducerId()).isEqualTo(-1L);
        }
    }

    @Test
    void leaderlessPartitionAnswersCoordinatorNotAvailable(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var tm = new TopicManager();
            tm.onTopicCommitted(TxnStateTopic.NAME, 1, 1, 0L);
            tm.onPartitionChange(TxnStateTopic.NAME, 0, /*leader*/ -1, List.of(), 0);
            var handler = handler(lm, tm, new BrokerRegistry());
            assertThat(handler.endTxn(end("t1", true)).getError()).isEqualTo(ErrorCode.COORDINATOR_NOT_AVAILABLE);
        }
    }

    @Test
    void foreignCoordinatorAnswersNotCoordinatorWithHints(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var tm = new TopicManager();
            tm.onTopicCommitted(TxnStateTopic.NAME, 1, 1, 0L);
            tm.onPartitionChange(TxnStateTopic.NAME, 0, /*leader*/ 2, List.of(2), 0);
            var registry = new BrokerRegistry();
            registry.onBrokerRegistration(2, "broker2.internal", 9092, "broker2.example.com", 31092, 7000);
            var handler = handler(lm, tm, registry);

            var resp = handler.addPartitionsToTxn(AddPartitionsToTxnRequest.newBuilder()
                    .setTransactionalId("t1")
                    .setProducerId(1L)
                    .setProducerEpoch(0)
                    .addPartitions(tp("orders", 0))
                    .build());
            assertThat(resp.getError()).isEqualTo(ErrorCode.NOT_COORDINATOR);
            assertThat(resp.getSuggestedCoordinatorId()).isEqualTo(2);
            assertThat(resp.getSuggestedCoordinatorHost()).isEqualTo("broker2.example.com");
            assertThat(resp.getSuggestedCoordinatorPort()).isEqualTo(31092);
        }
    }

    @Test
    void selfCoordinatedHappyPathRoundTrips(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var tm = new TopicManager();
            tm.onTopicCommitted(TxnStateTopic.NAME, 1, 1, 0L);
            tm.onPartitionChange(TxnStateTopic.NAME, 0, SELF, List.of(SELF), 0);
            var handler = handler(lm, tm, new BrokerRegistry());

            var initResp = handler.initTransactions(init("t1"));
            assertThat(initResp.getError()).isEqualTo(ErrorCode.OK);
            assertThat(initResp.getProducerId()).isEqualTo(1L);
            assertThat(initResp.getProducerEpoch()).isZero();

            var addResp = handler.addPartitionsToTxn(AddPartitionsToTxnRequest.newBuilder()
                    .setTransactionalId("t1")
                    .setProducerId(1L)
                    .setProducerEpoch(0)
                    .addPartitions(tp("orders", 0))
                    .build());
            assertThat(addResp.getError()).isEqualTo(ErrorCode.OK);

            assertThat(handler.endTxn(end("t1", true)).getError()).isEqualTo(ErrorCode.OK);

            // Stale-epoch retry after the decision: fenced by the core.
            var fenced = handler.addPartitionsToTxn(AddPartitionsToTxnRequest.newBuilder()
                    .setTransactionalId("t1")
                    .setProducerId(1L)
                    .setProducerEpoch(1)
                    .addPartitions(tp("orders", 0))
                    .build());
            assertThat(fenced.getError()).isEqualTo(ErrorCode.PRODUCER_FENCED);
        }
    }

    // --- helpers ---

    private static InitTransactionsRequest init(String txnId) {
        return InitTransactionsRequest.newBuilder().setTransactionalId(txnId).build();
    }

    private static EndTxnRequest end(String txnId, boolean commit) {
        return EndTxnRequest.newBuilder()
                .setTransactionalId(txnId)
                .setProducerId(1L)
                .setProducerEpoch(0)
                .setCommit(commit)
                .build();
    }

    private static TopicPartition tp(String topic, int partition) {
        return TopicPartition.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .build();
    }

    private static TxnHandler handler(LogManager lm, TopicManager tm, BrokerRegistry registry) {
        var pidCounter = new AtomicLong();
        var runtime = new TxnCoordinatorRuntime(
                lm,
                tm,
                new FollowerStateTracker(),
                SELF,
                /*clusterMinIsr*/ 2,
                pidCounter::incrementAndGet,
                instruction -> true,
                System::currentTimeMillis);
        return new TxnHandler(tm, registry, SELF, runtime);
    }

    private static LogManager lm(Path dir) throws Exception {
        return new LogManager(
                dir,
                new LogManager.Config(
                        128L * 1024 * 1024,
                        Long.MAX_VALUE,
                        LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        TimeUnit.MINUTES.toMillis(5)));
    }
}
