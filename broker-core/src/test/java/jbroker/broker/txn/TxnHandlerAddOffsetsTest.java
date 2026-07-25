package jbroker.broker.txn;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import jbroker.broker.BrokerRegistry;
import jbroker.broker.ConsumerOffsetsTopic;
import jbroker.broker.TopicManager;
import jbroker.broker.replication.FollowerStateTracker;
import jbroker.proto.common.ErrorCode;
import jbroker.proto.txn.AddOffsetsToTxnRequest;
import jbroker.proto.txn.InitTransactionsRequest;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code AddOffsetsToTxn} — the thin translation of a consumer group onto
 * its {@code __consumer_offsets} coordinator partition, registered in the
 * transaction like any data partition so the markers reach it.
 */
class TxnHandlerAddOffsetsTest {

    private static final int SELF = 1;
    private static final String GROUP = "ctp-app";

    @Test
    void registersTheGroupsOffsetsPartitionInTheTransaction(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var tm = new TopicManager();
            tm.onTopicCommitted(TxnStateTopic.NAME, 1, 1, 0L);
            tm.onPartitionChange(TxnStateTopic.NAME, 0, SELF, List.of(SELF), 0);
            tm.onTopicCommitted(ConsumerOffsetsTopic.NAME, 4, 1, 0L);
            var pair = handler(lm, tm);

            var init = pair.handler.initTransactions(InitTransactionsRequest.newBuilder()
                    .setTransactionalId("t1")
                    .build());
            assertThat(init.getError()).isEqualTo(ErrorCode.OK);

            var resp = pair.handler.addOffsetsToTxn(AddOffsetsToTxnRequest.newBuilder()
                    .setTransactionalId("t1")
                    .setProducerId(init.getProducerId())
                    .setProducerEpoch(init.getProducerEpoch())
                    .setGroupId(GROUP)
                    .build());
            assertThat(resp.getError()).isEqualTo(ErrorCode.OK);

            int expectedPartition = Math.floorMod(GROUP.hashCode(), 4);
            var state = pair.runtime.stateOf(0, "t1").orElseThrow();
            assertThat(state.partitions())
                    .as("the group's offsets partition is registered like a data partition")
                    .anySatisfy(tp -> {
                        assertThat(tp.getTopic()).isEqualTo(ConsumerOffsetsTopic.NAME);
                        assertThat(tp.getPartition()).isEqualTo(expectedPartition);
                    });
            assertThat(state.state()).isEqualTo(TxnState.ONGOING);
        }
    }

    @Test
    void missingOffsetsTopicAnswersCoordinatorNotAvailable(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var tm = new TopicManager();
            tm.onTopicCommitted(TxnStateTopic.NAME, 1, 1, 0L);
            tm.onPartitionChange(TxnStateTopic.NAME, 0, SELF, List.of(SELF), 0);
            var pair = handler(lm, tm);
            var init = pair.handler.initTransactions(InitTransactionsRequest.newBuilder()
                    .setTransactionalId("t1")
                    .build());

            var resp = pair.handler.addOffsetsToTxn(AddOffsetsToTxnRequest.newBuilder()
                    .setTransactionalId("t1")
                    .setProducerId(init.getProducerId())
                    .setProducerEpoch(init.getProducerEpoch())
                    .setGroupId(GROUP)
                    .build());
            assertThat(resp.getError()).isEqualTo(ErrorCode.COORDINATOR_NOT_AVAILABLE);
        }
    }

    @Test
    void staleEpochIsFenced(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var tm = new TopicManager();
            tm.onTopicCommitted(TxnStateTopic.NAME, 1, 1, 0L);
            tm.onPartitionChange(TxnStateTopic.NAME, 0, SELF, List.of(SELF), 0);
            tm.onTopicCommitted(ConsumerOffsetsTopic.NAME, 1, 1, 0L);
            var pair = handler(lm, tm);
            var init = pair.handler.initTransactions(InitTransactionsRequest.newBuilder()
                    .setTransactionalId("t1")
                    .build());

            var resp = pair.handler.addOffsetsToTxn(AddOffsetsToTxnRequest.newBuilder()
                    .setTransactionalId("t1")
                    .setProducerId(init.getProducerId())
                    .setProducerEpoch(init.getProducerEpoch() + 1)
                    .setGroupId(GROUP)
                    .build());
            assertThat(resp.getError()).isEqualTo(ErrorCode.PRODUCER_FENCED);
        }
    }

    @Test
    void foreignCoordinatorAnswersNotCoordinatorWithHints(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var tm = new TopicManager();
            tm.onTopicCommitted(TxnStateTopic.NAME, 1, 1, 0L);
            tm.onPartitionChange(TxnStateTopic.NAME, 0, /*leader*/ 2, List.of(2), 0);
            tm.onTopicCommitted(ConsumerOffsetsTopic.NAME, 1, 1, 0L);
            var registry = new BrokerRegistry();
            registry.onBrokerRegistration(2, "broker2.internal", 9092, "broker2.example.com", 31092, 7000);
            var runtime = runtime(lm, tm);
            var handler = new TxnHandler(tm, registry, SELF, runtime);

            var resp = handler.addOffsetsToTxn(AddOffsetsToTxnRequest.newBuilder()
                    .setTransactionalId("t1")
                    .setProducerId(1L)
                    .setProducerEpoch(0)
                    .setGroupId(GROUP)
                    .build());
            assertThat(resp.getError()).isEqualTo(ErrorCode.NOT_COORDINATOR);
            assertThat(resp.getSuggestedCoordinatorHost()).isEqualTo("broker2.example.com");
        }
    }

    // --- helpers ---

    private record Pair(TxnHandler handler, TxnCoordinatorRuntime runtime) {}

    private static Pair handler(LogManager lm, TopicManager tm) {
        var runtime = runtime(lm, tm);
        return new Pair(new TxnHandler(tm, new BrokerRegistry(), SELF, runtime), runtime);
    }

    private static TxnCoordinatorRuntime runtime(LogManager lm, TopicManager tm) {
        var pidCounter = new AtomicLong();
        return new TxnCoordinatorRuntime(
                lm,
                tm,
                new FollowerStateTracker(),
                SELF,
                /*clusterMinIsr*/ 2,
                pidCounter::incrementAndGet,
                instruction -> true,
                System::currentTimeMillis);
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
