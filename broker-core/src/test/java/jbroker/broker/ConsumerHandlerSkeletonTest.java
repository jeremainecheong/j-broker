package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import jbroker.proto.broker.CommitOffsetsRequest;
import jbroker.proto.broker.ConsumerGroupHeartbeatRequest;
import jbroker.proto.broker.FetchOffsetsRequest;
import jbroker.proto.broker.FindCoordinatorRequest;
import jbroker.proto.broker.ListOffsetsPartition;
import jbroker.proto.broker.ListOffsetsRequest;
import jbroker.proto.broker.OffsetCommit;
import jbroker.proto.common.ErrorCode;
import jbroker.proto.common.TopicPartition;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * P7.1 skeleton verification — ensures the new Consumer-group RPCs are wired
 * end-to-end through {@link ConsumerHandler} and return the expected
 * placeholder error codes ({@code COORDINATOR_NOT_AVAILABLE} for
 * coordinator-routed RPCs, {@code OFFSET_OUT_OF_RANGE} for
 * {@code FetchOffsets}). Subsequent slices replace the placeholders.
 *
 * <p>{@code ListOffsets} IS implemented in P7.1 — it just delegates to
 * {@link LogManager#logFor(String, int)} and returns {@code nextOffset()} for
 * the latest sentinel.
 */
class ConsumerHandlerSkeletonTest {

    @Test
    void findCoordinatorReturnsCoordinatorNotAvailableByDefault(@TempDir Path dir) throws IOException {
        var handler = newHandler(dir);
        var resp = handler.findCoordinator(
                FindCoordinatorRequest.newBuilder().setKey("any-group").build());
        assertThat(resp.getError()).isEqualTo(ErrorCode.COORDINATOR_NOT_AVAILABLE);
    }

    @Test
    void consumerGroupHeartbeatReturnsCoordinatorNotAvailableByDefault(@TempDir Path dir) throws IOException {
        var handler = newHandler(dir);
        var resp = handler.consumerGroupHeartbeat(
                ConsumerGroupHeartbeatRequest.newBuilder().setGroupId("g1").build());
        assertThat(resp.getError()).isEqualTo(ErrorCode.COORDINATOR_NOT_AVAILABLE);
    }

    @Test
    void commitOffsetsReturnsCoordinatorNotAvailableByDefault(@TempDir Path dir) throws IOException {
        var handler = newHandler(dir);
        var resp = handler.commitOffsets(CommitOffsetsRequest.newBuilder()
                .setGroupId("g1")
                .addCommits(OffsetCommit.newBuilder()
                        .setTp(TopicPartition.newBuilder()
                                .setTopic("orders")
                                .setPartition(0)
                                .build())
                        .setOffset(42L)
                        .build())
                .build());
        assertThat(resp.getResultsCount()).isEqualTo(1);
        assertThat(resp.getResults(0).getError()).isEqualTo(ErrorCode.COORDINATOR_NOT_AVAILABLE);
    }

    @Test
    void fetchOffsetsReturnsOffsetOutOfRangeByDefault(@TempDir Path dir) throws IOException {
        var handler = newHandler(dir);
        var resp = handler.fetchOffsets(FetchOffsetsRequest.newBuilder()
                .setGroupId("g1")
                .addTps(TopicPartition.newBuilder()
                        .setTopic("orders")
                        .setPartition(0)
                        .build())
                .build());
        assertThat(resp.getResultsCount()).isEqualTo(1);
        assertThat(resp.getResults(0).getError()).isEqualTo(ErrorCode.OFFSET_OUT_OF_RANGE);
        assertThat(resp.getResults(0).getOffset()).isEqualTo(-1L);
    }

    @Test
    void listOffsetsReturnsLatestOffsetForKnownPartition(@TempDir Path dir) throws IOException {
        var topicManager = new TopicManager();
        topicManager.onTopicCommitted("orders", 1, 1, System.currentTimeMillis());
        var logManager = newLogManager(dir);
        // Ensure the partition log exists so nextOffset() == 0.
        logManager.logFor("orders", 0);
        var handler = new ConsumerHandler(topicManager, logManager, new BrokerRegistry());

        var resp = handler.listOffsets(ListOffsetsRequest.newBuilder()
                .setReplicaId(-1)
                .addPartitions(ListOffsetsPartition.newBuilder()
                        .setTp(TopicPartition.newBuilder()
                                .setTopic("orders")
                                .setPartition(0)
                                .build())
                        .setTimestamp(-1)
                        .build())
                .build());

        assertThat(resp.getResultsCount()).isEqualTo(1);
        var result = resp.getResults(0);
        assertThat(result.getError()).isEqualTo(ErrorCode.OK);
        assertThat(result.getOffset()).isEqualTo(0L);
    }

    @Test
    void listOffsetsReturnsUnknownTopicForMissingTopic(@TempDir Path dir) throws IOException {
        var handler = newHandler(dir);
        var resp = handler.listOffsets(ListOffsetsRequest.newBuilder()
                .setReplicaId(-1)
                .addPartitions(ListOffsetsPartition.newBuilder()
                        .setTp(TopicPartition.newBuilder()
                                .setTopic("nope")
                                .setPartition(0)
                                .build())
                        .setTimestamp(-1)
                        .build())
                .build());
        assertThat(resp.getResults(0).getError()).isEqualTo(ErrorCode.UNKNOWN);
    }

    private static ConsumerHandler newHandler(Path dir) throws IOException {
        return new ConsumerHandler(new TopicManager(), newLogManager(dir), new BrokerRegistry());
    }

    private static LogManager newLogManager(Path dir) throws IOException {
        return new LogManager(
                dir,
                new LogManager.Config(
                        128L * 1024 * 1024,
                        Long.MAX_VALUE,
                        LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        java.util.concurrent.TimeUnit.MINUTES.toMillis(5)));
    }
}
