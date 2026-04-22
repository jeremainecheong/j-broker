package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import jbroker.proto.broker.OffsetsForLeaderEpochRequest;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import jbroker.storage.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class OffsetsForLeaderEpochHandlerTest {

    private static final int LEADER = 1;

    @Test
    void returnsStartOffsetOfNextEpochWhenHigherEpochExists(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 1, 0L);
        tm.onPartitionChange("t", 0, LEADER, List.of(LEADER), List.of(LEADER), /* epoch */ 5, 0);

        try (var lm = lm(dir)) {
            var cp = lm.leaderEpochCheckpoint("t", 0);
            cp.assign(2, 0L);
            cp.assign(3, 100L);
            cp.assign(5, 500L);
            // Leader has written past epoch 5's start.
            lm.logFor("t", 0).append(List.of(new Record(0, 0L, null, new byte[] {1})), 1_000L);

            var handler = new OffsetsForLeaderEpochHandler(lm, tm, LEADER);
            var resp = handler.handle(request("t", 0, /* ask about */ 3));

            // End of epoch 3 = start of epoch 5 (the next recorded epoch).
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(resp.getEndOffset()).isEqualTo(500L);
        }
    }

    @Test
    void returnsCurrentLeoWhenEpochIsTheLatest(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 1, 0L);
        tm.onPartitionChange("t", 0, LEADER, List.of(LEADER), List.of(LEADER), 5, 0);

        try (var lm = lm(dir)) {
            var cp = lm.leaderEpochCheckpoint("t", 0);
            cp.assign(5, 0L);
            var log = lm.logFor("t", 0);
            for (int i = 0; i < 7; i++) {
                log.append(List.of(new Record(0, 0L, null, new byte[] {(byte) i})), 1_000L);
            }

            var handler = new OffsetsForLeaderEpochHandler(lm, tm, LEADER);
            var resp = handler.handle(request("t", 0, 5));

            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(resp.getEndOffset()).isEqualTo(7L); // log end
        }
    }

    @Test
    void returnsNotLeaderWhenSelfIsNotPartitionLeader(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 1, 0L);
        tm.onPartitionChange("t", 0, /* other */ 2, List.of(2), List.of(2), 5, 0);

        try (var lm = lm(dir)) {
            var handler = new OffsetsForLeaderEpochHandler(lm, tm, LEADER);
            var resp = handler.handle(request("t", 0, 5));
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NOT_LEADER);
        }
    }

    @Test
    void returnsCurrentLeoWhenEpochIsUnknown(@TempDir Path dir) throws Exception {
        // Follower asks about an epoch the leader never saw. Kafka's
        // convention: reply with the UNDEFINED_EPOCH / undefined_epoch
        // sentinel so follower truncates to -1 (i.e. empty log). We use
        // endOffset = current LEO as a safer default — follower that
        // follows along sees max; the follower's own bookkeeping
        // compares against its own LEO.
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 1, 0L);
        tm.onPartitionChange("t", 0, LEADER, List.of(LEADER), List.of(LEADER), 10, 0);

        try (var lm = lm(dir)) {
            lm.logFor("t", 0).append(List.of(new Record(0, 0L, null, new byte[] {1})), 1_000L);

            var handler = new OffsetsForLeaderEpochHandler(lm, tm, LEADER);
            var resp = handler.handle(request("t", 0, /* unknown */ 99));

            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(resp.getEndOffset()).isEqualTo(1L);
        }
    }

    private static OffsetsForLeaderEpochRequest request(String topic, int partition, int leaderEpoch) {
        return OffsetsForLeaderEpochRequest.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setLeaderEpoch(leaderEpoch)
                .build();
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
