package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import jbroker.proto.broker.ReplicaFetchRequest;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import jbroker.storage.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReplicaFetchHandlerTest {

    private static final int LEADER = 1;
    private static final int FOLLOWER = 2;

    @Test
    void returnsBatchesAndHwmWhenEpochMatches(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, LEADER, List.of(LEADER, FOLLOWER), 0);

        try (var lm = lm(dir)) {
            // Leader already has one batch written locally.
            lm.logFor("orders", 0).append(List.of(new Record(0, 0L, null, new byte[] {1, 2, 3})), 1_000L);

            var handler = new ReplicaFetchHandler(lm, tm, LEADER);
            var req = ReplicaFetchRequest.newBuilder()
                    .setTopic("orders")
                    .setPartition(0)
                    .setFollowerBrokerId(FOLLOWER)
                    .setLeaderEpoch(0)
                    .setFetchOffset(0L)
                    .build();

            var resp = handler.handle(req);
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(resp.getCurrentLeaderEpoch()).isEqualTo(0);
            assertThat(resp.getHighWatermark()).isGreaterThanOrEqualTo(0L);
            assertThat(resp.getRecords().size()).isGreaterThan(0);
        }
    }

    @Test
    void rejectsWhenSelfIsNotPartitionLeader(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, FOLLOWER, List.of(FOLLOWER, LEADER), 0);

        try (var lm = lm(dir)) {
            var handler = new ReplicaFetchHandler(lm, tm, LEADER);
            var resp = handler.handle(request(FOLLOWER, 0, 0L));
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NOT_LEADER);
        }
    }

    @Test
    void rejectsWhenFollowerHasStaleEpoch(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, LEADER, List.of(LEADER, FOLLOWER), /* current epoch */ 5);

        try (var lm = lm(dir)) {
            var handler = new ReplicaFetchHandler(lm, tm, LEADER);
            // Follower thinks the leader epoch is 3; really it's 5.
            var resp = handler.handle(request(FOLLOWER, /* stale epoch */ 3, 0L));
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.FENCED_EPOCH);
            assertThat(resp.getCurrentLeaderEpoch()).isEqualTo(5);
        }
    }

    @Test
    void rejectsFetchFromBrokerNotInIsr(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        // ISR is [LEADER, FOLLOWER]; an impostor broker id 99 is not a replica.
        tm.onPartitionChange("orders", 0, LEADER, List.of(LEADER, FOLLOWER), 0);

        try (var lm = lm(dir)) {
            var handler = new ReplicaFetchHandler(lm, tm, LEADER);
            var resp = handler.handle(request(/* impostor */ 99, 0, 0L));
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NOT_LEADER);
            assertThat(resp.getError().getMessage()).contains("not a replica");
        }
    }

    @Test
    void returnsEmptyRecordsWhenFollowerIsAtLogEnd(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, LEADER, List.of(LEADER, FOLLOWER), 0);

        try (var lm = lm(dir)) {
            var log = lm.logFor("orders", 0);
            log.append(List.of(new Record(0, 0L, null, new byte[] {7})), 1_000L);

            var handler = new ReplicaFetchHandler(lm, tm, LEADER);
            var resp = handler.handle(request(FOLLOWER, 0, log.nextOffset()));
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(resp.getRecords().size()).isZero();
        }
    }

    private static ReplicaFetchRequest request(int follower, int epoch, long offset) {
        return ReplicaFetchRequest.newBuilder()
                .setTopic("orders")
                .setPartition(0)
                .setFollowerBrokerId(follower)
                .setLeaderEpoch(epoch)
                .setFetchOffset(offset)
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
