package jbroker.broker.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import jbroker.broker.TopicManager;
import jbroker.broker.replication.FollowerStateTracker;
import jbroker.storage.Log;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import jbroker.storage.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class DefaultPartitionMetricsProviderTest {

    private Path dir;
    private LogManager logManager;
    private TopicManager topicManager;
    private FollowerStateTracker tracker;

    @BeforeEach
    void setUp() throws IOException {
        dir = Files.createTempDirectory("pmp-");
        logManager = new LogManager(
                dir,
                new LogManager.Config(
                        128L * 1024 * 1024,
                        Long.MAX_VALUE,
                        LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        TimeUnit.MINUTES.toMillis(5)));
        topicManager = new TopicManager();
        tracker = new FollowerStateTracker();
    }

    @AfterEach
    void tearDown() throws IOException {
        logManager.close();
        try (var walk = Files.walk(dir)) {
            walk.sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                }
            });
        }
    }

    @Test
    void leaderOnlyPartitionsWithHwmAndLag() throws IOException {
        // Topic "orders" — partition 0: self (1) is leader, followers {2, 3}; partition 1: broker 2 leads.
        topicManager.onPartitionChange("orders", 0, 1, List.of(1, 2, 3), List.of(1, 2, 3), 0, 0);
        topicManager.onPartitionChange("orders", 1, 2, List.of(2, 3), List.of(2, 3), 0, 0);

        // Leader's LEO: append two records to partition 0 on local LogManager.
        Log p0 = logManager.logFor("orders", 0);
        p0.append(List.of(new Record(0, 0L, "k".getBytes(), "v".getBytes())), 1_000L);
        p0.append(List.of(new Record(0, 0L, "k2".getBytes(), "v2".getBytes())), 1_000L);
        long leaderLeo = p0.nextOffset();

        // Follower 2 fetched up to LEO; follower 3 stale at 1.
        tracker.record("orders", 0, 2, leaderLeo, 100L);
        tracker.record("orders", 0, 3, 1L, 100L);

        var provider = new DefaultPartitionMetricsProvider(1, topicManager, logManager, tracker);
        List<PartitionMetricsSnapshot> snap = provider.snapshot();

        assertThat(snap).hasSize(1);
        var p0Snap = snap.get(0);
        assertThat(p0Snap.topic()).isEqualTo("orders");
        assertThat(p0Snap.partition()).isEqualTo(0);
        assertThat(p0Snap.isrSize()).isEqualTo(3);
        assertThat(p0Snap.leaderLogEndOffset()).isEqualTo(leaderLeo);
        // HWM = min(LEO across ISR) = min(leaderLeo, leaderLeo, 1) = 1.
        assertThat(p0Snap.hwm()).isEqualTo(1L);
        assertThat(p0Snap.replicationLagBytes()).containsEntry(2, 0L).containsEntry(3, leaderLeo - 1L);
    }

    @Test
    void followerBrokerReturnsEmptySnapshot() {
        topicManager.onPartitionChange("orders", 0, 2, List.of(1, 2, 3), List.of(1, 2, 3), 0, 0);
        var provider = new DefaultPartitionMetricsProvider(1, topicManager, logManager, tracker);
        assertThat(provider.snapshot()).isEmpty();
    }
}
