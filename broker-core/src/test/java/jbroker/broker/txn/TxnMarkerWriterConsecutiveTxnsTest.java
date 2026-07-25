package jbroker.broker.txn;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import jbroker.broker.ErrorCodes;
import jbroker.broker.TopicManager;
import jbroker.broker.replication.FollowerStateTracker;
import jbroker.storage.Compression;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import jbroker.storage.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Consecutive transactions under a stable producer epoch produce
 * byte-shape-identical markers; the redelivery dedup must not swallow the
 * next transaction's marker (which would leave it undecided forever and
 * wedge the partition's LSO). Only a marker that is still the log's last
 * entry counts as a redelivery.
 */
class TxnMarkerWriterConsecutiveTxnsTest {

    private static final int SELF = 1;
    private static final long PID = 42L;

    @Test
    void secondSameEpochCommitMarkerLandsAsAFreshControlBatch(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var writer = writer(lm, leaderTm());
            var log = lm.logFor("orders", 0);

            log.append(
                    List.of(new Record(0, 0L, null, "a".getBytes())), 1L, PID, (short) 0, 0, 0, Compression.NONE, true);
            assertThat(writer.append("orders", 0, PID, 0, true, 0)).isEqualTo(ErrorCodes.NONE);
            log.append(
                    List.of(new Record(0, 0L, null, "b".getBytes())), 1L, PID, (short) 0, 1, 0, Compression.NONE, true);
            assertThat(writer.append("orders", 0, PID, 0, true, 0)).isEqualTo(ErrorCodes.NONE);

            assertThat(log.nextOffset()).as("data, marker, data, marker").isEqualTo(4L);
            assertThat(log.lastStableOffset(log.nextOffset()))
                    .as("both transactions decided")
                    .isEqualTo(4L);
        }
    }

    @Test
    void immediateRedeliveryStillReusesTheAppendedMarker(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var writer = writer(lm, leaderTm());
            var log = lm.logFor("orders", 0);
            log.append(
                    List.of(new Record(0, 0L, null, "a".getBytes())), 1L, PID, (short) 0, 0, 0, Compression.NONE, true);

            assertThat(writer.append("orders", 0, PID, 0, true, 0)).isEqualTo(ErrorCodes.NONE);
            long end = log.nextOffset();
            assertThat(writer.append("orders", 0, PID, 0, true, 0))
                    .as("nothing appended since — a true redelivery re-runs only the wait")
                    .isEqualTo(ErrorCodes.NONE);
            assertThat(log.nextOffset()).isEqualTo(end);
        }
    }

    // --- helpers ---

    private static TopicManager leaderTm() {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 1, 0L);
        tm.onPartitionChange("orders", 0, SELF, List.of(SELF), 0);
        return tm;
    }

    private static TxnMarkerWriter writer(LogManager lm, TopicManager tm) {
        return new TxnMarkerWriter(
                lm, tm, new FollowerStateTracker(), SELF, /*clusterMinIsr*/ 2, new TxnPartitionEpochs());
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
