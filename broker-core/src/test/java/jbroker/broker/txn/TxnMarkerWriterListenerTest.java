package jbroker.broker.txn;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
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
 * The marker-appended listener seam: fires exactly once per fresh control
 * batch (idempotent redeliveries re-run only the replication wait), and a
 * throwing listener never fails the marker append — the fold it drives is
 * reproducible from the log.
 */
class TxnMarkerWriterListenerTest {

    private static final int SELF = 1;
    private static final long PID = 42L;

    private record Fired(String topic, int partition, long producerId, int producerEpoch, boolean commit) {}

    @Test
    void listenerFiresOncePerFreshMarkerNotOnRedelivery(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var writer = writer(lm, leaderTm());
            var fired = new ArrayList<Fired>();
            writer.setMarkerListener((topic, partition, pid, epoch, commit) ->
                    fired.add(new Fired(topic, partition, pid, epoch, commit)));
            var log = lm.logFor("orders", 0);
            log.append(
                    List.of(new Record(0, 0L, null, "v".getBytes())), 1L, PID, (short) 0, 0, 0, Compression.NONE, true);

            assertThat(writer.append("orders", 0, PID, 0, true, 0)).isEqualTo(ErrorCodes.NONE);
            assertThat(fired).containsExactly(new Fired("orders", 0, PID, 0, true));

            // Identical redelivery: no new bytes, no second notification.
            assertThat(writer.append("orders", 0, PID, 0, true, 0)).isEqualTo(ErrorCodes.NONE);
            assertThat(fired).hasSize(1);
        }
    }

    @Test
    void throwingListenerDoesNotFailTheAppend(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var writer = writer(lm, leaderTm());
            writer.setMarkerListener((topic, partition, pid, epoch, commit) -> {
                throw new IllegalStateException("fold blew up");
            });
            var log = lm.logFor("orders", 0);
            log.append(
                    List.of(new Record(0, 0L, null, "v".getBytes())), 1L, PID, (short) 0, 0, 0, Compression.NONE, true);

            assertThat(writer.append("orders", 0, PID, 0, true, 0)).isEqualTo(ErrorCodes.NONE);
            assertThat(log.read(1L, 1 << 20).get(0).control()).isTrue();
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
