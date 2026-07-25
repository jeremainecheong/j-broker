package jbroker.broker.txn;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import jbroker.broker.ErrorCodes;
import jbroker.broker.TopicManager;
import jbroker.broker.replication.FollowerStateTracker;
import jbroker.proto.common.ErrorCode;
import jbroker.proto.common.TopicPartition;
import jbroker.proto.txn.TxnMarker;
import jbroker.proto.txn.WriteTxnMarkersRequest;
import jbroker.storage.Compression;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import jbroker.storage.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The leader-local marker append: leadership gate, deposed-coordinator
 * fencing, idempotent redelivery, and the effect a marker has on the
 * partition's transaction state (LSO release, aborted-range recording,
 * producer-epoch floor).
 */
class TxnMarkerWriterTest {

    private static final int SELF = 1;
    private static final long PID = 42L;

    @Test
    void commitMarkerLandsReleasesLsoAndRaisesEpochFloor(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var tm = leaderTm();
            var epochs = new TxnPartitionEpochs();
            var writer = writer(lm, tm, epochs);
            var log = lm.logFor("orders", 0);
            log.append(
                    List.of(new Record(0, 0L, null, "v0".getBytes())),
                    1L,
                    PID,
                    (short) 0,
                    0,
                    0,
                    Compression.NONE,
                    /*transactional*/ true);
            assertThat(log.lastStableOffset(log.nextOffset())).isZero();

            int code = writer.append("orders", 0, PID, /*epoch*/ 0, /*commit*/ true, /*coordEpoch*/ 0);
            assertThat(code).isEqualTo(ErrorCodes.NONE);
            assertThat(log.nextOffset()).isEqualTo(2L); // data + marker
            assertThat(log.lastStableOffset(log.nextOffset()))
                    .as("the marker decides the transaction, releasing the LSO")
                    .isEqualTo(2L);
            assertThat(log.read(1L, 1 << 20).get(0).control()).isTrue();
            assertThat(epochs.maxEpochOf("orders", 0, PID)).isEqualTo(0);
        }
    }

    @Test
    void abortMarkerRecordsTheAbortedRange(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var writer = writer(lm, leaderTm(), new TxnPartitionEpochs());
            var log = lm.logFor("orders", 0);
            log.append(
                    List.of(new Record(0, 0L, null, "d0".getBytes()), new Record(1, 0L, null, "d1".getBytes())),
                    1L,
                    PID,
                    (short) 0,
                    0,
                    0,
                    Compression.NONE,
                    true);
            // Abort with the bumped epoch (init-with-ongoing shape).
            int code = writer.append("orders", 0, PID, /*epoch*/ 1, /*commit*/ false, /*coordEpoch*/ 0);
            assertThat(code).isEqualTo(ErrorCodes.NONE);
            var aborted = log.abortedTxnsIn(0, log.nextOffset());
            assertThat(aborted).hasSize(1);
            assertThat(aborted.get(0).producerId()).isEqualTo(PID);
            assertThat(aborted.get(0).firstOffset()).isZero();
            assertThat(aborted.get(0).lastOffset()).isEqualTo(1L);
        }
    }

    @Test
    void duplicateDeliveryDoesNotAppendASecondMarker(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var writer = writer(lm, leaderTm(), new TxnPartitionEpochs());
            var log = lm.logFor("orders", 0);
            log.append(
                    List.of(new Record(0, 0L, null, "v".getBytes())), 1L, PID, (short) 0, 0, 0, Compression.NONE, true);
            assertThat(writer.append("orders", 0, PID, 0, true, 0)).isEqualTo(ErrorCodes.NONE);
            long end = log.nextOffset();
            assertThat(writer.append("orders", 0, PID, 0, true, 0))
                    .as("redelivery is confirmed without new bytes")
                    .isEqualTo(ErrorCodes.NONE);
            assertThat(log.nextOffset()).isEqualTo(end);
        }
    }

    @Test
    void nonLeaderRefusesTheMarker(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var tm = new TopicManager();
            tm.onTopicCommitted("orders", 1, 1, 0L);
            tm.onPartitionChange("orders", 0, /*leader*/ 2, List.of(2), 0);
            var writer = writer(lm, tm, new TxnPartitionEpochs());
            assertThat(writer.append("orders", 0, PID, 0, true, 0)).isEqualTo(ErrorCodes.NOT_LEADER);
            assertThat(lm.logFor("orders", 0).nextOffset()).isZero();
        }
    }

    @Test
    void deposedCoordinatorEpochIsFenced(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var writer = writer(lm, leaderTm(), new TxnPartitionEpochs());
            var log = lm.logFor("orders", 0);
            log.append(
                    List.of(new Record(0, 0L, null, "v".getBytes())), 1L, PID, (short) 0, 0, 0, Compression.NONE, true);
            assertThat(writer.append("orders", 0, PID, 0, true, /*coordEpoch*/ 2))
                    .isEqualTo(ErrorCodes.NONE);
            long end = log.nextOffset();
            assertThat(writer.append("orders", 0, PID, 0, true, /*coordEpoch*/ 1))
                    .as("a lower coordinator epoch is a deposed coordinator")
                    .isEqualTo(ErrorCodes.INVALID_TXN_STATE);
            assertThat(log.nextOffset()).isEqualTo(end);
        }
    }

    @Test
    void handlerMapsWriterCodesOntoTheWireEnum(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            // Partition 0 led by self, partition 1 led elsewhere.
            var tm = new TopicManager();
            tm.onTopicCommitted("orders", 2, 1, 0L);
            tm.onPartitionChange("orders", 0, SELF, List.of(SELF), 0);
            tm.onPartitionChange("orders", 1, 2, List.of(2), 0);
            var handler = new TxnMarkersHandler(writer(lm, tm, new TxnPartitionEpochs()));
            lm.logFor("orders", 0)
                    .append(
                            List.of(new Record(0, 0L, null, "v".getBytes())),
                            1L,
                            PID,
                            (short) 0,
                            0,
                            0,
                            Compression.NONE,
                            true);

            var resp = handler.handle(WriteTxnMarkersRequest.newBuilder()
                    .addMarkers(TxnMarker.newBuilder()
                            .setProducerId(PID)
                            .setProducerEpoch(0)
                            .setCommit(true)
                            .setCoordinatorEpoch(0)
                            .addPartitions(tp("orders", 0))
                            .addPartitions(tp("orders", 1)))
                    .build());

            assertThat(resp.getResultsList()).hasSize(2);
            assertThat(resp.getResults(0).getError()).isEqualTo(ErrorCode.OK);
            assertThat(resp.getResults(1).getError()).isEqualTo(ErrorCode.STALE_LEADER_EPOCH);
        }
    }

    // --- helpers ---

    private static TopicPartition tp(String topic, int partition) {
        return TopicPartition.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .build();
    }

    private static TopicManager leaderTm() {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 1, 0L);
        tm.onPartitionChange("orders", 0, SELF, List.of(SELF), 0);
        return tm;
    }

    private static TxnMarkerWriter writer(LogManager lm, TopicManager tm, TxnPartitionEpochs epochs) {
        return new TxnMarkerWriter(lm, tm, new FollowerStateTracker(), SELF, /*clusterMinIsr*/ 2, epochs);
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
