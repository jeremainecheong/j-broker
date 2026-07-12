package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import jbroker.broker.replication.FollowerStateTracker;
import jbroker.proto.broker.ProduceRequest;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The disk-headroom produce gate (R2.3): while the data volume sits below
 * the watermark, produces fail fast with retriable STORAGE_FULL and
 * nothing is appended; the broker recovers on its own when space frees.
 */
class ProduceHandlerStorageFullTest {

    private static final long GIB = 1024L * 1024 * 1024;

    private static LogManager lm(Path dir) throws Exception {
        return new LogManager(
                dir,
                new LogManager.Config(
                        128L * 1024 * 1024,
                        Long.MAX_VALUE,
                        LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        TimeUnit.MINUTES.toMillis(5)));
    }

    private static ProduceRequest produce() {
        var records = List.of(new Record(0, 0L, null, new byte[8]));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        RecordBatch.encode(buf, 0L, 0, 1_000L, 1_000L, -1L, (short) -1, -1, records);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return ProduceRequest.newBuilder()
                .setTopic("t")
                .setPartition(0)
                .setBatch(ByteString.copyFrom(bytes))
                .setAcks(1)
                .setProducerId(-1L)
                .setBaseSequence(-1)
                .build();
    }

    private static TopicManager topic() {
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 1, 0L);
        tm.onPartitionChange("t", 0, 1, List.of(1), List.of(1), 0, 0);
        return tm;
    }

    private static ProduceHandler handler(LogManager lmgr, DiskHeadroom headroom) {
        return new ProduceHandler(
                lmgr, topic(), 1, new FollowerStateTracker(), null, null, null, /*minInsyncReplicas*/ 1, headroom);
    }

    @Test
    void produceRefusedWhileLowAndNothingIsAppended(@TempDir Path dir) throws Exception {
        try (var lmgr = lm(dir);
                var headroom = new DiskHeadroom(() -> GIB / 2, GIB, 60_000)) {
            var h = handler(lmgr, headroom);

            var resp = h.handle(produce());

            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.STORAGE_FULL);
            assertThat(resp.getError().getMessage()).contains("headroom");
            assertThat(lmgr.logFor("t", 0).nextOffset()).isZero();
        }
    }

    @Test
    void produceResumesOnceSpaceFrees(@TempDir Path dir) throws Exception {
        var usable = new AtomicLong(GIB / 2);
        try (var lmgr = lm(dir);
                var headroom = new DiskHeadroom(usable::get, GIB, 20)) {
            var h = handler(lmgr, headroom);
            assertThat(h.handle(produce()).getError().getCode()).isEqualTo(ErrorCodes.STORAGE_FULL);

            usable.set(4 * GIB);
            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline && headroom.low()) {
                Thread.sleep(10);
            }

            var resp = h.handle(produce());
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(lmgr.logFor("t", 0).nextOffset()).isEqualTo(1L);
        }
    }

    @Test
    void disabledHeadroomNeverGates(@TempDir Path dir) throws Exception {
        try (var lmgr = lm(dir)) {
            var h = handler(lmgr, DiskHeadroom.disabled());

            assertThat(h.handle(produce()).getError().getCode()).isEqualTo(ErrorCodes.NONE);
        }
    }
}
