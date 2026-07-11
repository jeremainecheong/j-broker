package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import jbroker.broker.replication.FollowerStateTracker;
import jbroker.proto.broker.ProduceRequest;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ProduceHandlerMaxMessageBytesTest {

    private static LogManager lm(Path dir) throws Exception {
        return new LogManager(
                dir,
                new LogManager.Config(
                        128L * 1024 * 1024,
                        Long.MAX_VALUE,
                        LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        TimeUnit.MINUTES.toMillis(5)));
    }

    private static ProduceRequest produce(byte[] value) {
        var records = List.of(new Record(0, 0L, null, value));
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

    private static TopicManager topic(Map<String, String> config) {
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 1, 0L, false, false, config);
        tm.onPartitionChange("t", 0, 1, List.of(1), List.of(1), 0, 0);
        return tm;
    }

    @Test
    void oversizeBatchRejectedFatalWithoutAppend(@TempDir Path dir) throws Exception {
        try (var lmgr = lm(dir)) {
            var handler = new ProduceHandler(lmgr, topic(Map.of()), 1, new FollowerStateTracker());

            var resp = handler.handle(produce(new byte[ProduceHandler.DEFAULT_MAX_MESSAGE_BYTES + 1]));

            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.MESSAGE_TOO_LARGE);
            assertThat(resp.getError().getMessage()).contains("max.message.bytes");
            assertThat(lmgr.logFor("t", 0).nextOffset()).isZero();
        }
    }

    @Test
    void perTopicOverrideRaisesTheLimit(@TempDir Path dir) throws Exception {
        try (var lmgr = lm(dir)) {
            var tm = topic(Map.of(TopicDescription.MAX_MESSAGE_BYTES_CONFIG, String.valueOf(4 * 1024 * 1024)));
            var handler = new ProduceHandler(lmgr, tm, 1, new FollowerStateTracker());

            var resp = handler.handle(produce(new byte[2 * 1024 * 1024]));

            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(lmgr.logFor("t", 0).nextOffset()).isEqualTo(1L);
        }
    }

    @Test
    void perTopicOverrideCanTightenTheLimit(@TempDir Path dir) throws Exception {
        try (var lmgr = lm(dir)) {
            var tm = topic(Map.of(TopicDescription.MAX_MESSAGE_BYTES_CONFIG, "1024"));
            var handler = new ProduceHandler(lmgr, tm, 1, new FollowerStateTracker());

            var resp = handler.handle(produce(new byte[4 * 1024]));

            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.MESSAGE_TOO_LARGE);
            assertThat(lmgr.logFor("t", 0).nextOffset()).isZero();
        }
    }
}
