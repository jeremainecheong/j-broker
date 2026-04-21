package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import jbroker.proto.broker.ProduceRequest;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ProduceHandlerLeaderCheckTest {

    private static final int SELF = 1;
    private static final int OTHER = 2;

    @Test
    void rejectsWhenPartitionLeaderIsUnknown(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 1, 0L);
        // No onPartitionChange — partition-state map is empty.

        try (var lm = lm(dir)) {
            var handler = new ProduceHandler(lm, tm, SELF);
            var resp = handler.handle(produceRequest("orders", 0));
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NOT_LEADER);
            assertThat(resp.getError().getMessage()).contains("no leader");
        }
    }

    @Test
    void rejectsWhenSelfIsNotPartitionLeader(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, OTHER, List.of(OTHER, SELF), 1);

        try (var lm = lm(dir)) {
            var handler = new ProduceHandler(lm, tm, SELF);
            var resp = handler.handle(produceRequest("orders", 0));
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NOT_LEADER);
            assertThat(resp.getError().getMessage()).contains(String.valueOf(OTHER));
        }
    }

    @Test
    void acceptsWhenSelfIsPartitionLeader(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, SELF, List.of(SELF, OTHER), 1);

        try (var lm = lm(dir)) {
            var handler = new ProduceHandler(lm, tm, SELF);
            var resp = handler.handle(produceRequest("orders", 0));
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(resp.getBaseOffset()).isEqualTo(0L);
            assertThat(resp.getLastOffset()).isEqualTo(0L);
        }
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

    private static ProduceRequest produceRequest(String topic, int partition) {
        var records = List.of(new Record(0, 0L, null, new byte[] {1}));
        var buf = ByteBuffer.allocate(256);
        int written = RecordBatch.encode(buf, 0L, 0, 1L, 1L, -1L, (short) -1, -1, records);
        buf.flip();
        byte[] bytes = new byte[written];
        buf.get(bytes);
        return ProduceRequest.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setBatch(ByteString.copyFrom(bytes))
                .build();
    }
}
