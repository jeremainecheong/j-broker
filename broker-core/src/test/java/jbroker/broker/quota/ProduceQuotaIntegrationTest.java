package jbroker.broker.quota;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import jbroker.broker.ErrorCodes;
import jbroker.broker.ProduceHandler;
import jbroker.broker.TopicManager;
import jbroker.broker.replication.FollowerStateTracker;
import jbroker.proto.broker.ProduceRequest;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration coverage for {@link ProduceHandler} + {@link QuotaEnforcer}.
 * Verifies a denied admission returns {@link ErrorCodes#QUOTA_VIOLATED} instead of
 * appending to the log.
 */
final class ProduceQuotaIntegrationTest {

    private Path dir;
    private LogManager logManager;
    private TopicManager topicManager;

    @BeforeEach
    void setUp() throws Exception {
        dir = Files.createTempDirectory("produce-quota-");
        logManager = new LogManager(
                dir,
                new LogManager.Config(
                        128L * 1024 * 1024,
                        Long.MAX_VALUE,
                        LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        java.util.concurrent.TimeUnit.MINUTES.toMillis(5)));
        topicManager = new TopicManager();
        topicManager.onTopicCommitted("orders", 1, 1, System.currentTimeMillis());
        topicManager.onPartitionChange("orders", 0, 1, List.of(1), List.of(1), 0, 0);
    }

    @AfterEach
    void tearDown() throws Exception {
        logManager.close();
        try (var walk = Files.walk(dir)) {
            walk.sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (Exception ignored) {
                }
            });
        }
    }

    @Test
    void overQuotaProduceReturnsQuotaViolated() throws Exception {
        // 100 bytes/sec on produce, big fetch budget so only produce is throttled.
        var enforcer = new InMemoryQuotaEnforcer(100, 10_000);
        var handler = new ProduceHandler(logManager, topicManager, 1, new FollowerStateTracker(), null, enforcer);

        byte[] batchBytes = encodeSingleton("k", new byte[128], System.currentTimeMillis());
        var req = ProduceRequest.newBuilder()
                .setTopic("orders")
                .setPartition(0)
                .setBatch(ByteString.copyFrom(batchBytes))
                .build();

        var resp = handler.handle(req);
        assertThat(resp.hasError()).isTrue();
        assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.QUOTA_VIOLATED);
    }

    @Test
    void overQuotaProduceCountsDenialAndThrottleTime() throws Exception {
        var enforcer = new InMemoryQuotaEnforcer(100, 10_000);
        var metrics = new jbroker.broker.BrokerMetrics();
        var handler = new ProduceHandler(logManager, topicManager, 1, new FollowerStateTracker(), metrics, enforcer);

        byte[] batchBytes = encodeSingleton("k", new byte[128], System.currentTimeMillis());
        var req = ProduceRequest.newBuilder()
                .setTopic("orders")
                .setPartition(0)
                .setBatch(ByteString.copyFrom(batchBytes))
                .build();

        var resp = handler.handle(req);
        assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.QUOTA_VIOLATED);
        // The denial and its back-off hint land on the produce-side
        // counters; the fetch side stays untouched.
        assertThat(metrics.produceQuotaDenials()).isEqualTo(1L);
        assertThat(metrics.produceQuotaThrottleMillis()).isPositive();
        assertThat(metrics.fetchQuotaDenials()).isZero();
        assertThat(metrics.fetchQuotaThrottleMillis()).isZero();
    }

    @Test
    void belowQuotaProducePassesThrough() throws Exception {
        var enforcer = new InMemoryQuotaEnforcer(10_000, 10_000);
        var handler = new ProduceHandler(logManager, topicManager, 1, new FollowerStateTracker(), null, enforcer);

        byte[] batchBytes = encodeSingleton("k", "v".getBytes(), System.currentTimeMillis());
        var req = ProduceRequest.newBuilder()
                .setTopic("orders")
                .setPartition(0)
                .setBatch(ByteString.copyFrom(batchBytes))
                .build();

        var resp = handler.handle(req);
        assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NONE);
    }

    private static byte[] encodeSingleton(String key, byte[] value, long nowMillis) {
        var records = List.of(new Record(0, 0L, key.getBytes(), value));
        ByteBuffer buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        RecordBatch.encode(buf, 0L, 0, nowMillis, nowMillis, 0L, (short) 0, 0, records);
        buf.flip();
        byte[] out = new byte[buf.remaining()];
        buf.get(out);
        return out;
    }
}
