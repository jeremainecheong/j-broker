package jbroker.broker.quota;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import jbroker.broker.ErrorCodes;
import jbroker.broker.FetchHandler;
import jbroker.broker.FetchSessionCache;
import jbroker.broker.ProduceHandler;
import jbroker.broker.ReplicaFetchHandler;
import jbroker.broker.TopicManager;
import jbroker.broker.replication.FollowerStateTracker;
import jbroker.proto.broker.FetchRequest;
import jbroker.proto.broker.ProduceRequest;
import jbroker.proto.broker.ReplicaFetchRequest;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration coverage for {@link FetchHandler} + {@link QuotaEnforcer}.
 * A denied admission returns {@link ErrorCodes#QUOTA_VIOLATED} with a
 * back-off hint instead of records; the default (no-enforcer) handler is
 * untouched; and the replica-fetch path serves regardless of any fetch
 * quota — replication rides its own RPC with no quota gate.
 */
final class FetchQuotaIntegrationTest {

    private Path dir;
    private LogManager logManager;
    private TopicManager topicManager;

    @BeforeEach
    void setUp() throws Exception {
        dir = Files.createTempDirectory("fetch-quota-");
        logManager = new LogManager(
                dir,
                new LogManager.Config(
                        128L * 1024 * 1024,
                        Long.MAX_VALUE,
                        LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        java.util.concurrent.TimeUnit.MINUTES.toMillis(5)));
        topicManager = new TopicManager();
        topicManager.onTopicCommitted("orders", 1, 2, System.currentTimeMillis());
        // Broker 1 (self) leads; broker 2 is a follower replica so the
        // replica-fetch exemption below exercises an accepted fetch.
        topicManager.onPartitionChange("orders", 0, 1, List.of(1, 2), List.of(1, 2), 0, 0);
        seedRecords(4);
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
    void overQuotaFetchReturnsQuotaViolatedWithThrottleHint() {
        // 64 bytes/sec on fetch — far below one seeded batch; big produce
        // budget so only fetch is throttled.
        var enforcer = new InMemoryQuotaEnforcer(10_000_000, 64);
        var handler = new FetchHandler(
                logManager, topicManager, new FetchSessionCache(), new jbroker.broker.BrokerMetrics(), enforcer);

        var resp = handler.handle(fetchRequest());
        assertThat(resp.hasError()).isTrue();
        assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.QUOTA_VIOLATED);
        assertThat(resp.getError().getMessage())
                .contains("fetch quota exceeded")
                .contains("retry in");
        assertThat(resp.getRecords().isEmpty()).isTrue();
        // The allocated session survives the denial so the retry reuses it.
        assertThat(resp.getSessionId()).isPositive();
    }

    @Test
    void belowQuotaFetchPassesThrough() {
        var enforcer = new InMemoryQuotaEnforcer(10_000_000, 10_000_000);
        var handler = new FetchHandler(
                logManager, topicManager, new FetchSessionCache(), new jbroker.broker.BrokerMetrics(), enforcer);

        var resp = handler.handle(fetchRequest());
        assertThat(resp.hasError()).isFalse();
        assertThat(resp.getRecords().size()).isPositive();
    }

    @Test
    void defaultHandlerHasNoQuota() {
        var handler =
                new FetchHandler(logManager, topicManager, new FetchSessionCache(), new jbroker.broker.BrokerMetrics());
        // Any number of consecutive fetches passes — the default is NOOP.
        for (int i = 0; i < 10; i++) {
            var resp = handler.handle(fetchRequest());
            assertThat(resp.hasError()).isFalse();
            assertThat(resp.getRecords().size()).isPositive();
        }
    }

    @Test
    void replicaFetchServesRegardlessOfFetchQuota() {
        // A fetch quota strict enough to deny every client fetch...
        var enforcer = new InMemoryQuotaEnforcer(10_000_000, 1);
        var clientHandler = new FetchHandler(
                logManager, topicManager, new FetchSessionCache(), new jbroker.broker.BrokerMetrics(), enforcer);
        assertThat(clientHandler.handle(fetchRequest()).getError().getCode()).isEqualTo(ErrorCodes.QUOTA_VIOLATED);

        // ...must not touch replication: followers use the ReplicaFetch
        // RPC, which has no quota gate at all.
        var replicaHandler = new ReplicaFetchHandler(
                logManager, topicManager, 1, new FollowerStateTracker(), System::currentTimeMillis);
        for (int i = 0; i < 10; i++) {
            var resp = replicaHandler.handle(ReplicaFetchRequest.newBuilder()
                    .setTopic("orders")
                    .setPartition(0)
                    .setFollowerBrokerId(2)
                    .setLeaderEpoch(0)
                    .setFetchOffset(0)
                    .setMaxBytes(1 << 20)
                    .build());
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NONE);
            assertThat(resp.getRecords().size()).isPositive();
        }
    }

    private static FetchRequest fetchRequest() {
        return FetchRequest.newBuilder()
                .setTopic("orders")
                .setPartition(0)
                .setOffset(0)
                .setMaxBytes(1 << 20)
                .build();
    }

    private void seedRecords(int count) {
        var produce = new ProduceHandler(logManager, topicManager, 1, new FollowerStateTracker(), null);
        for (int i = 0; i < count; i++) {
            var resp = produce.handle(ProduceRequest.newBuilder()
                    .setTopic("orders")
                    .setPartition(0)
                    .setBatch(ByteString.copyFrom(encodeSingleton("k" + i, new byte[128], System.currentTimeMillis())))
                    .build());
            assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NONE);
        }
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
