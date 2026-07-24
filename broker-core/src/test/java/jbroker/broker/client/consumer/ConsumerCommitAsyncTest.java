package jbroker.broker.client.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import jbroker.proto.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * Broker-less commitAsync contract checks: an empty commit completes
 * without touching the network, and a commit that cannot reach the
 * coordinator surfaces on the future — never silently.
 */
class ConsumerCommitAsyncTest {

    @Test
    void nothingToCommitCompletesImmediatelyWithoutNetwork() {
        // Port 1 is never dialled: no assignment means an empty snapshot.
        var cfg = ConsumerConfig.builder("g1", "127.0.0.1", 1)
                .pollFetchDeadline(Duration.ofMillis(200))
                .build();
        try (var consumer = new Consumer<>(cfg, new StringDeserializer(), new StringDeserializer())) {
            assertThat(consumer.commitAsync()).isCompleted();
            assertThat(consumer.commitAsync(Map.of())).isCompleted();
        }
    }

    @Test
    void unreachableCoordinatorCompletesTheFutureExceptionally() throws Exception {
        var cfg = ConsumerConfig.builder("g1", "127.0.0.1", closedPort())
                .pollFetchDeadline(Duration.ofMillis(250))
                .build();
        try (var consumer = new Consumer<>(cfg, new StringDeserializer(), new StringDeserializer())) {
            var tp = TopicPartition.newBuilder()
                    .setTopic("orders")
                    .setPartition(0)
                    .build();
            var future = consumer.commitAsync(Map.of(tp, new OffsetAndMetadata(5L)));
            assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
        }
    }

    /** A port that was just free — nothing listens on it, so dials refuse fast. */
    private static int closedPort() throws IOException {
        try (var s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
