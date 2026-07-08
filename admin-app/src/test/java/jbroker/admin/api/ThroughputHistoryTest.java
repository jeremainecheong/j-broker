package jbroker.admin.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import jbroker.proto.broker.DescribeMetricsResponse;
import jbroker.proto.common.ErrorCode;
import org.junit.jupiter.api.Test;

/**
 * Coverage for the bounded time-series ring the admin-app
 * feeds sparklines from. Pure unit — no broker needed.
 */
class ThroughputHistoryTest {

    @Test
    void appendAggregatesPerBrokerResponsesIntoOneSample() {
        var history = new ThroughputHistory(10);
        var snap = new MetricsScraper.Snapshot(Map.of(
                1,
                metrics(1, 1000.0, 4000.0, 5_000_000, 20_000_000),
                2,
                metrics(2, 3000.0, 6000.0, 8_000_000, 15_000_000)));

        history.append(snap);

        var samples = history.all();
        assertThat(samples).hasSize(1);
        var s = samples.get(0);
        // produce/fetch bytes/s summed across brokers
        assertThat(s.produceBytesPerSec()).isEqualTo(4000.0);
        assertThat(s.fetchBytesPerSec()).isEqualTo(10_000.0);
        // p99 taken as max (conservative cluster-wide worst-case)
        assertThat(s.produceP99Nanos()).isEqualTo(8_000_000);
        assertThat(s.fetchP99Nanos()).isEqualTo(20_000_000);
    }

    @Test
    void ringEvictsOldestWhenOverCapacity() {
        var history = new ThroughputHistory(3);
        for (int i = 0; i < 5; i++) {
            history.append(new MetricsScraper.Snapshot(Map.of(1, metrics(1, /* produceBps */ i * 100.0, 0.0, 0, 0))));
        }

        var samples = history.all();
        assertThat(samples).hasSize(3);
        // Should be the last 3 (i=2,3,4).
        assertThat(samples)
                .extracting(ThroughputHistory.Sample::produceBytesPerSec)
                .containsExactly(200.0, 300.0, 400.0);
    }

    @Test
    void sinceWindowFiltersByTimestamp() throws InterruptedException {
        var history = new ThroughputHistory(10);
        history.append(new MetricsScraper.Snapshot(Map.of(1, metrics(1, 100, 0, 0, 0))));
        Thread.sleep(150);
        history.append(new MetricsScraper.Snapshot(Map.of(1, metrics(1, 200, 0, 0, 0))));

        // Window only captures the newer sample.
        var recent = history.since(Duration.ofMillis(100));
        assertThat(recent).hasSize(1);
        assertThat(recent.get(0).produceBytesPerSec()).isEqualTo(200.0);

        // Wide window captures both.
        var all = history.since(Duration.ofSeconds(10));
        assertThat(all).hasSize(2);
    }

    @Test
    void capacityMustBePositive() {
        assertThat(new ThroughputHistory(1).capacity()).isEqualTo(1);
        try {
            new ThroughputHistory(0);
            throw new AssertionError("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            assertThat(expected.getMessage()).contains("capacity");
        }
    }

    private static DescribeMetricsResponse metrics(
            int brokerId, double produceBps, double fetchBps, long produceP99Nanos, long fetchP99Nanos) {
        return DescribeMetricsResponse.newBuilder()
                .setError(ErrorCode.OK)
                .setBrokerId(brokerId)
                .setProduceBytesPerSec(produceBps)
                .setFetchBytesPerSec(fetchBps)
                .setProduceP99Nanos(produceP99Nanos)
                .setFetchP99Nanos(fetchP99Nanos)
                .build();
    }
}
