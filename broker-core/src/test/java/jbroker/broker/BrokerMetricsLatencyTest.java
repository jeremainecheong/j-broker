package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class BrokerMetricsLatencyTest {

    @Test
    void emptyMetricsReturnsZeroedSnapshots() {
        var m = new BrokerMetrics();
        var tp = m.throughputSnapshot();
        assertThat(tp.produceCount()).isZero();
        assertThat(tp.fetchCount()).isZero();
        assertThat(tp.produceBytesPerSec()).isEqualTo(0.0);
        var pl = m.produceLatencySnapshot();
        assertThat(pl.count()).isZero();
        assertThat(pl.p50Nanos()).isZero();
    }

    @Test
    void produceRecordingAdvancesCountAndLatencyPercentiles() {
        var m = new BrokerMetrics();
        for (int i = 1; i <= 100; i++) {
            m.recordProduce(i * 1_000L, 10L);
        }
        var tp = m.throughputSnapshot();
        assertThat(tp.produceCount()).isEqualTo(100);
        assertThat(tp.produceBytes()).isEqualTo(1_000);
        assertThat(tp.produceBytesPerSec()).isGreaterThan(0.0);
        var pl = m.produceLatencySnapshot();
        assertThat(pl.count()).isEqualTo(100);
        // p50 should be somewhere in the middle (50 * 1_000 ± reservoir noise).
        assertThat(pl.p50Nanos()).isBetween(40_000L, 60_000L);
        // p99 should be near the top of the distribution.
        assertThat(pl.p99Nanos()).isBetween(90_000L, 100_000L);
    }

    @Test
    void produceAndFetchTrackedIndependently() {
        var m = new BrokerMetrics();
        m.recordProduce(1_000L, 100L);
        m.recordFetch(5_000L, 500L);

        var tp = m.throughputSnapshot();
        assertThat(tp.produceCount()).isEqualTo(1);
        assertThat(tp.fetchCount()).isEqualTo(1);
        assertThat(tp.produceBytes()).isEqualTo(100L);
        assertThat(tp.fetchBytes()).isEqualTo(500L);

        var fl = m.fetchLatencySnapshot();
        assertThat(fl.count()).isEqualTo(1);
        assertThat(fl.p50Nanos()).isEqualTo(5_000L);
    }

    @Test
    void incrementalFetchHitsStillTrackedSeparately() {
        var m = new BrokerMetrics();
        m.recordIncrementalFetchHit();
        m.recordIncrementalFetchHit();
        assertThat(m.incrementalFetchHits()).isEqualTo(2L);
    }
}
