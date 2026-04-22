package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import jbroker.proto.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * P7.11 — unit tests for the incremental fetch session cache.
 * Covers: allocation, round-trip update/lookup, and LRU eviction at the
 * cap.
 */
class FetchSessionCacheTest {

    @Test
    void allocateReturnsMonotonicPositiveIds() {
        var cache = new FetchSessionCache(8);
        int a = cache.allocate();
        int b = cache.allocate();
        int c = cache.allocate();
        assertThat(a).isPositive();
        assertThat(b).isGreaterThan(a);
        assertThat(c).isGreaterThan(b);
        assertThat(cache.size()).isEqualTo(3);
    }

    @Test
    void updatePersistsPerPartitionStateWithinASession() {
        var cache = new FetchSessionCache(8);
        int id = cache.allocate();
        var tp0 = TopicPartition.newBuilder().setTopic("t").setPartition(0).build();
        var tp1 = TopicPartition.newBuilder().setTopic("t").setPartition(1).build();
        assertThat(cache.update(id, tp0, 100L, 3)).isTrue();
        assertThat(cache.update(id, tp1, 200L, 3)).isTrue();

        var state = cache.get(id).orElseThrow();
        assertThat(state.offsetFor(tp0)).isEqualTo(100L);
        assertThat(state.offsetFor(tp1)).isEqualTo(200L);
    }

    @Test
    void updateOnUnknownSessionReturnsFalse() {
        var cache = new FetchSessionCache(8);
        var tp = TopicPartition.newBuilder().setTopic("t").setPartition(0).build();
        assertThat(cache.update(9999, tp, 0L, 0)).isFalse();
        assertThat(cache.get(9999)).isEmpty();
    }

    @Test
    void exceedingCapacityEvictsTheLeastRecentlyUsedSession() {
        var cache = new FetchSessionCache(2);
        int a = cache.allocate();
        int b = cache.allocate();
        // Touch `a` so it becomes MRU; `b` is now LRU.
        var tp = TopicPartition.newBuilder().setTopic("t").setPartition(0).build();
        cache.update(a, tp, 1L, 0);
        int c = cache.allocate(); // evicts `b`

        assertThat(cache.get(a)).isPresent();
        assertThat(cache.get(b)).isEmpty();
        assertThat(cache.get(c)).isPresent();
        assertThat(cache.size()).isEqualTo(2);
    }
}
