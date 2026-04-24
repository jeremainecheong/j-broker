package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ProducerStateManagerTest {

    @Test
    void dedupOrAppendCachesSuccessfulResult() {
        var psm = new ProducerStateManager();
        var key = new ProducerStateManager.DedupKey("t", 0, 42L, 0);

        // First call: fresh append, returns fresh offsets.
        var r1 = psm.dedupOrAppend(key, /*baseSeq*/ 0, /*recordCount*/ 3, () -> new long[] {0L, 2L});
        assertThat(r1.cached()).isFalse();
        assertThat(r1.baseOffset()).isEqualTo(0L);
        assertThat(r1.lastOffset()).isEqualTo(2L);

        // Retry with identical (baseSeq, recordCount): returns cached.
        var r2 = psm.dedupOrAppend(key, 0, 3, () -> {
            throw new AssertionError("append must not run for a cached retry");
        });
        assertThat(r2.cached()).isTrue();
        assertThat(r2.baseOffset()).isEqualTo(0L);
        assertThat(r2.lastOffset()).isEqualTo(2L);
    }

    @Test
    void dedupOrAppendRejectsOutOfOrderSequence() {
        var psm = new ProducerStateManager();
        var key = new ProducerStateManager.DedupKey("t", 0, 42L, 0);

        psm.dedupOrAppend(key, /*baseSeq*/ 0, 3, () -> new long[] {0L, 2L});

        // Retry a stale baseSequence — out-of-order because cached window
        // advanced past it.
        var stale = psm.dedupOrAppend(
                key, /*baseSeq*/
                0, /*count*/
                1,
                () -> {
                    throw new AssertionError("must not append");
                });
        assertThat(stale.hasError()).isTrue();
        assertThat(stale.errorMessage()).contains("base_sequence");
    }

    @Test
    void observeAppendPopulatesStateForFollowerPath() {
        var psm = new ProducerStateManager();
        var key = new ProducerStateManager.DedupKey("t", 0, 42L, 0);

        psm.observeAppend(key, /*baseSeq*/ 5, /*count*/ 2, /*baseOff*/ 50L, /*lastOff*/ 51L);
        var cached = psm.get(key).orElseThrow();
        assertThat(cached.lastBaseSequence()).isEqualTo(5);
        assertThat(cached.recordCount()).isEqualTo(2);
        assertThat(cached.baseOffset()).isEqualTo(50L);
        assertThat(cached.lastOffset()).isEqualTo(51L);
    }

    @Test
    void observeAppendIgnoresOlderThanCurrentSequence() {
        var psm = new ProducerStateManager();
        var key = new ProducerStateManager.DedupKey("t", 0, 42L, 0);

        psm.observeAppend(key, 5, 2, 50L, 51L);
        psm.observeAppend(key, /*stale*/ 3, 1, 30L, 30L);

        assertThat(psm.get(key).orElseThrow().lastBaseSequence()).isEqualTo(5);
    }

    @Test
    void evictTopicDropsAllPartitionsOfThatTopic() {
        var psm = new ProducerStateManager();
        psm.observeAppend(new ProducerStateManager.DedupKey("a", 0, 1L, 0), 0, 1, 0L, 0L);
        psm.observeAppend(new ProducerStateManager.DedupKey("a", 1, 1L, 0), 0, 1, 0L, 0L);
        psm.observeAppend(new ProducerStateManager.DedupKey("b", 0, 1L, 0), 0, 1, 0L, 0L);

        psm.evictTopic("a");

        assertThat(psm.get(new ProducerStateManager.DedupKey("a", 0, 1L, 0))).isEmpty();
        assertThat(psm.get(new ProducerStateManager.DedupKey("a", 1, 1L, 0))).isEmpty();
        assertThat(psm.get(new ProducerStateManager.DedupKey("b", 0, 1L, 0))).isPresent();
    }

    @Test
    void lruEvictionKicksInWhenCapExceeded() {
        var psm = new ProducerStateManager(/*maxEntries*/ 3);

        // Fill past the cap — oldest eviction happens on the 4th insert.
        for (int pid = 0; pid < 5; pid++) {
            psm.observeAppend(
                    new ProducerStateManager.DedupKey("t", 0, pid, 0),
                    /*baseSeq*/ 0,
                    /*count*/ 1,
                    /*baseOff*/ pid,
                    /*lastOff*/ pid);
        }

        assertThat(psm.size()).isEqualTo(3);
        assertThat(psm.evictionCount()).isEqualTo(2L);
        // pid=0 and pid=1 were evicted (oldest in insertion order).
        assertThat(psm.get(new ProducerStateManager.DedupKey("t", 0, 0L, 0))).isEmpty();
        assertThat(psm.get(new ProducerStateManager.DedupKey("t", 0, 1L, 0))).isEmpty();
        assertThat(psm.get(new ProducerStateManager.DedupKey("t", 0, 4L, 0))).isPresent();
    }

    @Test
    void accessOrderKeepsHotKeysAlive() {
        var psm = new ProducerStateManager(/*maxEntries*/ 3);

        var hotKey = new ProducerStateManager.DedupKey("t", 0, 1L, 0);
        psm.observeAppend(hotKey, 0, 1, 0L, 0L);
        psm.observeAppend(new ProducerStateManager.DedupKey("t", 0, 2L, 0), 0, 1, 0L, 0L);
        psm.observeAppend(new ProducerStateManager.DedupKey("t", 0, 3L, 0), 0, 1, 0L, 0L);

        // Touch the hot key so it's the MOST recently used again.
        psm.get(hotKey);

        // Add two more — the two middle entries (pid=2, pid=3) should evict, not hotKey.
        psm.observeAppend(new ProducerStateManager.DedupKey("t", 0, 4L, 0), 0, 1, 0L, 0L);
        psm.observeAppend(new ProducerStateManager.DedupKey("t", 0, 5L, 0), 0, 1, 0L, 0L);

        assertThat(psm.get(hotKey)).as("hot key must survive eviction pass").isPresent();
    }

    @Test
    void rejectsInvalidMaxEntries() {
        assertThatThrownBy(() -> new ProducerStateManager(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ProducerStateManager(-1)).isInstanceOf(IllegalArgumentException.class);
    }
}
