package jbroker.broker.group;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class OffsetCacheTest {

    @Test
    void getReturnsEmptyForUnknownKey() {
        var cache = new OffsetCache();
        assertThat(cache.get("g1", "orders", 0)).isEmpty();
    }

    @Test
    void putThenGetRoundTrips() {
        var cache = new OffsetCache();
        cache.put("g1", "orders", 0, new OffsetCache.OffsetAndMetadata(42L, 3, "ckpt", 999L));
        var got = cache.get("g1", "orders", 0).orElseThrow();
        assertThat(got.offset()).isEqualTo(42L);
        assertThat(got.leaderEpoch()).isEqualTo(3);
        assertThat(got.metadata()).isEqualTo("ckpt");
        assertThat(got.commitTimestamp()).isEqualTo(999L);
    }

    @Test
    void putOverwritesExisting() {
        var cache = new OffsetCache();
        cache.put("g1", "orders", 0, new OffsetCache.OffsetAndMetadata(10L, 1, "", 100L));
        cache.put("g1", "orders", 0, new OffsetCache.OffsetAndMetadata(20L, 2, "", 200L));
        assertThat(cache.get("g1", "orders", 0).orElseThrow().offset()).isEqualTo(20L);
    }

    @Test
    void differentGroupsAndPartitionsAreIsolated() {
        var cache = new OffsetCache();
        cache.put("g1", "t", 0, new OffsetCache.OffsetAndMetadata(1L, 0, "", 0L));
        cache.put("g2", "t", 0, new OffsetCache.OffsetAndMetadata(2L, 0, "", 0L));
        cache.put("g1", "t", 1, new OffsetCache.OffsetAndMetadata(3L, 0, "", 0L));
        assertThat(cache.get("g1", "t", 0).orElseThrow().offset()).isEqualTo(1L);
        assertThat(cache.get("g2", "t", 0).orElseThrow().offset()).isEqualTo(2L);
        assertThat(cache.get("g1", "t", 1).orElseThrow().offset()).isEqualTo(3L);
        assertThat(cache.size()).isEqualTo(3);
    }

    @Test
    void offsetAndMetadataNormalisesNullMetadataToEmptyString() {
        var oam = new OffsetCache.OffsetAndMetadata(1L, 0, null, 0L);
        assertThat(oam.metadata()).isEmpty();
    }
}
