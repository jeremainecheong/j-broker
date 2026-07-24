package jbroker.broker.quota;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * {@link QuotaEnforcer#configured} selection rules: zero rates disable,
 * blank Redis URL keeps enforcement in-process, and an op with no
 * configured rate stays unlimited rather than inheriting the other op's.
 */
final class QuotaEnforcerConfiguredTest {

    @Test
    void bothRatesDisabledSelectsNoop() {
        assertThat(QuotaEnforcer.configured(0, 0, "")).isSameAs(QuotaEnforcer.NOOP);
        assertThat(QuotaEnforcer.configured(-1, 0, "redis://localhost:6379")).isSameAs(QuotaEnforcer.NOOP);
    }

    @Test
    void blankOrNullRedisUrlSelectsInMemory() {
        assertThat(QuotaEnforcer.configured(100, 100, "")).isInstanceOf(InMemoryQuotaEnforcer.class);
        assertThat(QuotaEnforcer.configured(100, 100, null)).isInstanceOf(InMemoryQuotaEnforcer.class);
    }

    @Test
    void nonBlankRedisUrlSelectsRedis() {
        assertThat(QuotaEnforcer.configured(100, 100, "redis://localhost:6379")).isInstanceOf(RedisQuotaEnforcer.class);
    }

    @Test
    void disabledFetchRateLeavesFetchUnlimited() {
        var enforcer = QuotaEnforcer.configured(100, 0, "");
        // Far past any bucket a 100 B/s produce rate could imply.
        for (int i = 0; i < 5; i++) {
            assertThat(enforcer.check("a", QuotaEnforcer.Op.FETCH, 1_000_000).allow())
                    .isTrue();
        }
        // The produce quota still bites.
        enforcer.check("a", QuotaEnforcer.Op.PRODUCE, 100);
        assertThat(enforcer.check("a", QuotaEnforcer.Op.PRODUCE, 100).allow()).isFalse();
    }

    @Test
    void disabledProduceRateLeavesProduceUnlimited() {
        var enforcer = QuotaEnforcer.configured(0, 100, "");
        for (int i = 0; i < 5; i++) {
            assertThat(enforcer.check("a", QuotaEnforcer.Op.PRODUCE, 1_000_000).allow())
                    .isTrue();
        }
        enforcer.check("a", QuotaEnforcer.Op.FETCH, 100);
        assertThat(enforcer.check("a", QuotaEnforcer.Op.FETCH, 100).allow()).isFalse();
    }
}
