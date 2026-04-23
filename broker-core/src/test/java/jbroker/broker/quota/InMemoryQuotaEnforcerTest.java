package jbroker.broker.quota;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

final class InMemoryQuotaEnforcerTest {

    @Test
    void firstSecondAcceptsUpToQuotaThenDenies() {
        // 100 bytes/sec produce quota; 1000 bytes/sec fetch.
        var enforcer = new InMemoryQuotaEnforcer(100, 1000);
        // Burst up to 100 bytes is allowed.
        assertThat(enforcer.check("client-a", QuotaEnforcer.Op.PRODUCE, 50).allow())
                .isTrue();
        assertThat(enforcer.check("client-a", QuotaEnforcer.Op.PRODUCE, 50).allow())
                .isTrue();
        // 1 more byte pushes over the ceiling; denied.
        var denied = enforcer.check("client-a", QuotaEnforcer.Op.PRODUCE, 1);
        assertThat(denied.allow()).isFalse();
        assertThat(denied.quotaBytesPerSec()).isEqualTo(100);
        assertThat(denied.throttleMillis()).isPositive();
    }

    @Test
    void separatePrincipalsHaveIndependentBuckets() {
        var enforcer = new InMemoryQuotaEnforcer(100, 100);
        enforcer.check("a", QuotaEnforcer.Op.PRODUCE, 100);
        // "b" still has a fresh bucket.
        assertThat(enforcer.check("b", QuotaEnforcer.Op.PRODUCE, 100).allow()).isTrue();
    }

    @Test
    void produceAndFetchBucketsAreIndependent() {
        var enforcer = new InMemoryQuotaEnforcer(100, 100);
        enforcer.check("a", QuotaEnforcer.Op.PRODUCE, 100);
        assertThat(enforcer.check("a", QuotaEnforcer.Op.FETCH, 100).allow()).isTrue();
    }

    @Test
    void refillRestoresBurstCapacity() throws Exception {
        var enforcer = new InMemoryQuotaEnforcer(1_000_000, 1_000_000);
        enforcer.check("a", QuotaEnforcer.Op.PRODUCE, 1_000_000);
        // Just-above-empty: a tiny wait refills proportionally.
        Thread.sleep(20);
        var d = enforcer.check("a", QuotaEnforcer.Op.PRODUCE, 10_000);
        assertThat(d.allow()).isTrue();
    }

    @Test
    void denyDecisionExposesRateAndThrottle() {
        var enforcer = new InMemoryQuotaEnforcer(10, 10);
        enforcer.check("a", QuotaEnforcer.Op.PRODUCE, 10);
        var denied = enforcer.check("a", QuotaEnforcer.Op.PRODUCE, 5);
        assertThat(denied.allow()).isFalse();
        assertThat(denied.quotaBytesPerSec()).isEqualTo(10);
        // 5 bytes at 10 bytes/sec ≈ 500ms.
        assertThat(denied.throttleMillis()).isBetween(100L, 1000L);
    }
}
