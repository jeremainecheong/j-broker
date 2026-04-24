package jbroker.broker.quota;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Testcontainers-backed IT proving the RESP protocol implementation
 * actually talks to Redis and that two enforcer instances share the same
 * counter. Opt into with {@code JBROKER_RUN_SLOW_TESTS=1} so default unit
 * runs don't pull the Redis image.
 */
@Testcontainers
@Tag("slow")
final class RedisQuotaEnforcerIT {

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    private static String redisUrl() {
        return "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
    }

    @Test
    void incrByRoundTripsAtomically() throws Exception {
        var enforcer = new RedisQuotaEnforcer(redisUrl(), Long.MAX_VALUE, Long.MAX_VALUE);
        try {
            long a = enforcer.incrByWithExpire("jbroker:test:atomic", 100);
            long b = enforcer.incrByWithExpire("jbroker:test:atomic", 50);
            long c = enforcer.incrByWithExpire("jbroker:test:atomic", 25);
            assertThat(a).isEqualTo(100L);
            assertThat(b).isEqualTo(150L);
            assertThat(c).isEqualTo(175L);
        } finally {
            enforcer.close();
        }
    }

    @Test
    void twoEnforcersShareCluster() throws Exception {
        var e1 = new RedisQuotaEnforcer(redisUrl(), Long.MAX_VALUE, Long.MAX_VALUE);
        var e2 = new RedisQuotaEnforcer(redisUrl(), Long.MAX_VALUE, Long.MAX_VALUE);
        try {
            // Simulates two brokers hitting the same Redis: each INCRBY
            // contributes to the same shared counter.
            assertThat(e1.incrByWithExpire("jbroker:test:shared", 10)).isEqualTo(10L);
            assertThat(e2.incrByWithExpire("jbroker:test:shared", 25)).isEqualTo(35L);
            assertThat(e1.incrByWithExpire("jbroker:test:shared", 5)).isEqualTo(40L);
        } finally {
            e1.close();
            e2.close();
        }
    }

    @Test
    void denyTriggersWhenCounterExceedsRate() throws Exception {
        var enforcer = new RedisQuotaEnforcer(redisUrl(), /*produceRate*/ 1_000, Long.MAX_VALUE);
        try {
            assertThat(enforcer.check("alice", QuotaEnforcer.Op.PRODUCE, 600).allow())
                    .isTrue();
            // 600 + 500 = 1100 > 1000 → deny.
            var denied = enforcer.check("alice", QuotaEnforcer.Op.PRODUCE, 500);
            assertThat(denied.allow()).isFalse();
            assertThat(denied.quotaBytesPerSec()).isEqualTo(1_000L);
            assertThat(denied.throttleMillis()).isGreaterThanOrEqualTo(100L);
        } finally {
            enforcer.close();
        }
    }

    @Test
    void concurrentIncrementsAllAtomicNoLostUpdates() throws Exception {
        var enforcer = new RedisQuotaEnforcer(redisUrl(), Long.MAX_VALUE, Long.MAX_VALUE);
        try {
            int threads = 8;
            int incrsPerThread = 100;
            var starter = new CountDownLatch(1);
            var done = new CountDownLatch(threads);
            var errors = new AtomicInteger(0);
            for (int t = 0; t < threads; t++) {
                Thread.ofVirtual().start(() -> {
                    try {
                        starter.await();
                        for (int i = 0; i < incrsPerThread; i++) {
                            enforcer.incrByWithExpire("jbroker:test:concurrent", 1);
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                });
            }
            starter.countDown();
            done.await();
            assertThat(errors.get()).isEqualTo(0);
            long finalCount = enforcer.incrByWithExpire("jbroker:test:concurrent", 0);
            assertThat(finalCount).isEqualTo(threads * incrsPerThread);
        } finally {
            enforcer.close();
        }
    }
}
