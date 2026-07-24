package jbroker.admin.events;

import static org.assertj.core.api.Assertions.assertThat;

import jbroker.admin.client.BrokerAdminClientPool;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Bean selection for the SSE fan-out. No Redis URL — the default — must
 * yield the in-process implementation, and a configured URL must swap in
 * the Redis bridge. Neither case may require a reachable Redis (or
 * broker) to refresh the context: the Redis fan-out tolerates connection
 * failures by design, and gRPC channels connect lazily.
 */
class EventFanoutConfigTest {

    private ApplicationContextRunner runner() {
        // Port 1 is never listening; the pool builds lazily-connecting
        // channels, so context refresh succeeds without a live broker.
        var pool = new BrokerAdminClientPool("127.0.0.1:1", false, "", "", "");
        return new ApplicationContextRunner()
                .withBean(AdminEventBus.class, () -> new AdminEventBus(pool))
                .withUserConfiguration(EventFanoutConfig.class);
    }

    @Test
    void noRedisUrlSelectsInProcessFanout() {
        runner().run(context ->
                assertThat(context.getBean(EventFanout.class)).isInstanceOf(InProcessEventFanout.class));
    }

    @Test
    void blankRedisUrlSelectsInProcessFanout() {
        // Every @SpringBootTest IT pins "jbroker.redis.url=" — the blank
        // form must behave exactly like the property being absent.
        runner().withPropertyValues("jbroker.redis.url=").run(context -> assertThat(context.getBean(EventFanout.class))
                .isInstanceOf(InProcessEventFanout.class));
    }

    @Test
    void redisUrlSelectsRedisFanoutWithoutNeedingAReachableRedis() {
        runner().withPropertyValues("jbroker.redis.url=redis://127.0.0.1:1")
                .run(context -> assertThat(context.getBean(EventFanout.class)).isInstanceOf(RedisEventFanout.class));
    }
}
