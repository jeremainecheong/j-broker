package jbroker.admin.events;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Chooses the {@link EventFanout} implementation from config.
 *
 * <p>{@code jbroker.redis.url} unset or blank — the default — selects
 * {@link InProcessEventFanout}: startup needs no reachable Redis and the
 * SSE stream is served entirely from this JVM's {@link AdminEventBus}.
 * A non-blank URL selects {@link RedisEventFanout}, which bridges events
 * across admin replicas — required whenever more than one admin replica
 * serves browsers, since SSE subscribers only ever hear their own
 * replica's bus.
 */
@Configuration(proxyBeanMethods = false)
public class EventFanoutConfig {

    @Bean
    EventFanout eventFanout(AdminEventBus bus, @Value("${jbroker.redis.url:}") String redisUrl) {
        if (redisUrl == null || redisUrl.isBlank()) {
            return new InProcessEventFanout();
        }
        return new RedisEventFanout(bus, redisUrl);
    }
}
