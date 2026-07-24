package jbroker.admin.events;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default fan-out: everything stays in this JVM. The {@link AdminEventBus}
 * broadcast that precedes {@link #publish} already delivered the event to
 * every SSE subscriber connected to this instance, and with one admin
 * replica that is every subscriber there is — so publishing is a no-op and
 * startup needs no reachable Redis.
 *
 * <p>Correct for exactly one admin replica. Running several replicas
 * behind a load balancer needs {@link RedisEventFanout} instead (set
 * {@code jbroker.redis.url}); otherwise each browser only sees events
 * ingested by the replica its SSE connection happens to be pinned to.
 */
public final class InProcessEventFanout implements EventFanout {

    private static final Logger log = LoggerFactory.getLogger(InProcessEventFanout.class);

    @PostConstruct
    void start() {
        log.info("in-process SSE event fan-out active (single admin replica); "
                + "set jbroker.redis.url to fan out across replicas");
    }

    @Override
    public void publish(AdminEventBus.LocalEvent event) {
        // The bus already broadcast to this instance's subscribers; there
        // are no peer replicas to notify.
    }
}
