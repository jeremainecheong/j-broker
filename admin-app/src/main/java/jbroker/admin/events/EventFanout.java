package jbroker.admin.events;

/**
 * Propagation seam for events the {@link AdminEventBus} ingests. The bus
 * broadcasts every event to this instance's SSE subscribers <em>before</em>
 * the fan-out sees it; implementations decide what happens beyond this
 * instance:
 *
 * <ul>
 *   <li>{@link InProcessEventFanout} (default) — nothing. With a single
 *       admin replica the local broadcast already reached every
 *       subscriber, and no Redis connection is ever attempted.</li>
 *   <li>{@link RedisEventFanout} — PUBLISHes to a shared Redis channel so
 *       peer admin replicas can deliver the event to <em>their</em> SSE
 *       subscribers too.</li>
 * </ul>
 *
 * <p>Selection happens in {@link EventFanoutConfig}, keyed on
 * {@code jbroker.redis.url}.
 */
public interface EventFanout {

    /** Propagate an event this instance has already broadcast locally. */
    void publish(AdminEventBus.LocalEvent event);
}
