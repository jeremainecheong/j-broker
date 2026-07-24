package jbroker.admin.events;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * The default (no-Redis) SSE path: a single admin instance ingesting
 * broker events delivers each one to its local subscribers exactly once,
 * with no Redis reachable anywhere. Mirrors the single-pod assertions of
 * {@link RedisEventFanoutIT} so both fan-out modes stay pinned to the
 * same browser-visible semantics.
 */
class InProcessEventFanoutTest {

    @Test
    void ingestedEventReachesLocalSubscriber() {
        var bus = new AdminEventBus(null);
        var received = new ConcurrentLinkedQueue<AdminEventBus.LocalEvent>();
        bus.subscribe("sse-sub", received::add);

        ReflectionTestUtils.invokeMethod(
                bus,
                "ingest",
                "broker-1:9092",
                jbroker.proto.broker.EventMessage.newBuilder()
                        .setId(7L)
                        .setType("leader_changed")
                        .setDataJson("{\"topic\":\"t\",\"partition\":0}")
                        .build());

        assertThat(received).hasSize(1);
        var e = received.peek();
        assertThat(e.brokerEndpoint()).isEqualTo("broker-1:9092");
        assertThat(e.type()).isEqualTo("leader_changed");
        assertThat(e.dataJson()).isEqualTo("{\"topic\":\"t\",\"partition\":0}");
        assertThat(e.brokerEventId()).isEqualTo(7L);
    }

    @Test
    void sameBrokerEventIngestedTwiceBroadcastsOnce() {
        var bus = new AdminEventBus(null);
        var deliveries = new AtomicInteger(0);
        bus.subscribe("sub", e -> deliveries.incrementAndGet());

        for (int i = 0; i < 2; i++) {
            ReflectionTestUtils.invokeMethod(
                    bus,
                    "ingest",
                    "broker-1:9092",
                    jbroker.proto.broker.EventMessage.newBuilder()
                            .setId(1L)
                            .setType("leader_changed")
                            .setDataJson("{\"a\":1}")
                            .build());
        }

        assertThat(deliveries.get())
                .as("dedupe on (endpoint, brokerEventId) prevents double broadcast")
                .isEqualTo(1);
    }

    @Test
    void publishIsANoOpBeyondTheLocalBroadcast() {
        var bus = new AdminEventBus(null);
        var deliveries = new AtomicInteger(0);
        var last = new java.util.concurrent.atomic.AtomicReference<AdminEventBus.LocalEvent>();
        bus.subscribe("sub", e -> {
            deliveries.incrementAndGet();
            last.set(e);
        });

        ReflectionTestUtils.invokeMethod(
                bus,
                "ingest",
                "broker-1:9092",
                jbroker.proto.broker.EventMessage.newBuilder()
                        .setId(3L)
                        .setType("topic_created")
                        .setDataJson("{\"name\":\"t\"}")
                        .build());
        assertThat(deliveries.get()).isEqualTo(1);

        // The interface hook must not re-broadcast: the bus already
        // delivered to every subscriber this instance has.
        new InProcessEventFanout().publish(last.get());
        assertThat(deliveries.get()).isEqualTo(1);
    }
}
