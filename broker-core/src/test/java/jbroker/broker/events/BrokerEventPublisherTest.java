package jbroker.broker.events;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class BrokerEventPublisherTest {

    @Test
    void idsAreMonotonic() {
        var p = new BrokerEventPublisher();
        long a = p.allocateId();
        long b = p.allocateId();
        long c = p.allocateId();
        assertThat(b).isGreaterThan(a);
        assertThat(c).isGreaterThan(b);
    }

    @Test
    void replayAfterReturnsOnlyNewerEvents() {
        var p = new BrokerEventPublisher(16);
        for (int i = 0; i < 5; i++) {
            long id = p.allocateId();
            p.publish(new BrokerEvent.BrokerFenced(id, 100 + i));
        }
        var tail = p.replayAfter(2L);
        assertThat(tail).hasSize(3);
        assertThat(tail.get(0).id()).isEqualTo(3L);
        assertThat(tail.get(tail.size() - 1).id()).isEqualTo(5L);
    }

    @Test
    void subscribersReceivePublishedEvents() throws Exception {
        var p = new BrokerEventPublisher();
        try (var sub = p.subscribe()) {
            long id = p.allocateId();
            p.publish(new BrokerEvent.BrokerRegistered(id, 7, "host-7", 9092));
            var received = sub.take();
            assertThat(received).isInstanceOf(BrokerEvent.BrokerRegistered.class);
            assertThat(received.id()).isEqualTo(id);
        }
    }

    @Test
    void closedSubscriptionStopsReceiving() throws Exception {
        var p = new BrokerEventPublisher();
        var sub = p.subscribe();
        sub.close();
        long id = p.allocateId();
        p.publish(new BrokerEvent.BrokerFenced(id, 1));
        // The closed subscription returns null on take() instead of blocking.
        var out = sub.take();
        assertThat(out).isNull();
    }

    @Test
    void ringBufferWrapsWithoutLosingRecentIds() {
        var p = new BrokerEventPublisher(4);
        for (int i = 0; i < 10; i++) {
            long id = p.allocateId();
            p.publish(new BrokerEvent.BrokerFenced(id, i));
        }
        // Ring capacity is 4, so we keep the last 4 events (ids 7..10).
        var tail = p.replayAfter(0);
        assertThat(tail).hasSize(4);
        assertThat(tail.get(0).id()).isEqualTo(7L);
        assertThat(tail.get(3).id()).isEqualTo(10L);
    }

    @Test
    void publishIsNonBlockingEvenWithSlowSubscriber() throws Exception {
        var p = new BrokerEventPublisher();
        var sub = p.subscribe();
        var delivered = new AtomicInteger();
        var pump = Thread.ofVirtual().start(() -> {
            try {
                while (true) {
                    var e = sub.take();
                    if (e == null) break;
                    delivered.incrementAndGet();
                }
            } catch (InterruptedException ignored) {
                // stop
            }
        });
        try {
            long start = System.nanoTime();
            for (int i = 0; i < 1000; i++) {
                long id = p.allocateId();
                p.publish(new BrokerEvent.BrokerFenced(id, i));
            }
            long elapsed = Duration.ofNanos(System.nanoTime() - start).toMillis();
            assertThat(elapsed)
                    .as("publishing 1000 events should be sub-second")
                    .isLessThan(1_000L);
        } finally {
            sub.close();
            pump.join(Duration.ofSeconds(2));
        }
    }
}
