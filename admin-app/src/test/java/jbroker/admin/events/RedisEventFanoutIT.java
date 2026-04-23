package jbroker.admin.events;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * end-to-end: two {@link RedisEventFanout} instances (standing in
 * for two admin pods) pointed at the same Redis container should propagate
 * events from pod A's local bus into pod B's local bus, without pod A
 * seeing a duplicate of its own publish on the way back in.
 *
 * <p>Tagged {@code slow}: Testcontainers pulls + boots Docker, so the
 * default CI run excludes it. Opt into with {@code JBROKER_RUN_SLOW_TESTS=1}.
 */
@Testcontainers
@Tag("slow")
class RedisEventFanoutIT {

    @Container
    static final GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @Test
    @Timeout(30)
    void eventPublishedByPodAReachesPodBViaRedisButNotItself() throws Exception {
        String url = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);

        var busA = new AdminEventBus(null);
        var busB = new AdminEventBus(null);

        // Track what each bus delivers to SSE subscribers.
        var receivedByA = new ConcurrentLinkedQueue<AdminEventBus.LocalEvent>();
        var receivedByB = new ConcurrentLinkedQueue<AdminEventBus.LocalEvent>();
        busA.subscribe("a-sub", receivedByA::add);
        busB.subscribe("b-sub", receivedByB::add);

        var fanoutA = new RedisEventFanout(busA, url);
        var fanoutB = new RedisEventFanout(busB, url);

        // Start lifecycle manually — Spring isn't bootstrapping in this IT.
        ReflectionTestUtils.invokeMethod(fanoutA, "start");
        ReflectionTestUtils.invokeMethod(fanoutB, "start");

        try {
            // Give both SUBSCRIBE sockets a moment to settle before the first
            // PUBLISH. Without this there's a slim race where fanoutA
            // publishes before fanoutB has SUBSCRIBEd and the message is
            // dropped — Redis pub/sub has no persistence.
            waitForSubscribeReady(fanoutA);
            waitForSubscribeReady(fanoutB);

            // Simulate broker X emitting event 7 — pod A ingests it directly.
            ReflectionTestUtils.invokeMethod(
                    busA,
                    "ingest",
                    "broker-1:9092",
                    jbroker.proto.broker.EventMessage.newBuilder()
                            .setId(7L)
                            .setType("leader_changed")
                            .setDataJson("{\"topic\":\"t\",\"partition\":0}")
                            .build());

            // Pod A saw it locally (the immediate broadcast path).
            assertThat(pollForSize(receivedByA, 1, Duration.ofSeconds(5))).isTrue();

            // Pod B should see it via Redis fan-out.
            assertThat(pollForSize(receivedByB, 1, Duration.ofSeconds(5))).isTrue();

            var aOnly = new java.util.ArrayList<>(receivedByA);
            var bOnly = new java.util.ArrayList<>(receivedByB);

            assertThat(aOnly).hasSize(1);
            assertThat(bOnly).hasSize(1);
            assertThat(aOnly.get(0).brokerEventId()).isEqualTo(7L);
            assertThat(bOnly.get(0).brokerEventId()).isEqualTo(7L);

            // Pod A MUST NOT re-broadcast its own echo — a second duplicate
            // would show up in receivedByA within ~1s if the self-filter
            // were broken.
            Thread.sleep(1_000);
            assertThat(receivedByA).hasSize(1);
        } finally {
            ReflectionTestUtils.invokeMethod(fanoutA, "stop");
            ReflectionTestUtils.invokeMethod(fanoutB, "stop");
        }
    }

    @Test
    @Timeout(30)
    void samePodIngestingSameBrokerEventTwiceBroadcastsOnce() throws Exception {
        String url = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);

        var bus = new AdminEventBus(null);
        var deliveries = new AtomicInteger(0);
        bus.subscribe("sub", e -> deliveries.incrementAndGet());

        var fanout = new RedisEventFanout(bus, url);
        ReflectionTestUtils.invokeMethod(fanout, "start");
        try {
            waitForSubscribeReady(fanout);

            // Drive two ingests of the same (endpoint, brokerEventId) pair.
            // Only the first should broadcast.
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
            Thread.sleep(500);
            assertThat(deliveries.get())
                    .as("dedupe on (endpoint, brokerEventId) prevents double broadcast")
                    .isEqualTo(1);
        } finally {
            ReflectionTestUtils.invokeMethod(fanout, "stop");
        }
    }

    private static boolean pollForSize(java.util.Collection<?> q, int target, Duration budget)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + budget.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (q.size() >= target) return true;
            Thread.sleep(25);
        }
        return q.size() >= target;
    }

    /**
     * Redis pub/sub doesn't persist messages, so this test must ensure both
     * fanouts have completed their initial SUBSCRIBE handshake before any
     * PUBLISH. We poll until the subscriber thread has named itself
     * "redis-event-subscriber" AND the socket field is non-null — the
     * socket is assigned only after connect + SUBSCRIBE reply succeed.
     */
    private static void waitForSubscribeReady(RedisEventFanout fanout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            var sock = (java.net.Socket) ReflectionTestUtils.getField(fanout, "subscriberSocket");
            if (sock != null && !sock.isClosed()) {
                // Give Redis a moment to register us in its subscribers map
                // — SUBSCRIBE is acknowledged before the server-side state
                // actually allows PUBLISH deliveries through.
                Thread.sleep(100);
                return;
            }
            Thread.sleep(25);
        }
        throw new AssertionError("fanout subscriber didn't open a socket within 5s");
    }
}
