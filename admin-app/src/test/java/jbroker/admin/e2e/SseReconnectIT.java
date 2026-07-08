package jbroker.admin.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import jbroker.admin.AdminApp;
import jbroker.app.Broker;
import jbroker.app.testkit.BindRetry;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Connect an SSE client, receive events, disconnect,
 * produce more events, reconnect with {@code Last-Event-ID}, assert no
 * events were dropped across the gap.
 */
@SpringBootTest(
        classes = AdminApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "jbroker.redis.url=")
class SseReconnectIT {

    private static Broker broker;
    private static Path dataDir;
    private static int brokerPort;

    @LocalServerPort
    int port;

    @BeforeAll
    static void startBroker() throws Exception {
        dataDir = Files.createTempDirectory("e2e-8-6-sse");
        broker = BindRetry.startWithBindRetry(() ->
                Broker.start(new Broker.Config(new NodeId(1), dataDir, BindRetry.freePort(), BindRetry.freePort())));
        brokerPort = broker.brokerPort();
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline && broker.role() != Role.LEADER) {
            Thread.sleep(25);
        }
    }

    @AfterAll
    static void stopBroker() {
        if (broker != null) broker.close();
        if (dataDir != null) deleteQuietly(dataDir);
    }

    @DynamicPropertySource
    static void brokers(DynamicPropertyRegistry registry) {
        registry.add("jbroker.admin.brokers", () -> "localhost:" + brokerPort);
    }

    @Test
    void reconnectWithLastEventIdReplaysMissedEvents() throws Exception {
        // Step 1: first connection — create a topic + record the event id.
        var firstIds = consume(1, null);
        try (var admin = new BrokerClient("localhost", brokerPort)) {
            admin.createTopic("before-disconnect", 1, 1);
        }
        long lastSeen = waitForId(firstIds, 3_000);
        assertThat(lastSeen)
                .as("first connection should yield at least one event id")
                .isGreaterThan(0);

        // Step 2: first connection dropped, trigger more events while no
        // one is listening.
        try (var admin = new BrokerClient("localhost", brokerPort)) {
            admin.createTopic("during-disconnect", 1, 1);
        }
        // Allow the broker's event stream to land in the admin-app's ring.
        Thread.sleep(500);

        // Step 3: reconnect with Last-Event-ID = lastSeen, expect the
        // "during-disconnect" event to be replayed.
        var secondIds = consume(1, Long.toString(lastSeen));
        long replayed = waitForId(secondIds, 3_000);
        assertThat(replayed)
                .as("reconnect should replay events with id > %d", lastSeen)
                .isGreaterThan(lastSeen);
    }

    /**
     * Opens an SSE connection that collects up to {@code stopAfter} {@code id:}
     * lines into the returned queue before closing. If {@code lastEventId} is
     * non-null, sends it as the {@code Last-Event-ID} header.
     */
    private LinkedBlockingQueue<Long> consume(int stopAfter, String lastEventId) throws InterruptedException {
        var ids = new LinkedBlockingQueue<Long>();
        var closed = new AtomicBoolean(false);
        var builder = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/v1/events"))
                .header("Accept", "text/event-stream");
        if (lastEventId != null) builder.header("Last-Event-ID", lastEventId);
        var req = builder.GET().build();

        Thread.ofVirtual().start(() -> {
            try {
                var resp = HttpClient.newBuilder().build().send(req, HttpResponse.BodyHandlers.ofLines());
                var received = new ArrayList<Long>();
                var iter = resp.body().iterator();
                while (!closed.get() && iter.hasNext()) {
                    String line = iter.next();
                    if (line.startsWith("id:")) {
                        long id = Long.parseLong(line.substring(3).trim());
                        ids.offer(id);
                        received.add(id);
                        if (received.size() >= stopAfter) {
                            closed.set(true);
                            break;
                        }
                    }
                }
            } catch (IOException | InterruptedException e) {
                // Expected on close.
            }
        });
        return ids;
    }

    private long waitForId(LinkedBlockingQueue<Long> ids, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long latest = 0;
        while (System.currentTimeMillis() < deadline) {
            Long id = ids.poll(100, TimeUnit.MILLISECONDS);
            if (id != null) {
                latest = Math.max(latest, id);
                // Drain anything else that landed in the same burst.
                while (true) {
                    Long extra = ids.poll();
                    if (extra == null) break;
                    latest = Math.max(latest, extra);
                }
                return latest;
            }
        }
        return latest;
    }

    private static void deleteQuietly(Path root) {
        try (var s = Files.walk(root)) {
            s.sorted((a, b) -> b.getNameCount() - a.getNameCount()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best-effort
                }
            });
        } catch (IOException ignored) {
            // best-effort
        }
    }

    @SuppressWarnings("unused")
    private record Unused(Duration d, List<Long> l) {}
}
