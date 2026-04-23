package jbroker.broker.chaos;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import jbroker.broker.events.BrokerEvent;
import jbroker.broker.events.BrokerEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * P9.3 — broker-side chaos control plane. Exposes seven cooperative
 * endpoints under {@code /debug/chaos/*} gated by
 * {@code jbroker.chaos.enabled=true}. The broker faults <em>itself</em>
 * in response; admin-app's proxy picks the right broker and targets
 * it directly.
 *
 * <p>Deliberately uses the JDK's built-in {@code HttpServer} so we don't
 * add Spring to the broker process. Requests are single-shot — no
 * long-polling, no streaming — so virtual threads per request are
 * fine.
 */
public final class ChaosHttpServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ChaosHttpServer.class);

    private final ChaosState state;
    private final int selfBrokerId;
    private final BrokerEventPublisher eventPublisher;
    private final Runnable forceElection;
    private final Runnable kill;
    private final HttpServer http;

    public ChaosHttpServer(
            ChaosState state,
            int selfBrokerId,
            int port,
            BrokerEventPublisher eventPublisher,
            Runnable forceElection,
            Runnable kill)
            throws IOException {
        this.state = state;
        this.selfBrokerId = selfBrokerId;
        this.eventPublisher = eventPublisher;
        this.forceElection = forceElection;
        this.kill = kill;
        this.http = HttpServer.create(new InetSocketAddress(port), 0);
        http.createContext("/debug/chaos/kill", wrap(this::handleKill));
        http.createContext("/debug/chaos/pause", wrap(this::handlePause));
        http.createContext("/debug/chaos/resume", wrap(this::handleResume));
        http.createContext("/debug/chaos/partition", wrap(this::handlePartition));
        http.createContext("/debug/chaos/heal-partition", wrap(this::handleHealPartition));
        http.createContext("/debug/chaos/inject-latency", wrap(this::handleInjectLatency));
        http.createContext("/debug/chaos/force-election", wrap(this::handleForceElection));
        http.setExecutor(Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("chaos-http-", 0).factory()));
        http.start();
    }

    public int port() {
        return http.getAddress().getPort();
    }

    @Override
    public void close() {
        http.stop(0);
    }

    private HttpHandler wrap(HttpHandler inner) {
        return ex -> {
            try {
                inner.handle(ex);
            } catch (Exception e) {
                log.warn("chaos handler failed: {}", ex.getRequestURI(), e);
                respond(ex, 500, "{\"error\":\"" + escape(e.toString()) + "\"}");
            }
        };
    }

    private void handleKill(HttpExchange ex) throws IOException {
        if (!"POST".equals(ex.getRequestMethod())) {
            respond(ex, 405, "{\"error\":\"POST only\"}");
            return;
        }
        respond(ex, 200, "{\"action\":\"kill\",\"brokerId\":" + selfBrokerId + "}");
        publish("kill", null, null);
        // Give the response time to flush before exiting.
        Thread.ofPlatform().start(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            kill.run();
        });
    }

    private void handlePause(HttpExchange ex) throws IOException {
        if (!"POST".equals(ex.getRequestMethod())) {
            respond(ex, 405, "{\"error\":\"POST only\"}");
            return;
        }
        state.pause();
        publish("pause", null, null);
        respond(ex, 200, "{\"action\":\"pause\",\"brokerId\":" + selfBrokerId + "}");
    }

    private void handleResume(HttpExchange ex) throws IOException {
        if (!"POST".equals(ex.getRequestMethod())) {
            respond(ex, 405, "{\"error\":\"POST only\"}");
            return;
        }
        state.resume();
        publish("resume", null, null);
        respond(ex, 200, "{\"action\":\"resume\",\"brokerId\":" + selfBrokerId + "}");
    }

    private void handlePartition(HttpExchange ex) throws IOException {
        if (!"POST".equals(ex.getRequestMethod())) {
            respond(ex, 405, "{\"error\":\"POST only\"}");
            return;
        }
        Map<String, String> q = parseQuery(ex.getRequestURI().getRawQuery());
        var peer = parseIntOrNull(q.get("peer"));
        if (peer == null) {
            respond(ex, 400, "{\"error\":\"peer query param required\"}");
            return;
        }
        state.blockPeer(peer);
        publish("partition", peer, null);
        respond(ex, 200, "{\"action\":\"partition\",\"brokerId\":" + selfBrokerId + ",\"peer\":" + peer + "}");
    }

    private void handleHealPartition(HttpExchange ex) throws IOException {
        if (!"POST".equals(ex.getRequestMethod())) {
            respond(ex, 405, "{\"error\":\"POST only\"}");
            return;
        }
        Map<String, String> q = parseQuery(ex.getRequestURI().getRawQuery());
        var peer = parseIntOrNull(q.get("peer"));
        if (peer == null) {
            state.clearBlockedPeers();
            publish("heal-all", null, null);
            respond(ex, 200, "{\"action\":\"heal-all\",\"brokerId\":" + selfBrokerId + "}");
            return;
        }
        state.unblockPeer(peer);
        publish("heal-partition", peer, null);
        respond(ex, 200, "{\"action\":\"heal-partition\",\"brokerId\":" + selfBrokerId + ",\"peer\":" + peer + "}");
    }

    private void handleInjectLatency(HttpExchange ex) throws IOException {
        if (!"POST".equals(ex.getRequestMethod())) {
            respond(ex, 405, "{\"error\":\"POST only\"}");
            return;
        }
        Map<String, String> q = parseQuery(ex.getRequestURI().getRawQuery());
        var ms = parseLongOrNull(q.get("ms"));
        if (ms == null) {
            respond(ex, 400, "{\"error\":\"ms query param required\"}");
            return;
        }
        state.setLatencyMs(ms);
        publish("inject-latency", null, ms);
        respond(ex, 200, "{\"action\":\"inject-latency\",\"brokerId\":" + selfBrokerId + ",\"millis\":" + ms + "}");
    }

    private void handleForceElection(HttpExchange ex) throws IOException {
        if (!"POST".equals(ex.getRequestMethod())) {
            respond(ex, 405, "{\"error\":\"POST only\"}");
            return;
        }
        forceElection.run();
        publish("force-election", null, null);
        respond(ex, 200, "{\"action\":\"force-election\",\"brokerId\":" + selfBrokerId + "}");
    }

    private void publish(String action, Integer peerId, Long millis) {
        if (eventPublisher == null) return;
        long id = eventPublisher.allocateId();
        eventPublisher.publish(new BrokerEvent.ChaosAction(id, action, selfBrokerId, peerId, millis));
    }

    private static Map<String, String> parseQuery(String raw) {
        var out = new HashMap<String, String>();
        if (raw == null || raw.isEmpty()) return out;
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            if (eq < 0) out.put(pair, "");
            else out.put(pair.substring(0, eq), pair.substring(eq + 1));
        }
        return out;
    }

    private static Integer parseIntOrNull(String s) {
        if (s == null || s.isBlank()) return null;
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Long parseLongOrNull(String s) {
        if (s == null || s.isBlank()) return null;
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static void respond(HttpExchange ex, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        try (var out = ex.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static String escape(String s) {
        return s == null ? "" : s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
