package jbroker.admin.client;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * client for broker-hosted {@code /debug/chaos/*} endpoints. Maps
 * broker id → chaos URL from the {@code jbroker.admin.chaos.brokers}
 * property (comma-separated {@code id=host:port} pairs).
 *
 * <p>Deliberately uses the JDK {@link HttpClient} — we already have it
 * for the CLI, it virtual-thread-friendly, and no extra dependency.
 */
public final class BrokerChaosClient {

    public static final class BrokerUnknown extends RuntimeException {
        BrokerUnknown(String m) {
            super(m);
        }
    }

    public static final class ChaosDisabled extends RuntimeException {
        ChaosDisabled(String m) {
            super(m);
        }
    }

    private final Map<Integer, String> brokerUrls;
    private final HttpClient http =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    public BrokerChaosClient(String mapping) {
        this.brokerUrls = parseMapping(mapping);
    }

    /** Visible for tests that want to wire chaos URLs dynamically (random ports). */
    public BrokerChaosClient(Map<Integer, String> brokerUrls) {
        this.brokerUrls = Map.copyOf(brokerUrls);
    }

    public String post(int brokerId, String path, String query) throws Exception {
        String base = brokerUrls.get(brokerId);
        if (base == null) throw new BrokerUnknown("no chaos URL configured for broker " + brokerId);
        String uri = base + path + (query == null || query.isEmpty() ? "" : "?" + query);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(uri))
                .timeout(Duration.ofSeconds(3))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        try {
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 404) throw new ChaosDisabled("broker " + brokerId + " returned 404 for " + path);
            if (resp.statusCode() >= 400)
                throw new RuntimeException(
                        "broker " + brokerId + " returned " + resp.statusCode() + ": " + resp.body());
            return resp.body();
        } catch (java.net.ConnectException e) {
            throw new ChaosDisabled("broker " + brokerId + " unreachable: " + e.getMessage());
        }
    }

    private static Map<Integer, String> parseMapping(String raw) {
        var out = new HashMap<Integer, String>();
        if (raw == null || raw.isBlank()) return out;
        for (String pair : raw.split(",")) {
            String p = pair.trim();
            if (p.isEmpty()) continue;
            int eq = p.indexOf('=');
            if (eq <= 0 || eq == p.length() - 1) continue;
            try {
                int id = Integer.parseInt(p.substring(0, eq).trim());
                String hostPort = p.substring(eq + 1).trim();
                out.put(id, "http://" + hostPort);
            } catch (NumberFormatException ignored) {
                // skip malformed entries
            }
        }
        return out;
    }
}
