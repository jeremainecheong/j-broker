package jbroker.admin.client;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Client for broker-hosted {@code /debug/chaos/*} endpoints. Maps
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
    private final String bearerToken;
    private final HttpClient http =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    public BrokerChaosClient(String mapping) {
        this(mapping, "");
    }

    /** Token-carrying form: brokers with a chaos auth token demand it on every request. */
    public BrokerChaosClient(String mapping, String bearerToken) {
        this.brokerUrls = parseMapping(mapping);
        this.bearerToken = bearerToken == null ? "" : bearerToken;
    }

    /** Visible for tests that want to wire chaos URLs dynamically (random ports). */
    public BrokerChaosClient(Map<Integer, String> brokerUrls) {
        this.brokerUrls = Map.copyOf(brokerUrls);
        this.bearerToken = "";
    }

    private HttpRequest.Builder request(String uri) {
        var b = HttpRequest.newBuilder().uri(URI.create(uri));
        if (!bearerToken.isBlank()) b.header("Authorization", "Bearer " + bearerToken);
        return b;
    }

    public String post(int brokerId, String path, String query) throws Exception {
        String base = brokerUrls.get(brokerId);
        if (base == null) throw new BrokerUnknown("no chaos URL configured for broker " + brokerId);
        String uri = base + path + (query == null || query.isEmpty() ? "" : "?" + query);
        HttpRequest req = request(uri)
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

    /**
     * Read-only chaos state for {@code brokerId}. Returns the raw JSON body
     * from {@code GET /debug/chaos/state}. Throws {@link ChaosDisabled} for
     * unreachable/404 brokers so the admin can render a neutral
     * "chaos-off" state without failing the whole topology poll.
     */
    public String getState(int brokerId) throws Exception {
        String base = brokerUrls.get(brokerId);
        if (base == null) throw new BrokerUnknown("no chaos URL configured for broker " + brokerId);
        HttpRequest req = request(base + "/debug/chaos/state")
                .timeout(Duration.ofSeconds(2))
                .GET()
                .build();
        try {
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 404) throw new ChaosDisabled("broker " + brokerId + " returned 404 for state");
            if (resp.statusCode() >= 400)
                throw new RuntimeException(
                        "broker " + brokerId + " returned " + resp.statusCode() + ": " + resp.body());
            return resp.body();
        } catch (java.net.ConnectException e) {
            throw new ChaosDisabled("broker " + brokerId + " unreachable: " + e.getMessage());
        }
    }

    public java.util.Set<Integer> knownBrokerIds() {
        return java.util.Set.copyOf(brokerUrls.keySet());
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
