package jbroker.admin.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import jbroker.admin.client.BrokerChaosClient;
import jbroker.admin.dto.ChaosStateSnapshot;
import jbroker.admin.dto.RestError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * admin-side proxy for the cooperative chaos endpoints every
 * broker hosts on its {@code jbroker.chaos.port}. The admin-app resolves
 * the target broker's chaos port from the static property
 * {@code jbroker.admin.chaos.brokers} (comma-separated {@code id=host:port})
 * and forwards the HTTP POST directly — no gRPC involved since chaos
 * endpoints are dev-only HTTP on the broker.
 */
@RestController
@RequestMapping("/api/v1/chaos")
public class ChaosController {

    private static final Logger log = LoggerFactory.getLogger(ChaosController.class);

    private final BrokerChaosClient client;

    public ChaosController(@Value("${jbroker.admin.chaos.brokers:}") String mapping) {
        this.client = new BrokerChaosClient(mapping);
    }

    @PostMapping("/kill-broker/{id}")
    public ResponseEntity<?> kill(@PathVariable int id) {
        return proxy(id, "/debug/chaos/kill", "");
    }

    @PostMapping("/pause-broker/{id}")
    public ResponseEntity<?> pause(@PathVariable int id) {
        return proxy(id, "/debug/chaos/pause", "");
    }

    @PostMapping("/resume-broker/{id}")
    public ResponseEntity<?> resume(@PathVariable int id) {
        return proxy(id, "/debug/chaos/resume", "");
    }

    /** Partition: body {@code {"from": id1, "to": id2}} fans out to both brokers. */
    @PostMapping("/partition")
    public ResponseEntity<?> partition(@RequestBody PartitionBody body) {
        var r1 = proxy(body.from(), "/debug/chaos/partition", "peer=" + body.to());
        var r2 = proxy(body.to(), "/debug/chaos/partition", "peer=" + body.from());
        if (!r1.getStatusCode().is2xxSuccessful()) return r1;
        return r2;
    }

    @PostMapping("/heal-partition")
    public ResponseEntity<?> healPartition(@RequestBody PartitionBody body) {
        var r1 = proxy(body.from(), "/debug/chaos/heal-partition", "peer=" + body.to());
        var r2 = proxy(body.to(), "/debug/chaos/heal-partition", "peer=" + body.from());
        if (!r1.getStatusCode().is2xxSuccessful()) return r1;
        return r2;
    }

    @PostMapping("/inject-latency/{id}")
    public ResponseEntity<?> injectLatency(@PathVariable int id, @RequestBody LatencyBody body) {
        return proxy(id, "/debug/chaos/inject-latency", "ms=" + body.millis());
    }

    @PostMapping("/force-election/{id}")
    public ResponseEntity<?> forceElection(@PathVariable int id) {
        return proxy(id, "/debug/chaos/force-election", "");
    }

    private ResponseEntity<?> proxy(int brokerId, String path, String query) {
        try {
            return ResponseEntity.ok(client.post(brokerId, path, query));
        } catch (BrokerChaosClient.BrokerUnknown e) {
            return ResponseEntity.status(404)
                    .body(new RestError(
                            "broker_unknown", e.getMessage(), Map.of("hint", "configure jbroker.admin.chaos.brokers")));
        } catch (BrokerChaosClient.ChaosDisabled e) {
            return ResponseEntity.status(503)
                    .body(new RestError(
                            "chaos_disabled",
                            e.getMessage(),
                            Map.of("hint", "start broker with jbroker.chaos.enabled=true")));
        } catch (Exception e) {
            log.warn("chaos proxy failed: broker {} path {}", brokerId, path, e);
            return ResponseEntity.status(502).body(new RestError("upstream_error", e.toString(), Map.of()));
        }
    }

    /**
     * Read-only cluster-wide chaos snapshot. Fan-out to every configured
     * broker's {@code /debug/chaos/state} and aggregate. Brokers whose
     * chaos endpoint is unreachable surface as {@code available=false};
     * callers (admin UI topology) render those with a neutral indicator
     * instead of pretending the rest of the snapshot is authoritative.
     */
    @GetMapping("/state")
    public ChaosStateSnapshot state() {
        var out = new ArrayList<ChaosStateSnapshot.BrokerChaosState>();
        var ids = new java.util.TreeSet<>(client.knownBrokerIds());
        for (int id : ids) {
            out.add(fetchState(id));
        }
        return new ChaosStateSnapshot(out);
    }

    private ChaosStateSnapshot.BrokerChaosState fetchState(int brokerId) {
        try {
            String body = client.getState(brokerId);
            return parseState(brokerId, body);
        } catch (BrokerChaosClient.ChaosDisabled | BrokerChaosClient.BrokerUnknown e) {
            return ChaosStateSnapshot.BrokerChaosState.unavailable(brokerId);
        } catch (Exception e) {
            log.warn("chaos state fetch failed: broker {}", brokerId, e);
            return ChaosStateSnapshot.BrokerChaosState.unavailable(brokerId);
        }
    }

    // Minimal JSON parse — the broker response is a fixed schema produced by
    // ChaosHttpServer.handleState. Keeping this regex-based avoids pulling
    // Jackson's ObjectMapper into a hot-ish poll path; the payload is small
    // and the shape is stable.
    private static final Pattern PAUSED = Pattern.compile("\"paused\"\\s*:\\s*(true|false)");
    private static final Pattern LATENCY = Pattern.compile("\"latency_ms\"\\s*:\\s*(\\d+)");
    private static final Pattern OUTBOUND = Pattern.compile("\"outbound_blocked_peers\"\\s*:\\s*\\[([^]]*)]");
    private static final Pattern INBOUND = Pattern.compile("\"inbound_blocked_peers\"\\s*:\\s*\\[([^]]*)]");

    private static ChaosStateSnapshot.BrokerChaosState parseState(int brokerId, String json) {
        boolean paused = matchBool(PAUSED, json, false);
        long latency = matchLong(LATENCY, json, 0L);
        List<Integer> outbound = matchIntList(OUTBOUND, json);
        List<Integer> inbound = matchIntList(INBOUND, json);
        return new ChaosStateSnapshot.BrokerChaosState(brokerId, true, paused, outbound, inbound, latency);
    }

    private static boolean matchBool(Pattern p, String s, boolean fallback) {
        Matcher m = p.matcher(s);
        return m.find() ? "true".equals(m.group(1)) : fallback;
    }

    private static long matchLong(Pattern p, String s, long fallback) {
        Matcher m = p.matcher(s);
        return m.find() ? Long.parseLong(m.group(1)) : fallback;
    }

    private static List<Integer> matchIntList(Pattern p, String s) {
        Matcher m = p.matcher(s);
        if (!m.find()) return List.of();
        String inside = m.group(1).trim();
        if (inside.isEmpty()) return List.of();
        var out = new ArrayList<Integer>();
        for (String tok : inside.split(",")) {
            String t = tok.trim();
            if (t.isEmpty()) continue;
            try {
                out.add(Integer.parseInt(t));
            } catch (NumberFormatException ignored) {
                /* skip malformed token */
            }
        }
        return out;
    }

    public record PartitionBody(int from, int to) {}

    public record LatencyBody(long millis) {}
}
