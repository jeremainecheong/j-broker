package jbroker.admin.api;

import java.util.Map;
import jbroker.admin.client.BrokerChaosClient;
import jbroker.admin.dto.RestError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * P9.3 — admin-side proxy for the cooperative chaos endpoints every
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

    public record PartitionBody(int from, int to) {}

    public record LatencyBody(long millis) {}
}
