package jbroker.admin.api;

import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import jbroker.admin.client.BrokerAdminClientPool;
import jbroker.proto.broker.DescribeMetricsResponse;
import jbroker.proto.common.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * {@code /api/v1/metrics/*} — throughput + latency charts for the UI.
 *
 * <p>P8.5 scope: simple cluster-wide sum / max merging. Phase 9 will replace
 * this with a proper time-series surface (Prometheus + Grafana).
 */
@RestController
@RequestMapping("/api/v1/metrics")
public class MetricsController {

    private static final Logger log = LoggerFactory.getLogger(MetricsController.class);

    private final BrokerAdminClientPool pool;

    public MetricsController(BrokerAdminClientPool pool) {
        this.pool = pool;
    }

    @GetMapping("/throughput")
    public ThroughputView throughput() {
        var responses = collectMetrics();
        double produceBps = 0, fetchBps = 0;
        long produceBytes = 0, fetchBytes = 0, produceCount = 0, fetchCount = 0;
        double windowSeconds = 0;
        for (var r : responses) {
            produceBps += r.getProduceBytesPerSec();
            fetchBps += r.getFetchBytesPerSec();
            produceBytes += r.getProduceBytes();
            fetchBytes += r.getFetchBytes();
            produceCount += r.getProduceCount();
            fetchCount += r.getFetchCount();
            windowSeconds = Math.max(windowSeconds, r.getWindowSeconds());
        }
        return new ThroughputView(
                windowSeconds, produceCount, produceBytes, fetchCount, fetchBytes, produceBps, fetchBps);
    }

    @GetMapping("/latency")
    public LatencyView latency() {
        var responses = collectMetrics();
        long produceP50 = 0, produceP99 = 0, produceP999 = 0;
        long fetchP50 = 0, fetchP99 = 0, fetchP999 = 0;
        // Max-per-percentile across brokers is a conservative proxy for
        // cluster-wide worst-case latency; adequate for a first-cut UI.
        for (var r : responses) {
            produceP50 = Math.max(produceP50, r.getProduceP50Nanos());
            produceP99 = Math.max(produceP99, r.getProduceP99Nanos());
            produceP999 = Math.max(produceP999, r.getProduceP999Nanos());
            fetchP50 = Math.max(fetchP50, r.getFetchP50Nanos());
            fetchP99 = Math.max(fetchP99, r.getFetchP99Nanos());
            fetchP999 = Math.max(fetchP999, r.getFetchP999Nanos());
        }
        return new LatencyView(
                new Percentiles(produceP50, produceP99, produceP999), new Percentiles(fetchP50, fetchP99, fetchP999));
    }

    private List<DescribeMetricsResponse> collectMetrics() {
        var out = new ArrayList<DescribeMetricsResponse>();
        for (var c : pool.clients()) {
            try {
                var r = c.describeMetrics();
                if (r.getError() == ErrorCode.OK) out.add(r);
            } catch (StatusRuntimeException e) {
                log.debug("broker {} unreachable for describeMetrics: {}", c.address(), e.getStatus());
            }
        }
        return out;
    }

    public record ThroughputView(
            double windowSeconds,
            long produceCount,
            long produceBytes,
            long fetchCount,
            long fetchBytes,
            double produceBytesPerSec,
            double fetchBytesPerSec) {}

    public record LatencyView(Percentiles produce, Percentiles fetch) {}

    public record Percentiles(long p50Nanos, long p99Nanos, long p999Nanos) {}
}
