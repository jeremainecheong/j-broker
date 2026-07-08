package jbroker.admin.api;

import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import jbroker.admin.client.BrokerAdminClientPool;
import jbroker.proto.broker.DescribeMetricsResponse;
import jbroker.proto.common.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * {@code /api/v1/metrics/*} — throughput + latency charts for the UI.
 *
 * <p>Simple cluster-wide sum / max merging; the proper time-series surface
 * is the Prometheus + Grafana path.
 */
@RestController
@RequestMapping("/api/v1/metrics")
public class MetricsController {

    private static final Logger log = LoggerFactory.getLogger(MetricsController.class);

    private final BrokerAdminClientPool pool;
    private final ThroughputHistory history;

    public MetricsController(BrokerAdminClientPool pool, ThroughputHistory history) {
        this.pool = pool;
        this.history = history;
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
        // Idle brokers report windowSeconds=0.0 because no sample has fired.
        // Serialise as null so downstream scrapers can distinguish "no data
        // yet" from "observed zero over N seconds" and not divide by zero.
        Double reportedWindow = windowSeconds == 0.0 ? null : windowSeconds;
        return new ThroughputView(
                reportedWindow, produceCount, produceBytes, fetchCount, fetchBytes, produceBps, fetchBps);
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

    /**
     * Persistent time-series over the admin-app-side
     * {@link ThroughputHistory} ring. Browsers hydrate their sparklines
     * from this endpoint on page load so navigation no longer resets
     * history. {@code window} accepts simple "5m", "30s", "1h" style
     * tokens; absent / malformed falls back to 5 min.
     */
    @GetMapping("/timeseries")
    public TimeseriesView timeseries(@RequestParam(name = "window", defaultValue = "5m") String window) {
        Duration requested = parseWindow(window);
        var samples = history.since(requested);
        var out = new ArrayList<TimeseriesSample>(samples.size());
        for (var s : samples) {
            out.add(new TimeseriesSample(
                    s.ts(), s.produceBytesPerSec(), s.fetchBytesPerSec(), s.produceP99Nanos(), s.fetchP99Nanos()));
        }
        return new TimeseriesView(requested.getSeconds(), out);
    }

    private static Duration parseWindow(String token) {
        if (token == null || token.isBlank()) return Duration.ofMinutes(5);
        try {
            char unit = token.charAt(token.length() - 1);
            long n = Long.parseLong(token.substring(0, token.length() - 1));
            return switch (unit) {
                case 's' -> Duration.ofSeconds(n);
                case 'm' -> Duration.ofMinutes(n);
                case 'h' -> Duration.ofHours(n);
                default -> Duration.ofMinutes(5);
            };
        } catch (Exception e) {
            return Duration.ofMinutes(5);
        }
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

    // `windowSeconds` is boxed (nullable) so an idle broker — which reports
    // zero sample time — emits `"window_seconds": null` instead of `0.0`.
    // Lets Prometheus-style scrapers distinguish "no data yet" from a real
    // zero and skip the div-by-zero computation without special-casing.
    public record ThroughputView(
            Double windowSeconds,
            long produceCount,
            long produceBytes,
            long fetchCount,
            long fetchBytes,
            double produceBytesPerSec,
            double fetchBytesPerSec) {}

    public record LatencyView(Percentiles produce, Percentiles fetch) {}

    public record Percentiles(long p50Nanos, long p99Nanos, long p999Nanos) {}

    public record TimeseriesSample(
            long ts, double produceBytesPerSec, double fetchBytesPerSec, long produceP99Nanos, long fetchP99Nanos) {}

    public record TimeseriesView(long windowSeconds, List<TimeseriesSample> samples) {}
}
