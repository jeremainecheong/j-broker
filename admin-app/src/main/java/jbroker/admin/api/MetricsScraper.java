package jbroker.admin.api;

import io.grpc.StatusRuntimeException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import jbroker.admin.client.BrokerAdminClientPool;
import jbroker.proto.broker.DescribeMetricsResponse;
import jbroker.proto.common.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Background scraper that fans out {@code DescribeMetrics} across
 * brokers and exposes the latest snapshot to {@link PrometheusMetricsBinder}.
 *
 * <p>Each cycle pulls every reachable broker; unreachable brokers drop
 * out of the snapshot for that cycle. Their per-broker Prometheus
 * gauges freeze at their last values, so the binder derives
 * {@code jbroker_broker_scrape_ok} from the absence and alerts key on
 * that rather than on the frozen values.
 */
@Component
public class MetricsScraper {

    private static final Logger log = LoggerFactory.getLogger(MetricsScraper.class);

    private final BrokerAdminClientPool pool;
    private final PrometheusMetricsBinder binder;
    private final ThroughputHistory history;
    private final int intervalSeconds;
    private final AtomicReference<Snapshot> latest = new AtomicReference<>(Snapshot.empty());
    private ScheduledExecutorService exec;

    public MetricsScraper(
            BrokerAdminClientPool pool,
            PrometheusMetricsBinder binder,
            ThroughputHistory history,
            @Value("${jbroker.metrics.scrape.intervalSeconds:5}") int intervalSeconds) {
        this.pool = pool;
        this.binder = binder;
        this.history = history;
        this.intervalSeconds = intervalSeconds;
    }

    @PostConstruct
    public void start() {
        exec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = Thread.ofVirtual().name("metrics-scraper").unstarted(r);
            t.setDaemon(true);
            return t;
        });
        exec.scheduleWithFixedDelay(this::tick, 0, intervalSeconds, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void stop() {
        if (exec != null) exec.shutdownNow();
    }

    void tick() {
        try {
            // Fan out describeMetrics across brokers concurrently
            // via a virtual-thread-per-task executor, closed inside a
            // try-with-resources so the scrape waits for every fork to
            // finish before returning (structured-concurrency semantics
            // without the StructuredTaskScope preview API). Scrape
            // completes in max(per-broker RTT) rather than sum.
            var byBroker = new HashMap<Integer, DescribeMetricsResponse>();
            var tasks = new ArrayList<java.util.concurrent.Callable<DescribeMetricsResponse>>();
            for (var c : pool.clients()) {
                tasks.add(() -> {
                    try {
                        return c.describeMetrics();
                    } catch (StatusRuntimeException e) {
                        log.debug("broker {} unreachable during scrape: {}", c.address(), e.getStatus());
                        return null;
                    }
                });
            }
            try (var scope = Executors.newVirtualThreadPerTaskExecutor()) {
                var futures = scope.invokeAll(tasks);
                for (var f : futures) {
                    try {
                        var r = f.get();
                        if (r != null && r.getError() == ErrorCode.OK) byBroker.put(r.getBrokerId(), r);
                    } catch (java.util.concurrent.ExecutionException ee) {
                        log.debug("scrape subtask failed", ee.getCause());
                    }
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
            var snap = new Snapshot(Map.copyOf(byBroker));
            latest.set(snap);
            binder.refresh(snap);
            history.append(snap);
        } catch (Exception e) {
            log.warn("metrics scrape cycle failed", e);
        }
    }

    public Snapshot snapshot() {
        return latest.get();
    }

    public record Snapshot(Map<Integer, DescribeMetricsResponse> byBroker) {
        public static Snapshot empty() {
            return new Snapshot(Map.of());
        }

        public List<DescribeMetricsResponse> responses() {
            return List.copyOf(byBroker.values());
        }
    }
}
