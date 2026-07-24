package jbroker.admin.api;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;
import org.springframework.stereotype.Component;

/**
 * Bridges {@link MetricsScraper}'s snapshots into the
 * {@link MeterRegistry} that backs {@code /actuator/prometheus}.
 *
 * <p>Per-broker scalars use functional gauges reading from an
 * {@link AtomicLong} / {@link DoubleAdder}; a single holder per meter
 * family avoids re-registering on each scrape. Per-partition metrics
 * use {@link MultiGauge} which re-emits a row per (topic, partition) on
 * each refresh.
 */
@Component
public class PrometheusMetricsBinder {

    private final MeterRegistry registry;
    private final ConcurrentHashMap<Integer, BrokerMeters> brokerMeters = new ConcurrentHashMap<>();

    private final MultiGauge isrSize;
    private final MultiGauge hwm;
    private final MultiGauge leaderLeo;
    private final MultiGauge replicationLag;

    public PrometheusMetricsBinder(MeterRegistry registry) {
        this.registry = registry;
        this.isrSize = MultiGauge.builder("jbroker_isr_size")
                .description("Size of the in-sync replica set for the partition")
                .register(registry);
        this.hwm = MultiGauge.builder("jbroker_hwm")
                .description("High watermark for the partition (min LEO across ISR)")
                .register(registry);
        this.leaderLeo = MultiGauge.builder("jbroker_leader_log_end_offset")
                .description("Leader's local log end offset for the partition")
                .register(registry);
        this.replicationLag = MultiGauge.builder("jbroker_replication_lag_records")
                .description("Per-follower replication lag in records")
                .register(registry);
    }

    public void refresh(MetricsScraper.Snapshot snap) {
        for (var entry : snap.byBroker().entrySet()) {
            int brokerId = entry.getKey();
            var resp = entry.getValue();
            var m = brokerMeters.computeIfAbsent(brokerId, this::registerBrokerMeters);
            m.produceCount.set(resp.getProduceCount());
            m.produceBytes.set(resp.getProduceBytes());
            m.fetchCount.set(resp.getFetchCount());
            m.fetchBytes.set(resp.getFetchBytes());
            setDouble(m.produceBytesPerSec, resp.getProduceBytesPerSec());
            setDouble(m.fetchBytesPerSec, resp.getFetchBytesPerSec());
            setDouble(m.produceP50, resp.getProduceP50Nanos() / 1_000_000_000.0);
            setDouble(m.produceP99, resp.getProduceP99Nanos() / 1_000_000_000.0);
            setDouble(m.produceP999, resp.getProduceP999Nanos() / 1_000_000_000.0);
            setDouble(m.fetchP50, resp.getFetchP50Nanos() / 1_000_000_000.0);
            setDouble(m.fetchP99, resp.getFetchP99Nanos() / 1_000_000_000.0);
            setDouble(m.fetchP999, resp.getFetchP999Nanos() / 1_000_000_000.0);
            m.raftTerm.set(resp.getRaftCurrentTerm());
            m.raftCommitIndex.set(resp.getRaftCommitIndex());
            m.raftLastApplied.set(resp.getRaftLastApplied());
            m.raftLastLogIndex.set(resp.getRaftLastLogIndex());
            m.diskUsableBytes.set(resp.getDiskUsableBytes());
            m.diskHeadroomLow.set(resp.getDiskHeadroomLow() ? 1 : 0);
        }
        var isrRows = new ArrayList<MultiGauge.Row<?>>();
        var hwmRows = new ArrayList<MultiGauge.Row<?>>();
        var leoRows = new ArrayList<MultiGauge.Row<?>>();
        var lagRows = new ArrayList<MultiGauge.Row<?>>();
        for (var resp : snap.responses()) {
            int brokerId = resp.getBrokerId();
            for (var pm : resp.getPartitionMetricsList()) {
                Tags baseTags = Tags.of(
                        "broker_id", Integer.toString(brokerId),
                        "topic", pm.getTopic(),
                        "partition", Integer.toString(pm.getPartition()));
                isrRows.add(MultiGauge.Row.of(baseTags, pm.getIsrSize()));
                hwmRows.add(MultiGauge.Row.of(baseTags, pm.getHwm()));
                leoRows.add(MultiGauge.Row.of(baseTags, pm.getLeaderLogEndOffset()));
                for (Map.Entry<Integer, Long> e :
                        pm.getReplicationLagByFollowerMap().entrySet()) {
                    Tags followerTags = baseTags.and("follower_id", Integer.toString(e.getKey()));
                    lagRows.add(MultiGauge.Row.of(followerTags, e.getValue()));
                }
            }
        }
        isrSize.register(isrRows, true);
        hwm.register(hwmRows, true);
        leaderLeo.register(leoRows, true);
        replicationLag.register(lagRows, true);
    }

    private BrokerMeters registerBrokerMeters(int brokerId) {
        var m = new BrokerMeters();
        String bid = Integer.toString(brokerId);
        // Monotonic values exposed as gauges because Micrometer's Counter
        // reads its own FunctionCounter state, while we just want to mirror
        // a snapshot from DescribeMetrics. Name drops `_total` deliberately:
        // Micrometer strips it from gauge names, so matching the emitted
        // name here keeps asserts + dashboards consistent.
        Gauge.builder("jbroker_produce_count", m.produceCount, AtomicLong::doubleValue)
                .tag("broker_id", bid)
                .description("Total produce RPCs")
                .register(registry);
        Gauge.builder("jbroker_produce_bytes", m.produceBytes, AtomicLong::doubleValue)
                .tag("broker_id", bid)
                .description("Total bytes produced")
                .register(registry);
        Gauge.builder("jbroker_fetch_count", m.fetchCount, AtomicLong::doubleValue)
                .tag("broker_id", bid)
                .description("Total fetch RPCs")
                .register(registry);
        Gauge.builder("jbroker_fetch_bytes", m.fetchBytes, AtomicLong::doubleValue)
                .tag("broker_id", bid)
                .description("Total bytes fetched")
                .register(registry);
        Gauge.builder("jbroker_produce_bytes_per_second", m.produceBytesPerSec, DoubleAdder::doubleValue)
                .tag("broker_id", bid)
                .register(registry);
        Gauge.builder("jbroker_fetch_bytes_per_second", m.fetchBytesPerSec, DoubleAdder::doubleValue)
                .tag("broker_id", bid)
                .register(registry);
        Gauge.builder("jbroker_produce_latency_seconds", m.produceP50, DoubleAdder::doubleValue)
                .tag("broker_id", bid)
                .tag("quantile", "0.5")
                .register(registry);
        Gauge.builder("jbroker_produce_latency_seconds", m.produceP99, DoubleAdder::doubleValue)
                .tag("broker_id", bid)
                .tag("quantile", "0.99")
                .register(registry);
        Gauge.builder("jbroker_produce_latency_seconds", m.produceP999, DoubleAdder::doubleValue)
                .tag("broker_id", bid)
                .tag("quantile", "0.999")
                .register(registry);
        Gauge.builder("jbroker_fetch_latency_seconds", m.fetchP50, DoubleAdder::doubleValue)
                .tag("broker_id", bid)
                .tag("quantile", "0.5")
                .register(registry);
        Gauge.builder("jbroker_fetch_latency_seconds", m.fetchP99, DoubleAdder::doubleValue)
                .tag("broker_id", bid)
                .tag("quantile", "0.99")
                .register(registry);
        Gauge.builder("jbroker_fetch_latency_seconds", m.fetchP999, DoubleAdder::doubleValue)
                .tag("broker_id", bid)
                .tag("quantile", "0.999")
                .register(registry);
        Gauge.builder("jbroker_raft_current_term", m.raftTerm, AtomicLong::doubleValue)
                .tag("broker_id", bid)
                .register(registry);
        Gauge.builder("jbroker_raft_commit_index", m.raftCommitIndex, AtomicLong::doubleValue)
                .tag("broker_id", bid)
                .register(registry);
        Gauge.builder("jbroker_raft_last_applied", m.raftLastApplied, AtomicLong::doubleValue)
                .tag("broker_id", bid)
                .register(registry);
        Gauge.builder("jbroker_raft_last_log_index", m.raftLastLogIndex, AtomicLong::doubleValue)
                .tag("broker_id", bid)
                .register(registry);
        Gauge.builder("jbroker_disk_usable_bytes", m.diskUsableBytes, AtomicLong::doubleValue)
                .tag("broker_id", bid)
                .description("Usable bytes on the broker's data volume at its last headroom probe")
                .register(registry);
        Gauge.builder("jbroker_disk_headroom_low", m.diskHeadroomLow, AtomicLong::doubleValue)
                .tag("broker_id", bid)
                .description("1 while the broker refuses produces because usable space is below the watermark")
                .register(registry);
        return m;
    }

    private static void setDouble(DoubleAdder adder, double v) {
        adder.reset();
        adder.add(v);
    }

    private static final class BrokerMeters {
        final AtomicLong produceCount = new AtomicLong();
        final AtomicLong produceBytes = new AtomicLong();
        final AtomicLong fetchCount = new AtomicLong();
        final AtomicLong fetchBytes = new AtomicLong();
        final DoubleAdder produceBytesPerSec = new DoubleAdder();
        final DoubleAdder fetchBytesPerSec = new DoubleAdder();
        final DoubleAdder produceP50 = new DoubleAdder();
        final DoubleAdder produceP99 = new DoubleAdder();
        final DoubleAdder produceP999 = new DoubleAdder();
        final DoubleAdder fetchP50 = new DoubleAdder();
        final DoubleAdder fetchP99 = new DoubleAdder();
        final DoubleAdder fetchP999 = new DoubleAdder();
        final AtomicLong raftTerm = new AtomicLong();
        final AtomicLong raftCommitIndex = new AtomicLong();
        final AtomicLong raftLastApplied = new AtomicLong();
        final AtomicLong raftLastLogIndex = new AtomicLong();
        final AtomicLong diskUsableBytes = new AtomicLong();
        final AtomicLong diskHeadroomLow = new AtomicLong();
    }
}
