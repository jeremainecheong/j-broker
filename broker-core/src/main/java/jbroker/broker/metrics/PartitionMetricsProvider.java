package jbroker.broker.metrics;

import java.util.List;

/**
 * P9.1 — supplies per-partition metrics for {@code DescribeMetrics}.
 * Returns entries only for partitions this broker leads; followers omit
 * remote-led partitions to avoid double-counting across a cluster-wide
 * Prometheus scrape.
 */
public interface PartitionMetricsProvider {

    PartitionMetricsProvider NOOP = List::of;

    List<PartitionMetricsSnapshot> snapshot();
}
