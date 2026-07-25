package jbroker.admin.api;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;
import jbroker.proto.broker.DescribeMetricsResponse;
import jbroker.proto.common.ErrorCode;
import org.junit.jupiter.api.Test;

/**
 * {@link PrometheusMetricsBinder} against a {@link SimpleMeterRegistry}
 * and hand-built snapshots — no brokers, no Spring context. Covers the
 * counter-mirroring gauges and the cluster-level offline-partitions
 * gauge (max across the fan-out, recomputed every refresh).
 */
class PrometheusMetricsBinderTest {

    private static DescribeMetricsResponse.Builder broker(int id) {
        return DescribeMetricsResponse.newBuilder().setError(ErrorCode.OK).setBrokerId(id);
    }

    private static MetricsScraper.Snapshot snapshot(DescribeMetricsResponse... responses) {
        var byBroker = new java.util.HashMap<Integer, DescribeMetricsResponse>();
        for (var r : responses) byBroker.put(r.getBrokerId(), r);
        return new MetricsScraper.Snapshot(Map.copyOf(byBroker));
    }

    private static double gauge(SimpleMeterRegistry registry, String name, String... tags) {
        return registry.get(name).tags(tags).gauge().value();
    }

    @Test
    void countersAreMirroredAsPerBrokerGauges() {
        var registry = new SimpleMeterRegistry();
        var binder = new PrometheusMetricsBinder(registry);

        binder.refresh(snapshot(broker(1)
                .setNotEnoughReplicasRejections(5)
                .setProduceQuotaDenials(3)
                .setFetchQuotaDenials(2)
                .setProduceQuotaThrottleMillis(1_500)
                .setFetchQuotaThrottleMillis(700)
                .build()));

        assertThat(gauge(registry, "jbroker_not_enough_replicas_rejections", "broker_id", "1"))
                .isEqualTo(5.0);
        assertThat(gauge(registry, "jbroker_quota_denials", "broker_id", "1", "op", "produce"))
                .isEqualTo(3.0);
        assertThat(gauge(registry, "jbroker_quota_denials", "broker_id", "1", "op", "fetch"))
                .isEqualTo(2.0);
        assertThat(gauge(registry, "jbroker_quota_throttle_millis", "broker_id", "1", "op", "produce"))
                .isEqualTo(1_500.0);
        assertThat(gauge(registry, "jbroker_quota_throttle_millis", "broker_id", "1", "op", "fetch"))
                .isEqualTo(700.0);
    }

    @Test
    void offlinePartitionsTakesTheMaxAcrossTheFanOut() {
        var registry = new SimpleMeterRegistry();
        var binder = new PrometheusMetricsBinder(registry);

        // Broker 2 is the controller and sees two leaderless partitions;
        // the non-controllers report 0.
        binder.refresh(snapshot(
                broker(1).build(),
                broker(2).setOfflinePartitions(2).build(),
                broker(3).build()));

        assertThat(gauge(registry, "jbroker_offline_partitions")).isEqualTo(2.0);
    }

    @Test
    void offlinePartitionsIsRecomputedNotFrozen() {
        var registry = new SimpleMeterRegistry();
        var binder = new PrometheusMetricsBinder(registry);

        binder.refresh(
                snapshot(broker(1).setOfflinePartitions(3).build(), broker(2).build()));
        assertThat(gauge(registry, "jbroker_offline_partitions")).isEqualTo(3.0);

        // Controller moved to broker 2 and the partitions recovered; the
        // old reporter now sends 0 and the gauge must follow it down.
        binder.refresh(snapshot(broker(1).build(), broker(2).build()));
        assertThat(gauge(registry, "jbroker_offline_partitions")).isZero();

        // New controller sees one leaderless partition.
        binder.refresh(
                snapshot(broker(1).build(), broker(2).setOfflinePartitions(1).build()));
        assertThat(gauge(registry, "jbroker_offline_partitions")).isEqualTo(1.0);
    }
}
