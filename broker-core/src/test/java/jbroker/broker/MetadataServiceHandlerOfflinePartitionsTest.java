package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import jbroker.proto.broker.DescribeMetricsRequest;
import jbroker.proto.common.ErrorCode;
import org.junit.jupiter.api.Test;

/**
 * {@code DescribeMetrics.offline_partitions}: the replicated partition
 * table's leaderless entries (leader <= 0), reported only by the broker
 * that currently is the controller. Every other broker reports 0 so the
 * admin binder's max() across the fan-out has exactly one authoritative
 * non-zero source.
 */
class MetadataServiceHandlerOfflinePartitionsTest {

    /** Topic "t": partition 0 led by broker 1, partitions 1 and 2 leaderless. */
    private static TopicManager topicManagerWithTwoOffline() {
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 3, 3, 0L);
        tm.onPartitionChange("t", 0, 1, List.of(1, 2), List.of(1, 2, 3), 0, 0);
        // Fencer convention for "every ISR member died": leader -1 with the
        // committed-record holders preserved in the ISR.
        tm.onPartitionChange("t", 1, -1, List.of(2), List.of(1, 2, 3), 1, 0);
        tm.onPartitionChange("t", 2, 0, List.of(3), List.of(1, 2, 3), 0, 0);
        return tm;
    }

    private static MetadataServiceHandler handler(int selfId, Optional<Integer> controllerId, TopicManager tm) {
        return new MetadataServiceHandler(
                selfId,
                new BrokerRegistry(),
                new BrokerLiveness(),
                () -> "FOLLOWER",
                () -> controllerId,
                () -> 1L,
                () -> 0L,
                System::nanoTime,
                MetadataServiceHandler.DEFAULT_STALENESS_NANOS,
                tm,
                null,
                null,
                null,
                null,
                new BrokerMetrics());
    }

    @Test
    void controllerCountsPartitionsWithoutALiveLeader() {
        var handler = handler(1, Optional.of(1), topicManagerWithTwoOffline());

        var resp = handler.describeMetrics(DescribeMetricsRequest.getDefaultInstance());

        assertThat(resp.getError()).isEqualTo(ErrorCode.OK);
        assertThat(resp.getOfflinePartitions()).isEqualTo(2L);
    }

    @Test
    void nonControllerReportsZeroEvenWithOfflinePartitionsInView() {
        var handler = handler(1, Optional.of(2), topicManagerWithTwoOffline());

        var resp = handler.describeMetrics(DescribeMetricsRequest.getDefaultInstance());

        assertThat(resp.getError()).isEqualTo(ErrorCode.OK);
        assertThat(resp.getOfflinePartitions()).isZero();
    }

    @Test
    void noKnownControllerReportsZero() {
        var handler = handler(1, Optional.empty(), topicManagerWithTwoOffline());

        var resp = handler.describeMetrics(DescribeMetricsRequest.getDefaultInstance());

        assertThat(resp.getOfflinePartitions()).isZero();
    }

    @Test
    void controllerWithAllLeadersLiveReportsZero() {
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 3, 0L);
        tm.onPartitionChange("t", 0, 1, List.of(1, 2, 3), List.of(1, 2, 3), 0, 0);
        var handler = handler(1, Optional.of(1), tm);

        var resp = handler.describeMetrics(DescribeMetricsRequest.getDefaultInstance());

        assertThat(resp.getOfflinePartitions()).isZero();
    }
}
