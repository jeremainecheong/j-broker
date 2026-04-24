package jbroker.admin.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import jbroker.proto.broker.DescribeTopicPartitionsResponse;
import jbroker.proto.broker.PartitionStateInfo;
import jbroker.proto.common.ErrorCode;
import org.junit.jupiter.api.Test;

/**
 * covers {@link TopicsController#mergeLeaderReportedOffsets}. The
 * broker-side {@code DescribeTopicPartitions} only fills real HWM/LEO when
 * self is the partition leader; every other broker returns -1 for those two
 * fields. Admin-app therefore fans out + overlays leader-reported offsets
 * before serving the JSON.
 */
final class TopicsControllerMergeTest {

    private static PartitionStateInfo ps(int p, int leader, long hwm, long leo) {
        return PartitionStateInfo.newBuilder()
                .setPartition(p)
                .setLeader(leader)
                .setHighWatermark(hwm)
                .setLogEndOffset(leo)
                .build();
    }

    private static DescribeTopicPartitionsResponse resp(String topic, List<PartitionStateInfo> states) {
        var b = DescribeTopicPartitionsResponse.newBuilder()
                .setError(ErrorCode.OK)
                .setTopic(topic);
        for (var s : states) b.addPartitionStates(s);
        return b.build();
    }

    @Test
    void leaderReportedOffsetsOverlayFollowerSentinels() {
        // broker1 is follower (returns -1/-1), broker3 is leader (returns 42/42)
        var follower = resp("hello", List.of(ps(0, 3, -1, -1), ps(1, 3, -1, -1)));
        var leader = resp("hello", List.of(ps(0, 3, 42, 42), ps(1, 3, 17, 17)));

        var merged = TopicsController.mergeLeaderReportedOffsets(follower, List.of(follower, leader));
        assertThat(merged.getPartitionStatesList())
                .extracting(PartitionStateInfo::getHighWatermark)
                .containsExactly(42L, 17L);
        assertThat(merged.getPartitionStatesList())
                .extracting(PartitionStateInfo::getLogEndOffset)
                .containsExactly(42L, 17L);
    }

    @Test
    void leadershipSpreadAcrossBrokersStillMergesCorrectly() {
        // broker1 leads p0 (returns 5/5), broker3 leads p1 (returns 9/9);
        // each broker reports -1 for partitions it doesn't lead.
        var broker1 = resp("hello", List.of(ps(0, 1, 5, 5), ps(1, 3, -1, -1)));
        var broker3 = resp("hello", List.of(ps(0, 1, -1, -1), ps(1, 3, 9, 9)));

        var merged = TopicsController.mergeLeaderReportedOffsets(broker1, List.of(broker1, broker3));
        assertThat(merged.getPartitionStatesList())
                .extracting(PartitionStateInfo::getHighWatermark)
                .containsExactly(5L, 9L);
    }

    @Test
    void primaryReturnedUntouchedWhenNoBrokerReportsRealOffsets() {
        // Every broker is a follower or partition has no leader yet; keep
        // the primary's sentinel offsets instead of corrupting the response.
        var allFollowers = resp("hello", List.of(ps(0, -1, -1, -1)));
        var merged = TopicsController.mergeLeaderReportedOffsets(allFollowers, List.of(allFollowers));
        assertThat(merged.getPartitionStatesList().get(0).getHighWatermark()).isEqualTo(-1L);
        assertThat(merged.getPartitionStatesList().get(0).getLogEndOffset()).isEqualTo(-1L);
    }

    @Test
    void errorResponsesIgnoredDuringMerge() {
        // One broker responded with an error (e.g., still catching up). Its
        // partition states must not overwrite the leader's real values.
        var errResp = DescribeTopicPartitionsResponse.newBuilder()
                .setError(ErrorCode.UNKNOWN)
                .addPartitionStates(ps(0, 3, 999, 999)) // bogus — should be ignored because error != OK
                .build();
        var leaderResp = resp("hello", List.of(ps(0, 3, 7, 7)));

        var merged = TopicsController.mergeLeaderReportedOffsets(leaderResp, List.of(errResp, leaderResp));
        assertThat(merged.getPartitionStatesList().get(0).getHighWatermark()).isEqualTo(7L);
    }
}
