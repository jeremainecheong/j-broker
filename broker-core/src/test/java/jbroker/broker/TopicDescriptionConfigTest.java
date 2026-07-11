package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Effective-config resolution: explicit topic config wins, the cluster
 * default fills the gaps, and malformed stored values (which validation
 * should have kept out) degrade to the cluster default instead of
 * breaking the cleaner or the produce path.
 */
class TopicDescriptionConfigTest {

    private static TopicDescription topic(Map<String, String> config) {
        return new TopicDescription("orders", 1, 3, 0L, false, false, config);
    }

    @Test
    void retentionOverridesWinAndMinusOneMeansUnlimited() {
        var t = topic(Map.of(
                TopicDescription.RETENTION_MS_CONFIG, "60000",
                TopicDescription.RETENTION_BYTES_CONFIG, "-1"));

        assertThat(t.effectiveRetentionMillis(7L * 24 * 60 * 60 * 1000)).isEqualTo(60_000L);
        assertThat(t.effectiveRetentionBytes(1_000_000L)).isEqualTo(-1L);
    }

    @Test
    void clusterDefaultsApplyWithoutOverrides() {
        var t = topic(Map.of());

        assertThat(t.effectiveRetentionMillis(604_800_000L)).isEqualTo(604_800_000L);
        assertThat(t.effectiveRetentionBytes(-1L)).isEqualTo(-1L);
        assertThat(t.effectiveSegmentBytes(128L * 1024 * 1024)).isEqualTo(128L * 1024 * 1024);
    }

    @Test
    void malformedStoredValuesFallBackToTheClusterDefault() {
        var t = topic(Map.of(
                TopicDescription.RETENTION_MS_CONFIG, "one-week",
                TopicDescription.SEGMENT_BYTES_CONFIG, "big"));

        assertThat(t.effectiveRetentionMillis(1234L)).isEqualTo(1234L);
        assertThat(t.effectiveSegmentBytes(4096L)).isEqualTo(4096L);
    }

    @Test
    void segmentBytesClampsToTheFloor() {
        var t = topic(Map.of(TopicDescription.SEGMENT_BYTES_CONFIG, "1"));

        assertThat(t.effectiveSegmentBytes(128L * 1024 * 1024)).isEqualTo(TopicDescription.SEGMENT_BYTES_FLOOR);
    }
}
