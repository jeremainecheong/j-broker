package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * round-trip tests for the {@code __consumer_offsets} key/value codec.
 *
 * <p>Two record types live in the topic:
 * <ul>
 *   <li>Type 1 — offset commit. Key = {@code (group, topic, partition)};
 *       value = {@code (offset, leader_epoch, metadata, ts)}.</li>
 *   <li>Type 2 — group metadata. Key = {@code group}; value = serialized
 *       {@link ConsumerOffsetsTopic.GroupMetadataValue}. Group metadata is
 *       written by {@link GroupCoordinator} on every membership change so a
 *       fresh coordinator can rebuild state on failover ().</li>
 * </ul>
 */
class ConsumerOffsetsTopicCodecTest {

    @Test
    void offsetKeyRoundTrips() {
        byte[] key = ConsumerOffsetsTopic.keyForOffset("g1", "orders", 7);
        var decoded = ConsumerOffsetsTopic.decodeOffsetKey(key).orElseThrow();
        assertThat(decoded.group()).isEqualTo("g1");
        assertThat(decoded.topic()).isEqualTo("orders");
        assertThat(decoded.partition()).isEqualTo(7);
    }

    @Test
    void offsetValueRoundTrips() {
        byte[] value = ConsumerOffsetsTopic.valueForOffset(42L, 3, "checkpoint-at-X", 999L);
        var decoded = ConsumerOffsetsTopic.decodeOffsetValue(value);
        assertThat(decoded.offset()).isEqualTo(42L);
        assertThat(decoded.leaderEpoch()).isEqualTo(3);
        assertThat(decoded.metadata()).isEqualTo("checkpoint-at-X");
        assertThat(decoded.commitTimestamp()).isEqualTo(999L);
    }

    @Test
    void groupMetadataKeyRoundTrips() {
        byte[] key = ConsumerOffsetsTopic.keyForGroupMetadata("g1");
        // A group-metadata key cannot decode as an offset key (different type byte).
        assertThat(ConsumerOffsetsTopic.decodeOffsetKey(key)).isEmpty();
        assertThat(ConsumerOffsetsTopic.decodeGroupMetadataKey(key)).contains("g1");
    }

    @Test
    void offsetKeyDecodeRejectsWrongTypeByte() {
        byte[] groupKey = ConsumerOffsetsTopic.keyForGroupMetadata("g1");
        assertThat(ConsumerOffsetsTopic.decodeOffsetKey(groupKey)).isEmpty();
    }

    @Test
    void offsetValueDecodeRejectsTruncatedBytes() {
        byte[] value = ConsumerOffsetsTopic.valueForOffset(1L, 0, "", 0L);
        byte[] truncated = new byte[value.length - 1];
        System.arraycopy(value, 0, truncated, 0, truncated.length);
        assertThatThrownBy(() -> ConsumerOffsetsTopic.decodeOffsetValue(truncated))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void offsetKeyDecodeRejectsTruncatedBytes() {
        byte[] key = ConsumerOffsetsTopic.keyForOffset("g1", "t", 0);
        byte[] truncated = new byte[key.length - 1];
        System.arraycopy(key, 0, truncated, 0, truncated.length);
        assertThat(ConsumerOffsetsTopic.decodeOffsetKey(truncated)).isEmpty();
    }
}
