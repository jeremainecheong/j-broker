package jbroker.broker.client.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import jbroker.proto.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * Pure bookkeeping behind the consumer's flow controls: pause/resume
 * filtering, the {@code max.poll.records} buffer (surplus preserved in
 * order), and position advancement tied to records actually returned.
 */
class FetchStateTest {

    private static final TopicPartition P0 = tp(0);
    private static final TopicPartition P1 = tp(1);

    @Test
    void drainCapsAtMaxAndKeepsSurplusInOrder() {
        var state = new FetchState<String, String>();
        state.position(P0, 0L);
        state.buffer(P0, records(P0, 0, 20));

        var first = state.drain(10);
        assertThat(offsets(first.get(P0))).containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        assertThat(state.position(P0)).isEqualTo(10L);
        assertThat(state.bufferedCount()).isEqualTo(10);

        var second = state.drain(10);
        assertThat(offsets(second.get(P0))).containsExactly(10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L);
        assertThat(state.position(P0)).isEqualTo(20L);
        assertThat(state.bufferedCount()).isZero();

        assertThat(state.drain(10)).isEmpty();
    }

    @Test
    void drainSpansPartitionsUpToTheBound() {
        var state = new FetchState<String, String>();
        state.buffer(P0, records(P0, 0, 5));
        state.buffer(P1, records(P1, 100, 105));

        var out = state.drain(7);
        assertThat(offsets(out.get(P0))).containsExactly(0L, 1L, 2L, 3L, 4L);
        assertThat(offsets(out.get(P1))).containsExactly(100L, 101L);
        assertThat(state.position(P1)).isEqualTo(102L);

        // Surplus continues exactly where the previous drain stopped.
        assertThat(offsets(state.drain(7).get(P1))).containsExactly(102L, 103L, 104L);
    }

    @Test
    void positionAdvancesOnlyWhenRecordsAreReturned() {
        var state = new FetchState<String, String>();
        state.position(P0, 0L);
        state.buffer(P0, records(P0, 0, 10));

        // Fetched-but-unreturned records leave the position untouched — a
        // commit taken now must not cover them.
        assertThat(state.position(P0)).isZero();

        state.drain(4);
        assertThat(state.position(P0)).isEqualTo(4L);
    }

    @Test
    void pausedPartitionIsNotFetchableAndItsBufferIsHeldBack() {
        var state = new FetchState<String, String>();
        state.buffer(P0, records(P0, 0, 3));
        state.pause(P0);

        assertThat(state.fetchable(P0)).isFalse();
        assertThat(state.paused()).containsExactly(P0);
        assertThat(state.drain(10)).isEmpty();
        assertThat(state.bufferedCount()).isEqualTo(3);

        state.resume(P0);
        assertThat(offsets(state.drain(10).get(P0))).containsExactly(0L, 1L, 2L);
    }

    @Test
    void pausedPartitionDoesNotBlockOthers() {
        var state = new FetchState<String, String>();
        state.buffer(P0, records(P0, 0, 3));
        state.buffer(P1, records(P1, 50, 53));
        state.pause(P0);

        var out = state.drain(10);
        assertThat(out).containsOnlyKeys(P1);
        assertThat(offsets(out.get(P1))).containsExactly(50L, 51L, 52L);
        assertThat(state.fetchable(P1)).isTrue();
    }

    @Test
    void bufferedPartitionIsNotFetchableUntilFullyDrained() {
        var state = new FetchState<String, String>();
        assertThat(state.fetchable(P0)).isTrue();

        state.buffer(P0, records(P0, 0, 6));
        assertThat(state.fetchable(P0)).isFalse();

        state.drain(3);
        assertThat(state.fetchable(P0)).isFalse();

        state.drain(3);
        assertThat(state.fetchable(P0)).isTrue();
    }

    @Test
    void forgetLeavesNoResidue() {
        var state = new FetchState<String, String>();
        state.position(P0, 7L);
        state.pause(P0);
        state.buffer(P0, records(P0, 7, 9));

        state.forget(P0);

        assertThat(state.hasPosition(P0)).isFalse();
        assertThat(state.paused()).isEmpty();
        assertThat(state.fetchable(P0)).isTrue();
        assertThat(state.drain(10)).isEmpty();
    }

    private static TopicPartition tp(int partition) {
        return TopicPartition.newBuilder()
                .setTopic("orders")
                .setPartition(partition)
                .build();
    }

    private static List<ConsumerRecord<String, String>> records(TopicPartition tp, long from, long toExclusive) {
        var out = new ArrayList<ConsumerRecord<String, String>>();
        for (long o = from; o < toExclusive; o++) {
            out.add(new ConsumerRecord<>(tp, o, null, "v" + o));
        }
        return out;
    }

    private static List<Long> offsets(List<ConsumerRecord<String, String>> records) {
        return records.stream().map(ConsumerRecord::offset).toList();
    }
}
