package jbroker.broker.replication;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class FollowerStateTrackerTest {

    @Test
    void recordOverwritesPriorEntryForSameFollower() {
        var tracker = new FollowerStateTracker();
        tracker.record("orders", 0, /* brokerId */ 2, /* leo */ 10L, /* now */ 1_000L);
        tracker.record("orders", 0, 2, 42L, 2_000L);

        var got = tracker.get("orders", 0, 2).orElseThrow();
        assertThat(got.leo()).isEqualTo(42L);
        assertThat(got.lastFetchMillis()).isEqualTo(2_000L);
    }

    @Test
    void getReturnsEmptyForUnknownFollower() {
        var tracker = new FollowerStateTracker();
        assertThat(tracker.get("orders", 0, 2)).isEmpty();
    }

    @Test
    void computeHwmTakesMinLeoAcrossIsrIncludingLeader() {
        var tracker = new FollowerStateTracker();
        tracker.record("orders", 0, /* follower */ 2, /* leo */ 50L, 1_000L);
        tracker.record("orders", 0, /* follower */ 3, /* leo */ 40L, 1_000L);

        long hwm = tracker.computeHwm("orders", 0, List.of(1, 2, 3), /* leaderId */ 1, /* leaderLeo */ 60L);
        assertThat(hwm).isEqualTo(40L); // min(60, 50, 40)
    }

    @Test
    void computeHwmIgnoresNonIsrFollowersEvenIfTracked() {
        var tracker = new FollowerStateTracker();
        tracker.record("orders", 0, 2, 50L, 1_000L);
        tracker.record("orders", 0, 3, 10L, 1_000L); // not in ISR — excluded

        long hwm = tracker.computeHwm("orders", 0, List.of(1, 2), 1, 60L);
        assertThat(hwm).isEqualTo(50L); // broker 3 skipped
    }

    @Test
    void computeHwmZeroWhenIsrMemberHasNeverFetched() {
        var tracker = new FollowerStateTracker();
        // Broker 2 is in ISR but has never sent a fetch request yet.
        long hwm = tracker.computeHwm("orders", 0, List.of(1, 2), 1, 100L);
        assertThat(hwm).isZero();
    }

    @Test
    void laggardsOfReturnsFollowersWithStaleLastFetch() {
        var tracker = new FollowerStateTracker();
        tracker.record("orders", 0, 2, 50L, /* now */ 1_000L);
        tracker.record("orders", 0, 3, 50L, /* now */ 13_500L);

        var laggards = tracker.laggardsOf("orders", 0, List.of(2, 3), /* now */ 15_000L, /* lagTimeoutMs */ 3_000L);
        // broker 2 last fetched 14s ago (lag), broker 3 only 1.5s ago (fresh)
        assertThat(laggards).containsExactly(2);
    }

    @Test
    void laggardsOfTreatsNeverFetchedAsLaggard() {
        var tracker = new FollowerStateTracker();
        var laggards = tracker.laggardsOf("orders", 0, List.of(2), 10_000L, 3_000L);
        assertThat(laggards).containsExactly(2);
    }
}
