package jbroker.broker.replication;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ReassignmentThrottleTest {

    @Test
    void disabledThrottleGrantsTheFullRequest() {
        var t = new ReassignmentThrottle(0, () -> 0L);
        assertThat(t.reserve(1_000_000)).isEqualTo(1_000_000);
        assertThat(t.reserve(1_000_000)).isEqualTo(1_000_000);
    }

    @Test
    void startsEmptySoTheFirstFetchWaitsForRefill() {
        var clock = new AtomicLong(0);
        var t = new ReassignmentThrottle(1_000_000, clock::get);
        // No time has passed since construction — nothing to grant yet.
        assertThat(t.reserve(64 * 1024)).isZero();
    }

    @Test
    void grantsAtMostTheConfiguredRateOverTime() {
        var clock = new AtomicLong(0);
        long rate = 1_000_000; // 1 MB/s
        var t = new ReassignmentThrottle(rate, clock::get);

        // Advance 100ms and drain: at most 100ms worth of budget (100 KB).
        clock.addAndGet(100_000_000L);
        long granted = 0;
        for (int i = 0; i < 100; i++) {
            granted += t.reserve(64 * 1024);
        }
        assertThat(granted).isEqualTo(100_000); // exactly 0.1s * 1MB/s
    }

    @Test
    void sustainedRateStaysUnderTheCapAcrossAWindow() {
        var clock = new AtomicLong(0);
        long rate = 1_000_000;
        var t = new ReassignmentThrottle(rate, clock::get);

        long granted = 0;
        // Simulate 2 seconds in 25ms poll ticks, draining fully each tick.
        for (int tick = 0; tick < 80; tick++) {
            clock.addAndGet(25_000_000L);
            int g;
            while ((g = t.reserve(1024 * 1024)) > 0) {
                granted += g;
            }
        }
        // 2s at 1 MB/s = 2 MB; the empty-start bucket makes it exact.
        assertThat(granted).isEqualTo(2_000_000L);
    }

    @Test
    void doesNotBankMoreThanOneSecondOfBurstWhileIdle() {
        var clock = new AtomicLong(0);
        long rate = 1_000_000;
        var t = new ReassignmentThrottle(rate, clock::get);

        // Idle for 10 seconds — the bucket caps at 1s worth.
        clock.addAndGet(10_000_000_000L);
        assertThat(t.reserve(Integer.MAX_VALUE)).isEqualTo(1_000_000);
        // And nothing left immediately after.
        assertThat(t.reserve(Integer.MAX_VALUE)).isZero();
    }
}
