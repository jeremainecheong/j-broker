package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class MonotonicClockTest {

    @Test
    void nanoTimeIsMonotonicallyNonDecreasing() {
        var clock = new MonotonicClock();
        long first = clock.nanoTime();
        long second = clock.nanoTime();
        assertThat(second).isGreaterThanOrEqualTo(first);
    }
}
