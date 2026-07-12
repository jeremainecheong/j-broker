package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class DiskHeadroomTest {

    private static final long GIB = 1024L * 1024 * 1024;

    @Test
    void tripsWhenUsableSpaceFallsBelowTheWatermarkAndRecovers() throws Exception {
        var usable = new AtomicLong(10 * GIB);
        try (var headroom = new DiskHeadroom(usable::get, GIB, 20)) {
            assertThat(headroom.low()).isFalse();

            usable.set(GIB / 2);
            awaitLow(headroom, true);
            assertThat(headroom.lastUsableBytes()).isEqualTo(GIB / 2);

            // Recovery is automatic — the next probe above the watermark
            // clears the gate without operator action.
            usable.set(2 * GIB);
            awaitLow(headroom, false);
        }
    }

    @Test
    void bootingBelowTheWatermarkDegradesImmediately() throws Exception {
        // The first probe is synchronous: a broker started onto a full
        // volume must refuse produces from the first request, not after
        // the first async tick.
        try (var headroom = new DiskHeadroom(() -> GIB / 4, GIB, 60_000)) {
            assertThat(headroom.low()).isTrue();
        }
    }

    @Test
    void probeFailureKeepsThePreviousState() throws Exception {
        var usable = new AtomicLong(GIB / 2);
        var fail = new java.util.concurrent.atomic.AtomicBoolean(false);
        try (var headroom = new DiskHeadroom(
                () -> {
                    if (fail.get()) throw new RuntimeException("statvfs unavailable");
                    return usable.get();
                },
                GIB,
                20)) {
            assertThat(headroom.low()).isTrue();

            // A transient probe failure must not flap the gate open.
            fail.set(true);
            Thread.sleep(100);
            assertThat(headroom.low()).isTrue();

            fail.set(false);
            usable.set(2 * GIB);
            awaitLow(headroom, false);
        }
    }

    private static void awaitLow(DiskHeadroom headroom, boolean expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (headroom.low() == expected) return;
            Thread.sleep(10);
        }
        throw new AssertionError("headroom.low() did not become " + expected + " within 5s");
    }
}
