package jbroker.broker.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

final class ChaosStateTest {

    @Test
    void pauseToggles() {
        var s = new ChaosState();
        assertThat(s.isPaused()).isFalse();
        s.pause();
        assertThat(s.isPaused()).isTrue();
        s.resume();
        assertThat(s.isPaused()).isFalse();
    }

    @Test
    void blockPeerTracksIndependently() {
        var s = new ChaosState();
        s.blockPeer(2);
        assertThat(s.isBlocked(2)).isTrue();
        assertThat(s.isBlocked(3)).isFalse();
        s.unblockPeer(2);
        assertThat(s.isBlocked(2)).isFalse();
    }

    @Test
    void clearBlockedPeersRemovesAll() {
        var s = new ChaosState();
        s.blockPeer(2);
        s.blockPeer(3);
        s.clearBlockedPeers();
        assertThat(s.isBlocked(2)).isFalse();
        assertThat(s.isBlocked(3)).isFalse();
    }

    @Test
    void latencyClampsToZero() {
        var s = new ChaosState();
        s.setLatencyMs(-100);
        assertThat(s.latencyMs()).isZero();
        s.setLatencyMs(50);
        assertThat(s.latencyMs()).isEqualTo(50);
    }

    @Test
    void maybeSleepRespectsInterrupt() {
        var s = new ChaosState();
        s.setLatencyMs(10_000);
        Thread.currentThread().interrupt();
        long t0 = System.nanoTime();
        s.maybeSleep();
        long elapsed = System.nanoTime() - t0;
        // Interrupt clears the flag inside maybeSleep but propagates via
        // Thread.currentThread().interrupt(); here we simply assert the
        // sleep returned quickly rather than waited 10s.
        assertThat(java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(elapsed)).isLessThan(1_000);
        // Reset the interrupt flag for other tests.
        Thread.interrupted();
    }
}
