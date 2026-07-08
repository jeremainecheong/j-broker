package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * The pause gate must no-op the tick so peers stop seeing fresh
 * heartbeats and eventually flip this broker to dead. We test the gate
 * without spinning up actual gRPC stubs by checking the observable side
 * effect: no channel activity while paused, resumed activity after unpause.
 *
 * <p>The sender is constructed with an empty peer list so the tick itself
 * does no I/O; what we're validating is that the pause check happens before
 * the per-peer loop, so behaviour is identical whether we have 0 or N peers.
 */
final class BrokerHeartbeatSenderPauseTest {

    @Test
    void pauseGateShortCircuitsTickBeforeAnyPeerWork() throws Exception {
        var paused = new AtomicBoolean(false);
        var offsetCalls = new AtomicInteger(0);

        // metadataOffset supplier doubles as a "tick ran" counter. If the
        // tick short-circuits on paused, the supplier is never called.
        var sender = new BrokerHeartbeatSender(
                1,
                List.of(),
                () -> {
                    offsetCalls.incrementAndGet();
                    return 0L;
                },
                10L,
                paused::get);
        try {
            sender.start();

            // Let a few ticks fire normally.
            Thread.sleep(50);
            int baseline = offsetCalls.get();
            assertThat(baseline).isGreaterThanOrEqualTo(1);

            // Pause and let more scheduled ticks fire.
            paused.set(true);
            int pausedAt = offsetCalls.get();
            Thread.sleep(80);
            int duringPause = offsetCalls.get();
            // Allow one extra call for the tick already in-flight when the
            // flag flipped. Anything more means the gate isn't blocking.
            assertThat(duringPause).isLessThanOrEqualTo(pausedAt + 1);

            // Resume: ticks should resume immediately.
            paused.set(false);
            Thread.sleep(80);
            int afterResume = offsetCalls.get();
            assertThat(afterResume).isGreaterThan(duringPause + 1);
        } finally {
            sender.close();
        }
    }

    @Test
    void backCompatConstructorDefaultsToUnpaused() {
        // 4-arg constructor behaves identically to the earlier unpaused-only constructor. Just
        // verifies it constructs without NPE; the real behaviour is exercised
        // by every existing BrokerHeartbeatSender user in the ITs.
        var sender = new BrokerHeartbeatSender(1, List.of(), () -> 0L, 1_000L);
        sender.close();
    }
}
