package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Root-cause guard for the MultiBrokerFailoverIT flake: when a peer's
 * broker port is unreachable at sender startup (sequential cluster start,
 * broker restart), the gRPC channel's exponential reconnect backoff grows
 * past the heartbeat interval. A peer that then comes up gets NO heartbeats
 * for multiple seconds — long enough that a broker can live and die without
 * ever landing in anyone's {@link BrokerLiveness}, which made it unfenceable.
 *
 * <p>The sender's 250ms tick <em>is</em> the retry policy; channel-level
 * backoff must not stack on top of it. This test lets backoff build for 8s
 * against an unbound port, then binds the receiver and requires the first
 * heartbeat to land within 1.5s. Without backoff-reset the next connect
 * attempt is typically 0.5–4s out (and each further failure doubles it);
 * with it, delivery is bounded by ~one interval.
 */
final class BrokerHeartbeatSenderBackoffRecoveryTest {

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void lateStartingPeerReceivesHeartbeatWithinOneIntervalNotBackoff() throws Exception {
        int port = freePort();
        var liveness = new BrokerLiveness();
        var handler = new BrokerHeartbeatHandler(liveness, System::nanoTime);

        var sender = new BrokerHeartbeatSender(
                1, List.of(new BrokerHeartbeatSender.PeerAddress(2, "127.0.0.1", port)), () -> 0L, 100L);
        try {
            sender.start();

            // Accumulate connect failures so channel backoff grows well past
            // the heartbeat interval (gRPC default: 1s initial, ×1.6).
            Thread.sleep(8_000);
            assertThat(liveness.lastSignal(1))
                    .as("sanity: nothing delivered while port unbound")
                    .isEmpty();

            // Peer comes up late.
            Server server = NettyServerBuilder.forPort(port)
                    .addService(BrokerGrpcServices.cluster(handler))
                    .build()
                    .start();
            try {
                long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(1_500);
                while (liveness.lastSignal(1).isEmpty() && System.nanoTime() < deadline) {
                    Thread.sleep(20);
                }
                assertThat(liveness.lastSignal(1))
                        .as("first heartbeat must land within 1.5s of the peer becoming reachable; "
                                + "channel connect backoff must not outlive the 100ms send interval")
                        .isPresent();
            } finally {
                server.shutdownNow();
                server.awaitTermination(2, TimeUnit.SECONDS);
            }
        } finally {
            sender.close();
        }
    }
}
