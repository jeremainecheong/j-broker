package jbroker.broker.replication;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.grpc.Status;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.nio.channels.UnresolvedAddressException;
import jbroker.proto.broker.ReplicaFetchRequest;
import org.junit.jupiter.api.Test;

/**
 * Channel-rebuild policy for the replica-fetch peer client (#115 defect 1:
 * DNS-wedged channels starved replication long after the peer returned).
 *
 * <p>{@code .invalid} is reserved by RFC 6761 — resolvers must return
 * NXDOMAIN, so the unresolvable-host case is deterministic with or without
 * network access.
 */
class ReplicaPeerClientRebuildTest {

    private static ReplicaFetchRequest anyFetch() {
        return ReplicaFetchRequest.newBuilder()
                .setTopic("orders")
                .setPartition(0)
                .setFollowerBrokerId(2)
                .setFetchOffset(0)
                .setMaxBytes(1024)
                .build();
    }

    @Test
    void unresolvableHostRebuildsChannelAfterThreshold() {
        try (var client = new ReplicaPeerClient("jbroker-peer.invalid", 9092)) {
            for (int i = 0; i < ReplicaPeerClient.REBUILD_AFTER_UNRESOLVABLE; i++) {
                assertThatThrownBy(() -> client.fetch(anyFetch(), 3_000))
                        .isInstanceOf(io.grpc.StatusRuntimeException.class);
            }
            assertThat(client.channelRebuilds()).isEqualTo(1);

            // A second full streak rebuilds again — the policy keeps working
            // for as long as the resolver stays wedged.
            for (int i = 0; i < ReplicaPeerClient.REBUILD_AFTER_UNRESOLVABLE; i++) {
                assertThatThrownBy(() -> client.fetch(anyFetch(), 3_000))
                        .isInstanceOf(io.grpc.StatusRuntimeException.class);
            }
            assertThat(client.channelRebuilds()).isEqualTo(2);
        }
    }

    @Test
    void connectionRefusedNeverRebuilds() throws Exception {
        int port;
        try (var sock = new ServerSocket(0)) {
            port = sock.getLocalPort();
        }
        // Port is closed now: the name resolves (loopback), the connect is
        // refused. That's "peer down", not "resolver wedged" — no rebuild.
        try (var client = new ReplicaPeerClient("127.0.0.1", port)) {
            for (int i = 0; i < ReplicaPeerClient.REBUILD_AFTER_UNRESOLVABLE + 2; i++) {
                assertThatThrownBy(() -> client.fetch(anyFetch(), 1_000))
                        .isInstanceOf(io.grpc.StatusRuntimeException.class);
            }
            assertThat(client.channelRebuilds()).isZero();
        }
    }

    @Test
    void nameResolutionFailureWalksCauseChains() {
        assertThat(ReplicaPeerClient.nameResolutionFailure(new UnknownHostException("broker2")))
                .isTrue();
        assertThat(ReplicaPeerClient.nameResolutionFailure(new RuntimeException(new UnresolvedAddressException())))
                .isTrue();
        // grpc surfaces resolver failures as UNAVAILABLE with the cause on
        // the Status, not on the exception itself.
        assertThat(ReplicaPeerClient.nameResolutionFailure(Status.UNAVAILABLE
                        .withCause(new UnknownHostException("broker2"))
                        .asRuntimeException()))
                .isTrue();
        assertThat(ReplicaPeerClient.nameResolutionFailure(Status.UNAVAILABLE
                        .withDescription("Unable to resolve host broker2")
                        .asRuntimeException()))
                .isTrue();

        assertThat(ReplicaPeerClient.nameResolutionFailure(Status.DEADLINE_EXCEEDED
                        .withDescription("deadline exceeded")
                        .asRuntimeException()))
                .isFalse();
        assertThat(ReplicaPeerClient.nameResolutionFailure(Status.UNAVAILABLE
                        .withDescription("io exception")
                        .withCause(new java.net.ConnectException("Connection refused"))
                        .asRuntimeException()))
                .isFalse();
    }
}
