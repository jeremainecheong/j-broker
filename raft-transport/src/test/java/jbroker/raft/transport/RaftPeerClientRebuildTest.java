package jbroker.raft.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.ServerSocket;
import jbroker.proto.raft.AppendEntriesRequest;
import jbroker.raft.core.NodeId;
import org.junit.jupiter.api.Test;

/**
 * Channel-rebuild policy for the Raft peer client (#115 defect 1: a
 * DNS-wedged channel meant a voter that never rejoined elections or
 * replication after a container restart).
 *
 * <p>{@code .invalid} is reserved by RFC 6761 — resolvers must return
 * NXDOMAIN, so the unresolvable-host case is deterministic with or without
 * network access.
 */
class RaftPeerClientRebuildTest {

    private static AppendEntriesRequest anyAppend() {
        return AppendEntriesRequest.newBuilder()
                .setTerm(1)
                .setLeaderId(1)
                .setPrevLogIndex(0)
                .setPrevLogTerm(0)
                .setLeaderCommit(0)
                .build();
    }

    @Test
    void unresolvableHostRebuildsChannelAfterThreshold() {
        try (var client = new RaftPeerClient(new NodeId(2), "jbroker-peer.invalid", 9192)) {
            for (int i = 0; i < RaftPeerClient.REBUILD_AFTER_UNRESOLVABLE; i++) {
                assertThatThrownBy(() -> client.appendEntries(anyAppend()))
                        .isInstanceOf(io.grpc.StatusRuntimeException.class);
            }
            assertThat(client.channelRebuilds()).isEqualTo(1);

            for (int i = 0; i < RaftPeerClient.REBUILD_AFTER_UNRESOLVABLE; i++) {
                assertThatThrownBy(() -> client.appendEntries(anyAppend()))
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
        try (var client = new RaftPeerClient(new NodeId(2), "127.0.0.1", port)) {
            for (int i = 0; i < RaftPeerClient.REBUILD_AFTER_UNRESOLVABLE + 2; i++) {
                assertThatThrownBy(() -> client.appendEntries(anyAppend()))
                        .isInstanceOf(io.grpc.StatusRuntimeException.class);
            }
            assertThat(client.channelRebuilds()).isZero();
        }
    }
}
