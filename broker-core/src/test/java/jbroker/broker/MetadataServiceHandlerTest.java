package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import jbroker.proto.broker.DescribeClusterRequest;
import jbroker.proto.common.ErrorCode;
import org.junit.jupiter.api.Test;

class MetadataServiceHandlerTest {

    private static final long NOW_NANOS = 1_000_000_000L;
    private static final long STALENESS = TimeUnit.SECONDS.toNanos(3);

    @Test
    void describeCluster_returnsControllerIdFromLeaderSupplier() {
        var reg = new BrokerRegistry();
        reg.onBrokerRegistration(1, "h1", 9092);
        reg.onBrokerRegistration(2, "h2", 9092);
        var liveness = new BrokerLiveness();
        var handler = new MetadataServiceHandler(
                1,
                reg,
                liveness,
                () -> "LEADER",
                () -> Optional.of(2),
                () -> 7L,
                () -> 42L,
                () -> NOW_NANOS,
                STALENESS);

        var resp = handler.describeCluster(DescribeClusterRequest.newBuilder().build());

        assertThat(resp.getError()).isEqualTo(ErrorCode.OK);
        assertThat(resp.getControllerId()).isEqualTo(2);
        assertThat(resp.getCurrentTerm()).isEqualTo(7L);
        assertThat(resp.getMetadataOffset()).isEqualTo(42L);
    }

    @Test
    void describeCluster_controllerIdIsMinusOneWhenNoLeaderObserved() {
        var handler = new MetadataServiceHandler(
                1,
                new BrokerRegistry(),
                new BrokerLiveness(),
                () -> "FOLLOWER",
                Optional::empty,
                () -> 0L,
                () -> 0L,
                () -> NOW_NANOS,
                STALENESS);

        var resp = handler.describeCluster(DescribeClusterRequest.newBuilder().build());

        assertThat(resp.getControllerId()).isEqualTo(-1);
        // Fallback to self when registry is empty so the UI never sees a
        // 0-length node list.
        assertThat(resp.getNodesCount()).isEqualTo(1);
        assertThat(resp.getNodes(0).getBrokerId()).isEqualTo(1);
    }

    @Test
    void describeCluster_selfEntryReportsOwnRoleAndIsAlwaysAlive() {
        var reg = new BrokerRegistry();
        reg.onBrokerRegistration(1, "self-host", 9192);
        var handler = new MetadataServiceHandler(
                1,
                reg,
                new BrokerLiveness(),
                () -> "LEADER",
                () -> Optional.of(1),
                () -> 3L,
                () -> 0L,
                () -> NOW_NANOS,
                STALENESS);

        var self = handler.describeCluster(DescribeClusterRequest.newBuilder().build())
                .getNodes(0);

        assertThat(self.getBrokerId()).isEqualTo(1);
        assertThat(self.getRole()).isEqualTo("LEADER");
        assertThat(self.getAlive()).isTrue();
        assertThat(self.getHost()).isEqualTo("self-host");
        assertThat(self.getPort()).isEqualTo(9192);
    }

    @Test
    void describeCluster_peerAliveWhenLivenessFresh_staleWhenOld() {
        var reg = new BrokerRegistry();
        reg.onBrokerRegistration(1, "h1", 9092);
        reg.onBrokerRegistration(2, "h2", 9092);
        var liveness = new BrokerLiveness();
        // Peer 2 heartbeat landed 1s ago — within staleness threshold.
        liveness.recordSignal(2, /*metadataOffset*/ 0L, NOW_NANOS - TimeUnit.SECONDS.toNanos(1));

        var handler = new MetadataServiceHandler(
                1, reg, liveness, () -> "LEADER", () -> Optional.of(1), () -> 1L, () -> 0L, () -> NOW_NANOS, STALENESS);

        var peer = handler.describeCluster(DescribeClusterRequest.newBuilder().build())
                .getNodes(1);
        assertThat(peer.getBrokerId()).isEqualTo(2);
        assertThat(peer.getAlive()).isTrue();

        // Move the clock past the staleness window — same peer now
        // declared dead.
        var clock = new AtomicLong(NOW_NANOS + TimeUnit.SECONDS.toNanos(10));
        var handlerLater = new MetadataServiceHandler(
                1, reg, liveness, () -> "LEADER", () -> Optional.of(1), () -> 1L, () -> 0L, clock::get, STALENESS);
        var peerLater = handlerLater
                .describeCluster(DescribeClusterRequest.newBuilder().build())
                .getNodes(1);
        assertThat(peerLater.getAlive()).isFalse();
    }

    @Test
    void describeCluster_peerWithNoHeartbeatYetIsDead() {
        var reg = new BrokerRegistry();
        reg.onBrokerRegistration(1, "h1", 9092);
        reg.onBrokerRegistration(2, "h2", 9092);
        var handler = new MetadataServiceHandler(
                1,
                reg,
                new BrokerLiveness(),
                () -> "LEADER",
                () -> Optional.of(1),
                () -> 1L,
                () -> 0L,
                () -> NOW_NANOS,
                STALENESS);

        var peer = handler.describeCluster(DescribeClusterRequest.newBuilder().build())
                .getNodes(1);
        assertThat(peer.getAlive()).isFalse();
        assertThat(peer.getLastSeenMillis()).isEqualTo(0L);
    }

    @Test
    void describeCluster_peersReportRoleUnknown() {
        var reg = new BrokerRegistry();
        reg.onBrokerRegistration(1, "h1", 9092);
        reg.onBrokerRegistration(2, "h2", 9092);
        var liveness = new BrokerLiveness();
        liveness.recordSignal(2, 0L, NOW_NANOS);

        var handler = new MetadataServiceHandler(
                1, reg, liveness, () -> "LEADER", () -> Optional.of(2), () -> 1L, () -> 0L, () -> NOW_NANOS, STALENESS);

        var resp = handler.describeCluster(DescribeClusterRequest.newBuilder().build());
        var peer = resp.getNodes(1);
        assertThat(peer.getBrokerId()).isEqualTo(2);
        assertThat(peer.getRole()).isEqualTo("UNKNOWN");
    }
}
