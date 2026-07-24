package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import jbroker.proto.broker.ApiVersionsRequest;
import jbroker.proto.broker.DescribeClusterRequest;
import jbroker.proto.common.ErrorCode;
import org.junit.jupiter.api.Test;

class MetadataServiceHandlerApiVersionsTest {

    private static final long NOW_NANOS = 1_000_000_000L;
    private static final long STALENESS = TimeUnit.SECONDS.toNanos(3);

    private static MetadataServiceHandler handler(BrokerRegistry reg, BrokerLiveness liveness) {
        return new MetadataServiceHandler(
                1, reg, liveness, () -> "LEADER", () -> Optional.of(1), () -> 1L, () -> 0L, () -> NOW_NANOS, STALENESS);
    }

    @Test
    void apiVersions_answersSupportedRangeAndBrokerVersion() {
        var resp = handler(new BrokerRegistry(), new BrokerLiveness())
                .apiVersions(ApiVersionsRequest.newBuilder().build());

        assertThat(resp.getError()).isEqualTo(ErrorCode.OK);
        assertThat(resp.getMinProtocolVersion()).isEqualTo(ProtocolVersion.MIN_SUPPORTED);
        assertThat(resp.getMaxProtocolVersion()).isEqualTo(ProtocolVersion.CURRENT);
        assertThat(resp.getBrokerVersion()).isEqualTo(ProtocolVersion.BROKER_VERSION);
    }

    @Test
    void describeCluster_selfEntryCarriesOwnProtocolRange() {
        var reg = new BrokerRegistry();
        reg.onBrokerRegistration(1, "h1", 9092);

        var self = handler(reg, new BrokerLiveness())
                .describeCluster(DescribeClusterRequest.newBuilder().build())
                .getNodes(0);

        assertThat(self.hasSupportedProtocolMin()).isTrue();
        assertThat(self.getSupportedProtocolMin()).isEqualTo(ProtocolVersion.MIN_SUPPORTED);
        assertThat(self.getSupportedProtocolMax()).isEqualTo(ProtocolVersion.CURRENT);
    }

    @Test
    void describeCluster_peerRangeComesFromItsLastHeartbeat() {
        var reg = new BrokerRegistry();
        reg.onBrokerRegistration(1, "h1", 9092);
        reg.onBrokerRegistration(2, "h2", 9092);
        var liveness = new BrokerLiveness();
        liveness.recordSignal(2, 0L, NOW_NANOS);
        liveness.recordProtocolRange(2, 1, 1);

        var peer = handler(reg, liveness)
                .describeCluster(DescribeClusterRequest.newBuilder().build())
                .getNodes(1);

        assertThat(peer.getBrokerId()).isEqualTo(2);
        assertThat(peer.hasSupportedProtocolMin()).isTrue();
        assertThat(peer.getSupportedProtocolMin()).isEqualTo(1);
        assertThat(peer.getSupportedProtocolMax()).isEqualTo(1);
    }

    @Test
    void describeCluster_neverHeardPeerLeavesRangeAbsent() {
        var reg = new BrokerRegistry();
        reg.onBrokerRegistration(1, "h1", 9092);
        reg.onBrokerRegistration(2, "h2", 9092);

        var peer = handler(reg, new BrokerLiveness())
                .describeCluster(DescribeClusterRequest.newBuilder().build())
                .getNodes(1);

        assertThat(peer.getBrokerId()).isEqualTo(2);
        assertThat(peer.hasSupportedProtocolMin()).isFalse();
        assertThat(peer.hasSupportedProtocolMax()).isFalse();
    }
}
