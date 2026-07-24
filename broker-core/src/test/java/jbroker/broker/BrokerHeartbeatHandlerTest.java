package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import jbroker.proto.broker.BrokerHeartbeatRequest;
import org.junit.jupiter.api.Test;

class BrokerHeartbeatHandlerTest {

    @Test
    void handlingHeartbeatUpdatesLiveness() {
        var liveness = new BrokerLiveness();
        var clock = new long[] {1_000L};
        var handler = new BrokerHeartbeatHandler(liveness, () -> clock[0]);

        var resp = handler.handle(BrokerHeartbeatRequest.newBuilder()
                .setBrokerId(3)
                .setCurrentMetadataOffset(42L)
                .build());

        assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NONE);
        assertThat(liveness.lastSignal(3)).contains(new BrokerLiveness.Signal(1_000L, 42L));
    }

    @Test
    void laterHeartbeatAdvancesLiveness() {
        var liveness = new BrokerLiveness();
        var clock = new long[] {1_000L};
        var handler = new BrokerHeartbeatHandler(liveness, () -> clock[0]);

        handler.handle(BrokerHeartbeatRequest.newBuilder()
                .setBrokerId(3)
                .setCurrentMetadataOffset(10L)
                .build());
        clock[0] = 2_500L;
        handler.handle(BrokerHeartbeatRequest.newBuilder()
                .setBrokerId(3)
                .setCurrentMetadataOffset(20L)
                .build());

        assertThat(liveness.lastSignal(3)).contains(new BrokerLiveness.Signal(2_500L, 20L));
    }

    @Test
    void heartbeatCarryingProtocolRangeRecordsIt() {
        var liveness = new BrokerLiveness();
        var handler = new BrokerHeartbeatHandler(liveness, () -> 1_000L);

        handler.handle(BrokerHeartbeatRequest.newBuilder()
                .setBrokerId(3)
                .setSupportedProtocolMin(1)
                .setSupportedProtocolMax(1)
                .build());

        assertThat(liveness.protocolRange(3)).contains(new BrokerLiveness.ProtocolRange(1, 1));
    }

    @Test
    void heartbeatWithoutProtocolRangeLeavesRangeUnknown() {
        var liveness = new BrokerLiveness();
        var handler = new BrokerHeartbeatHandler(liveness, () -> 1_000L);

        // A sender predating version discovery leaves both fields at 0 —
        // the peer's range must stay unknown, not become a bogus 0/0.
        handler.handle(BrokerHeartbeatRequest.newBuilder()
                .setBrokerId(3)
                .setCurrentMetadataOffset(42L)
                .build());

        assertThat(liveness.lastSignal(3)).isPresent();
        assertThat(liveness.protocolRange(3)).isEmpty();
    }
}
