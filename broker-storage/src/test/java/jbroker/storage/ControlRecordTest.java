package jbroker.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

class ControlRecordTest {

    @Test
    void commitRoundTrips() {
        var marker = new ControlRecord(ControlRecord.Type.COMMIT, 7);
        var decoded = ControlRecord.decode(marker.encode());
        assertThat(decoded).isEqualTo(marker);
        assertThat(decoded.type()).isEqualTo(ControlRecord.Type.COMMIT);
        assertThat(decoded.coordinatorEpoch()).isEqualTo(7);
    }

    @Test
    void abortRoundTrips() {
        var marker = new ControlRecord(ControlRecord.Type.ABORT, Integer.MAX_VALUE);
        assertThat(ControlRecord.decode(marker.encode())).isEqualTo(marker);
    }

    @Test
    void encodedLengthIsFixed() {
        assertThat(new ControlRecord(ControlRecord.Type.COMMIT, 0).encode()).hasSize(ControlRecord.ENCODED_LENGTH);
        assertThat(new ControlRecord(ControlRecord.Type.ABORT, -1).encode()).hasSize(ControlRecord.ENCODED_LENGTH);
    }

    @Test
    void typeIdsArePinnedToTheDiskEncoding() {
        assertThat(ControlRecord.Type.COMMIT.id()).isZero();
        assertThat(ControlRecord.Type.ABORT.id()).isEqualTo(1);
        assertThat(ControlRecord.Type.fromId(0)).isEqualTo(ControlRecord.Type.COMMIT);
        assertThat(ControlRecord.Type.fromId(1)).isEqualTo(ControlRecord.Type.ABORT);
    }

    @Test
    void unknownTypeIdRefuses() {
        var bytes = new ControlRecord(ControlRecord.Type.COMMIT, 3).encode();
        bytes[1] = 9;
        assertThatThrownBy(() -> ControlRecord.decode(bytes))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown control record type id 9");
    }

    @Test
    void unknownVersionRefuses() {
        var bytes = new ControlRecord(ControlRecord.Type.ABORT, 3).encode();
        bytes[0] = 1;
        assertThatThrownBy(() -> ControlRecord.decode(bytes))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unsupported control record version 1");
    }

    @Test
    void wrongLengthAndNullValueRefuse() {
        var bytes = new ControlRecord(ControlRecord.Type.COMMIT, 3).encode();
        assertThatThrownBy(() -> ControlRecord.decode(Arrays.copyOf(bytes, 5)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("6 bytes");
        assertThatThrownBy(() -> ControlRecord.decode(Arrays.copyOf(bytes, 7)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("6 bytes");
        assertThatThrownBy(() -> ControlRecord.decode(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null");
    }

    @Test
    void nullTypeRefuses() {
        assertThatThrownBy(() -> new ControlRecord(null, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("type");
    }
}
