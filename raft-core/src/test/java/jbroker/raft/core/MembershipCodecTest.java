package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class MembershipCodecTest {

    private static final NodeId N1 = new NodeId(1);
    private static final NodeId N2 = new NodeId(2);
    private static final NodeId N3 = new NodeId(3);
    private static final NodeId N4 = new NodeId(4);

    @Test
    void votersRoundTrip() {
        var m = new Membership(List.of(N1, N2, N3), List.of());
        assertThat(MembershipCodec.decodeMembership(MembershipCodec.encode(m))).isEqualTo(m);
    }

    @Test
    void votersAndLearnersRoundTrip() {
        var m = new Membership(List.of(N1, N2, N3), List.of(N4));
        var decoded = MembershipCodec.decodeMembership(MembershipCodec.encode(m));
        assertThat(decoded.voters()).containsExactly(N1, N2, N3);
        assertThat(decoded.learners()).containsExactly(N4);
    }

    @Test
    void legacyVoterListPayloadDecodesWithNoLearners() {
        // A CONFIG_CHANGE written before learners existed: bare
        // <count><ids...> with no version sentinel. Must still decode so
        // logs from before this change replay correctly.
        byte[] legacy = MembershipCodec.encode(List.of(N1, N2, N3));

        var decoded = MembershipCodec.decodeMembership(legacy);

        assertThat(decoded.voters()).containsExactly(N1, N2, N3);
        assertThat(decoded.learners()).isEmpty();
    }

    @Test
    void encodeVoterListAndDecodeVotersStayCompatible() {
        // The old encode(List<NodeId>) / decode(byte[]) → List<NodeId> pair
        // keeps working for call sites that only care about voters.
        byte[] payload = MembershipCodec.encode(new Membership(List.of(N1, N2), List.of(N3)));
        assertThat(MembershipCodec.decode(payload)).containsExactly(N1, N2);
    }

    @Test
    void corruptPayloadRejected() {
        assertThatThrownBy(() -> MembershipCodec.decodeMembership(new byte[] {0x01, 0x02}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void membershipRejectsANodeThatIsBothVoterAndLearner() {
        assertThatThrownBy(() -> new Membership(List.of(N1, N2), List.of(N2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("both");
    }
}
