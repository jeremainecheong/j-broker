package jbroker.bench;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class SoakLedgerTest {

    @Test
    void cleanRunReportsNoLossNoDuplicates() {
        var r = SoakLedger.compare(List.of("p0-0", "p0-1", "p1-0"), List.of("p0-0", "p0-1", "p1-0"));
        assertThat(r.missing()).isEmpty();
        assertThat(r.duplicated()).isEmpty();
        assertThat(r.clean()).isTrue();
    }

    @Test
    void ackedButNeverConsumedIsLoss() {
        var r = SoakLedger.compare(List.of("p0-0", "p0-1"), List.of("p0-0"));
        assertThat(r.missing()).containsExactly("p0-1");
        assertThat(r.clean()).isFalse();
    }

    @Test
    void consumedTwiceIsDuplication() {
        var r = SoakLedger.compare(List.of("p0-0", "p0-1"), List.of("p0-0", "p0-1", "p0-1"));
        assertThat(r.duplicated()).containsExactly("p0-1");
        assertThat(r.clean()).isFalse();
    }

    @Test
    void unackedRecordInLogIsNotAViolation() {
        // A produce whose ack never reached the client may still have
        // committed — its presence exactly once is fine; only acked-and-
        // missing (loss) or present-more-than-once (duplication) violate
        // the soak loss/duplication invariant.
        var r = SoakLedger.compare(List.of("p0-0"), List.of("p0-0", "p0-99"));
        assertThat(r.missing()).isEmpty();
        assertThat(r.duplicated()).isEmpty();
        assertThat(r.clean()).isTrue();
    }

    @Test
    void unackedRecordAppearingTwiceIsStillDuplication() {
        var r = SoakLedger.compare(List.of("p0-0"), List.of("p0-0", "p0-99", "p0-99"));
        assertThat(r.duplicated()).containsExactly("p0-99");
        assertThat(r.clean()).isFalse();
    }
}
