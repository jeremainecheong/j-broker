package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class BrokerLivenessTest {

    @Test
    void lookupUnknownReturnsEmpty() {
        var l = new BrokerLiveness();
        assertThat(l.lastSignal(42)).isEmpty();
    }

    @Test
    void recordSignalPopulatesEntry() {
        var l = new BrokerLiveness();
        l.recordSignal(1, /*metadataOffset*/ 100L, /*nanos*/ 500L);
        assertThat(l.lastSignal(1))
                .contains(new BrokerLiveness.Signal(/*wallClockNanos*/ 500L, /*metadataOffset*/ 100L));
    }

    @Test
    void recordSignalOverwritesWithLaterSignal() {
        var l = new BrokerLiveness();
        l.recordSignal(1, 100L, 500L);
        l.recordSignal(1, 200L, 900L);
        assertThat(l.lastSignal(1)).contains(new BrokerLiveness.Signal(900L, 200L));
    }

    @Test
    void olderSignalIsIgnored() {
        // A late-arriving heartbeat with a smaller wall-clock than the
        // current entry must not clobber the fresher signal. Clock skew
        // between peers makes this non-hypothetical.
        var l = new BrokerLiveness();
        l.recordSignal(1, 100L, 1_000L);
        l.recordSignal(1, 50L, 500L);
        assertThat(l.lastSignal(1)).contains(new BrokerLiveness.Signal(1_000L, 100L));
    }

    @Test
    void knownBrokerIdsListsEveryBrokerWithASignal() {
        var l = new BrokerLiveness();
        l.recordSignal(1, 0L, 100L);
        l.recordSignal(2, 0L, 200L);
        l.recordSignal(3, 0L, 300L);
        assertThat(l.knownBrokerIds()).containsExactlyInAnyOrder(1, 2, 3);
    }
}
