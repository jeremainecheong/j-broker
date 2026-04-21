package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ProducerIdRegistryTest {

    @Test
    void startsAtOneAndAllocatesMonotonicallyIncreasingIds() {
        // Counter starts at 1 so producer_id = 0 stays reserved as the
        // proto3-default "unassigned" sentinel.
        var reg = new ProducerIdRegistry();
        assertThat(reg.peekNextProducerId()).isEqualTo(1L);
        assertThat(reg.allocateNext()).isEqualTo(1L);
        assertThat(reg.allocateNext()).isEqualTo(2L);
        assertThat(reg.allocateNext()).isEqualTo(3L);
        assertThat(reg.peekNextProducerId()).isEqualTo(4L);
    }

    @Test
    void applyAssignmentAdvancesCounterButNeverRegresses() {
        var reg = new ProducerIdRegistry();
        reg.applyAssignment(5L);
        assertThat(reg.peekNextProducerId()).isEqualTo(5L);

        // Stale or duplicate replay must not regress the counter.
        reg.applyAssignment(3L);
        assertThat(reg.peekNextProducerId()).isEqualTo(5L);

        reg.applyAssignment(10L);
        assertThat(reg.peekNextProducerId()).isEqualTo(10L);
    }
}
