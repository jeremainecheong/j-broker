package jbroker.broker.txn;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TxnPartitionEpochsTest {

    @Test
    void unknownProducerHasNoFloor() {
        var epochs = new TxnPartitionEpochs();
        assertThat(epochs.maxEpochOf("orders", 0, 42L)).isEqualTo(-1);
    }

    @Test
    void observationsAreMonotonePerPartitionAndProducer() {
        var epochs = new TxnPartitionEpochs();
        epochs.observe("orders", 0, 42L, 3);
        epochs.observe("orders", 0, 42L, 1); // stale observation must not lower the floor
        assertThat(epochs.maxEpochOf("orders", 0, 42L)).isEqualTo(3);
        epochs.observe("orders", 0, 42L, 7);
        assertThat(epochs.maxEpochOf("orders", 0, 42L)).isEqualTo(7);
    }

    @Test
    void partitionsAndProducersAreIndependent() {
        var epochs = new TxnPartitionEpochs();
        epochs.observe("orders", 0, 42L, 5);
        assertThat(epochs.maxEpochOf("orders", 1, 42L)).isEqualTo(-1);
        assertThat(epochs.maxEpochOf("orders", 0, 43L)).isEqualTo(-1);
        assertThat(epochs.maxEpochOf("other", 0, 42L)).isEqualTo(-1);
    }

    @Test
    void evictTopicDropsOnlyThatTopic() {
        var epochs = new TxnPartitionEpochs();
        epochs.observe("orders", 0, 42L, 5);
        // A topic whose name shares a prefix must survive the eviction.
        epochs.observe("orders-archive", 0, 42L, 9);
        epochs.evictTopic("orders");
        assertThat(epochs.maxEpochOf("orders", 0, 42L)).isEqualTo(-1);
        assertThat(epochs.maxEpochOf("orders-archive", 0, 42L)).isEqualTo(9);
    }
}
