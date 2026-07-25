package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class BrokerRegistryRackTest {

    @Test
    void rackDefaultsToEmptyForUnknownBrokers() {
        var registry = new BrokerRegistry();

        assertThat(registry.rackFor(7)).isEmpty();
        assertThat(registry.racks()).isEmpty();
    }

    @Test
    void notedRackIsReadableAndSnapshotted() {
        var registry = new BrokerRegistry();

        registry.noteRack(1, "zone-a");
        registry.noteRack(2, "zone-b");

        assertThat(registry.rackFor(1)).isEqualTo("zone-a");
        assertThat(registry.racks()).containsOnlyKeys(1, 2).containsEntry(2, "zone-b");
    }

    @Test
    void blankRackClearsAPreviousLabel() {
        // A broker restarted without a rack must shed its old label —
        // stale rack info would mislead placement into a fake spread.
        var registry = new BrokerRegistry();
        registry.noteRack(1, "zone-a");

        registry.noteRack(1, "");

        assertThat(registry.rackFor(1)).isEmpty();
        assertThat(registry.racks()).isEmpty();
    }

    @Test
    void reDeclarationOverwrites() {
        var registry = new BrokerRegistry();
        registry.noteRack(1, "zone-a");

        registry.noteRack(1, "zone-b");

        assertThat(registry.rackFor(1)).isEqualTo("zone-b");
    }
}
