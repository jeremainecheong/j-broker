package jbroker.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ProducerStateSnapshotTest {

    @Test
    void roundTripsMultipleEntries(@TempDir Path dir) throws Exception {
        var snap = new ProducerStateSnapshot();
        snap.put(new ProducerStateSnapshot.Entry(42L, (short) 7, 10, 1000L));
        snap.put(new ProducerStateSnapshot.Entry(43L, (short) 1, 5, 2000L));

        var path = dir.resolve("producer-state");
        snap.writeTo(path);

        var loaded = ProducerStateSnapshot.readFrom(path);
        assertThat(loaded.entries()).hasSize(2);
        assertThat(loaded.entries().get(42L).lastOffset()).isEqualTo(1000L);
        assertThat(loaded.entries().get(43L).producerEpoch()).isEqualTo((short) 1);
    }

    @Test
    void readFromMissingFileReturnsEmpty(@TempDir Path dir) throws Exception {
        var loaded = ProducerStateSnapshot.readFrom(dir.resolve("does-not-exist"));
        assertThat(loaded.entries()).isEmpty();
    }
}
