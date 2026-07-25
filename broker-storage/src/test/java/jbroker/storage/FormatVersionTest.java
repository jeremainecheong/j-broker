package jbroker.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FormatVersionTest {

    private static Path marker(Path dir) {
        return dir.resolve(FormatVersion.FILE_NAME);
    }

    private static Path tmp(Path dir) {
        return dir.resolve(FormatVersion.FILE_NAME + ".tmp");
    }

    @Test
    void freshDirIsStampedWithCurrent(@TempDir Path dir) throws Exception {
        assertThat(FormatVersion.check(dir)).isEqualTo(FormatVersion.CURRENT);
        assertThat(marker(dir)).exists();
        assertThat(Files.readString(marker(dir)).trim()).isEqualTo(Integer.toString(FormatVersion.CURRENT));
        // The temp file never outlives a successful stamp.
        assertThat(tmp(dir)).doesNotExist();
    }

    @Test
    void markerEqualToCurrentReopens(@TempDir Path dir) throws Exception {
        FormatVersion.check(dir);
        assertThat(FormatVersion.check(dir)).isEqualTo(FormatVersion.CURRENT);
    }

    @Test
    void olderMarkerProceedsWithoutRestamping(@TempDir Path dir) throws Exception {
        Files.writeString(marker(dir), "1\n");
        // This binary proceeds on an older marker and leaves it alone:
        // only a completed migration may move it forward, so a crash before
        // that still rolls back cleanly to the older binary.
        assertThat(FormatVersion.check(dir)).isEqualTo(1);
        assertThat(Files.readString(marker(dir))).isEqualTo("1\n");
    }

    @Test
    void newerMarkerRefuses(@TempDir Path dir) throws Exception {
        int newer = FormatVersion.CURRENT + 1;
        Files.writeString(marker(dir), newer + "\n");
        assertThatThrownBy(() -> FormatVersion.check(dir))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("format " + newer)
                .hasMessageContaining("format " + FormatVersion.CURRENT)
                .hasMessageContaining("newer broker")
                .hasMessageContaining("upgrade the binary or restore from backup");
        // Refusal never rewrites operator data.
        assertThat(Files.readString(marker(dir))).isEqualTo(newer + "\n");
    }

    @Test
    void downgradedBinaryRefusesATransactionsMarker(@TempDir Path dir) throws Exception {
        // The point of the control-batch gate: once a directory is stamped
        // with the transactions format, a binary from before control
        // batches (current = 1) must refuse it at open.
        Files.writeString(marker(dir), FormatVersion.TRANSACTIONS + "\n");
        assertThatThrownBy(() -> FormatVersion.check(dir, 1))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("format " + FormatVersion.TRANSACTIONS)
                .hasMessageContaining("newer broker");
    }

    @Test
    void ensureAtLeastRaisesAnOlderMarker(@TempDir Path dir) throws Exception {
        Files.writeString(marker(dir), "1\n");
        assertThat(FormatVersion.ensureAtLeast(dir, FormatVersion.TRANSACTIONS)).isEqualTo(FormatVersion.TRANSACTIONS);
        assertThat(Files.readString(marker(dir)).trim()).isEqualTo(Integer.toString(FormatVersion.TRANSACTIONS));
    }

    @Test
    void ensureAtLeastLeavesAnEqualOrNewerMarkerAlone(@TempDir Path dir) throws Exception {
        Files.writeString(marker(dir), FormatVersion.TRANSACTIONS + "\n");
        assertThat(FormatVersion.ensureAtLeast(dir, FormatVersion.TRANSACTIONS)).isEqualTo(FormatVersion.TRANSACTIONS);
        assertThat(Files.readString(marker(dir))).isEqualTo(FormatVersion.TRANSACTIONS + "\n");
    }

    @Test
    void ensureAtLeastStampsAFreshDir(@TempDir Path dir) throws Exception {
        assertThat(FormatVersion.ensureAtLeast(dir, FormatVersion.TRANSACTIONS)).isEqualTo(FormatVersion.CURRENT);
        assertThat(Files.readString(marker(dir)).trim()).isEqualTo(Integer.toString(FormatVersion.CURRENT));
    }

    @Test
    void ensureAtLeastRefusesVersionsThisBinaryCannotWrite(@TempDir Path dir) {
        assertThatThrownBy(() -> FormatVersion.ensureAtLeast(dir, FormatVersion.CURRENT + 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only writes up to " + FormatVersion.CURRENT);
    }

    @Test
    void ensureAtLeastStillRefusesUnreadableMarkers(@TempDir Path dir) throws Exception {
        Files.writeString(marker(dir), "garbage\n");
        assertThatThrownBy(() -> FormatVersion.ensureAtLeast(dir, FormatVersion.TRANSACTIONS))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unreadable format marker");
        assertThat(Files.readString(marker(dir))).isEqualTo("garbage\n");
    }

    @Test
    void emptyMarkerRefusesWithoutOverwriting(@TempDir Path dir) throws Exception {
        Files.writeString(marker(dir), "");
        assertThatThrownBy(() -> FormatVersion.check(dir))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unreadable format marker")
                .hasMessageContaining("inspect the file or restore from backup");
        assertThat(Files.readString(marker(dir))).isEmpty();
    }

    @Test
    void garbageMarkerRefusesWithoutOverwriting(@TempDir Path dir) throws Exception {
        Files.writeString(marker(dir), "not-a-version\n");
        assertThatThrownBy(() -> FormatVersion.check(dir))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unreadable format marker")
                .hasMessageContaining("not-a-version");
        assertThat(Files.readString(marker(dir))).isEqualTo("not-a-version\n");
    }

    @Test
    void nonPositiveMarkerRefuses(@TempDir Path dir) throws Exception {
        Files.writeString(marker(dir), "0\n");
        assertThatThrownBy(() -> FormatVersion.check(dir))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unreadable format marker")
                .hasMessageContaining("version 0");
    }

    @Test
    void staleTempFromCrashedStampIsDiscarded(@TempDir Path dir) throws Exception {
        // A crash between the temp write and the atomic move leaves a
        // (possibly partial) temp file and no marker. The next open must
        // treat the directory as unstamped and stamp it cleanly.
        Files.writeString(tmp(dir), "gar");
        assertThat(FormatVersion.check(dir)).isEqualTo(FormatVersion.CURRENT);
        assertThat(Files.readString(marker(dir)).trim()).isEqualTo(Integer.toString(FormatVersion.CURRENT));
        assertThat(tmp(dir)).doesNotExist();
    }

    @Test
    void staleTempNeverShadowsAnExistingMarker(@TempDir Path dir) throws Exception {
        Files.writeString(marker(dir), "1\n");
        Files.writeString(tmp(dir), "gar");
        assertThat(FormatVersion.check(dir)).isEqualTo(1);
        assertThat(Files.readString(marker(dir))).isEqualTo("1\n");
        assertThat(tmp(dir)).doesNotExist();
    }
}
