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
        // A hypothetical newer binary proceeds and leaves the marker alone:
        // only a completed migration may move it forward, so a crash before
        // that still rolls back cleanly to the older binary.
        assertThat(FormatVersion.check(dir, 2)).isEqualTo(1);
        assertThat(Files.readString(marker(dir))).isEqualTo("1\n");
    }

    @Test
    void newerMarkerRefuses(@TempDir Path dir) throws Exception {
        Files.writeString(marker(dir), "2\n");
        assertThatThrownBy(() -> FormatVersion.check(dir))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("format 2")
                .hasMessageContaining("format 1")
                .hasMessageContaining("newer broker")
                .hasMessageContaining("upgrade the binary or restore from backup");
        // Refusal never rewrites operator data.
        assertThat(Files.readString(marker(dir))).isEqualTo("2\n");
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
