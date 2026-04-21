package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileRaftLogFsyncTest {

    @Test
    void entriesPersistAcrossReopen(@TempDir Path dir) throws Exception {
        var path = dir.resolve("raft.log");
        try (var log = FileRaftLog.open(path)) {
            log.append(List.of(
                    new LogEntry(1, new Term(1), LogEntry.Type.NORMAL, new byte[] {1, 2, 3}),
                    new LogEntry(2, new Term(1), LogEntry.Type.NORMAL, new byte[] {4})));
        }
        try (var reopened = FileRaftLog.open(path)) {
            assertThat(reopened.lastIndex()).isEqualTo(2L);
            var entries = reopened.read(1, 10);
            assertThat(entries).hasSize(2);
            assertThat(entries.get(0).payload()).containsExactly(1, 2, 3);
            assertThat(entries.get(1).payload()).containsExactly(4);
        }
    }
}
