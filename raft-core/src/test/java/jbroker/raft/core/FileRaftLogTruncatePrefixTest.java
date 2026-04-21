package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Log-prefix compaction: {@link RaftLog#truncatePrefix(long, Term)} drops
 * entries strictly before {@code firstIndex} and records the last included
 * index/term so the log's logical indexing stays stable across restarts.
 */
class FileRaftLogTruncatePrefixTest {

    @Test
    void truncatePrefixDropsOldEntriesAndRetainsSuffix(@TempDir Path dir) throws Exception {
        var path = dir.resolve("log.bin");
        try (var log = FileRaftLog.open(path)) {
            log.append(List.of(
                    new LogEntry(1L, new Term(1), LogEntry.Type.NORMAL, new byte[] {1}),
                    new LogEntry(2L, new Term(1), LogEntry.Type.NORMAL, new byte[] {2}),
                    new LogEntry(3L, new Term(2), LogEntry.Type.NORMAL, new byte[] {3}),
                    new LogEntry(4L, new Term(2), LogEntry.Type.NORMAL, new byte[] {4})));

            log.truncatePrefix(3L, new Term(1));

            assertThat(log.lastIndex()).isEqualTo(4L);
            assertThat(log.firstIndex()).isEqualTo(3L);
            assertThat(log.lastIncludedIndex()).isEqualTo(2L);
            assertThat(log.lastIncludedTerm()).isEqualTo(new Term(1));
            assertThat(log.termAt(1L)).isEmpty();
            assertThat(log.termAt(2L)).contains(new Term(1));
            assertThat(log.termAt(3L)).contains(new Term(2));
            assertThat(log.read(3L, 10)).hasSize(2);
        }
    }

    @Test
    void truncatePrefixSurvivesRestart(@TempDir Path dir) throws Exception {
        var path = dir.resolve("log.bin");
        try (var log = FileRaftLog.open(path)) {
            log.append(List.of(
                    new LogEntry(1L, new Term(1), LogEntry.Type.NORMAL, new byte[] {1}),
                    new LogEntry(2L, new Term(1), LogEntry.Type.NORMAL, new byte[] {2}),
                    new LogEntry(3L, new Term(2), LogEntry.Type.NORMAL, new byte[] {3})));
            log.truncatePrefix(3L, new Term(1));
        }
        try (var reopened = FileRaftLog.open(path)) {
            assertThat(reopened.lastIndex()).isEqualTo(3L);
            assertThat(reopened.firstIndex()).isEqualTo(3L);
            assertThat(reopened.lastIncludedIndex()).isEqualTo(2L);
            assertThat(reopened.lastIncludedTerm()).isEqualTo(new Term(1));
            assertThat(reopened.termAt(3L)).contains(new Term(2));
        }
    }
}
