package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileRaftLogTest {

    @Test
    void emptyLogHasZeroLastIndex(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("raft.log"))) {
            assertThat(log.lastIndex()).isZero();
        }
    }

    @Test
    void appendThenReadRoundtrips(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("raft.log"))) {
            var entries = List.of(
                    new LogEntry(1, new Term(1), LogEntry.Type.NORMAL, new byte[] {1}),
                    new LogEntry(2, new Term(1), LogEntry.Type.NORMAL, new byte[] {2, 3}));
            log.append(entries);

            assertThat(log.lastIndex()).isEqualTo(2L);
            assertThat(log.termAt(1)).contains(new Term(1));
            assertThat(log.termAt(2)).contains(new Term(1));
            assertThat(log.termAt(3)).isEmpty();

            var read = log.read(1, 10);
            assertThat(read).hasSize(2);
            assertThat(read.get(0).payload()).containsExactly(1);
            assertThat(read.get(1).payload()).containsExactly(2, 3);
        }
    }

    @Test
    void appendRejectsNonContiguousIndices(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("raft.log"))) {
            assertThatThrownBy(() -> log.append(List.of(new LogEntry(5, Term.ZERO, LogEntry.Type.NORMAL, new byte[0]))))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void truncateFromRemovesTail(@TempDir Path dir) throws Exception {
        try (var log = FileRaftLog.open(dir.resolve("raft.log"))) {
            log.append(List.of(
                    new LogEntry(1, new Term(1), LogEntry.Type.NORMAL, new byte[] {1}),
                    new LogEntry(2, new Term(1), LogEntry.Type.NORMAL, new byte[] {2}),
                    new LogEntry(3, new Term(2), LogEntry.Type.NORMAL, new byte[] {3})));
            log.truncateFrom(2);
            assertThat(log.lastIndex()).isEqualTo(1L);
            assertThat(log.termAt(2)).isEmpty();
        }
    }
}
