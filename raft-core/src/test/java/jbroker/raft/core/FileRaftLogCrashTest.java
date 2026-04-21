package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileRaftLogCrashTest {

    @Test
    void reopenRecoversFromTornTailingWrite(@TempDir Path dir) throws Exception {
        var path = dir.resolve("raft.log");
        try (var log = FileRaftLog.open(path)) {
            log.append(List.of(
                    new LogEntry(1, new Term(1), LogEntry.Type.NORMAL, new byte[] {1}),
                    new LogEntry(2, new Term(1), LogEntry.Type.NORMAL, new byte[] {2})));
        }

        // Simulate a torn trailing write: append 5 bytes of garbage past EOF,
        // representing an interrupted fsync/append.
        try (var raw = FileChannel.open(path, StandardOpenOption.WRITE)) {
            raw.position(raw.size());
            raw.write(java.nio.ByteBuffer.wrap(new byte[] {9, 9, 9, 9, 9}));
        }

        try (var reopened = FileRaftLog.open(path)) {
            assertThat(reopened.lastIndex()).isEqualTo(2L);
            assertThat(reopened.read(1, 10)).hasSize(2);
        }
    }

    @Test
    void reopenTruncatesFrameWithCorruptLargeLengthPrefix(@TempDir Path dir) throws Exception {
        var path = dir.resolve("raft.log");
        try (var log = FileRaftLog.open(path)) {
            log.append(List.of(new LogEntry(1, new Term(1), LogEntry.Type.NORMAL, new byte[] {1})));
        }

        // Append a 4-byte length prefix that's absurdly large, followed by some junk.
        try (var raw = FileChannel.open(path, StandardOpenOption.WRITE)) {
            raw.position(raw.size());
            var corrupt = java.nio.ByteBuffer.allocate(4 + 10);
            corrupt.putInt(Integer.MAX_VALUE);
            corrupt.put(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
            corrupt.flip();
            raw.write(corrupt);
        }

        try (var reopened = FileRaftLog.open(path)) {
            assertThat(reopened.lastIndex()).isEqualTo(1L);
            assertThat(reopened.read(1, 10)).hasSize(1);
        }
    }

    @Test
    void reopenTruncatesFrameWithNegativeLengthPrefix(@TempDir Path dir) throws Exception {
        var path = dir.resolve("raft.log");
        try (var log = FileRaftLog.open(path)) {
            log.append(List.of(new LogEntry(1, new Term(1), LogEntry.Type.NORMAL, new byte[] {1})));
        }

        try (var raw = FileChannel.open(path, StandardOpenOption.WRITE)) {
            raw.position(raw.size());
            var corrupt = java.nio.ByteBuffer.allocate(4);
            corrupt.putInt(-42);
            corrupt.flip();
            raw.write(corrupt);
        }

        try (var reopened = FileRaftLog.open(path)) {
            assertThat(reopened.lastIndex()).isEqualTo(1L);
            assertThat(reopened.read(1, 10)).hasSize(1);
        }
    }
}
