package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that a mid-list IOException from {@link FileRaftLog#append} leaves
 * the log in a consistent state — the on-disk channel is truncated back to
 * its pre-append length and the in-memory index reflects that truncation.
 * Otherwise subsequent operations would see an index that claims entries the
 * disk doesn't have, producing cascading corruption on the next append.
 */
class FileRaftLogAppendAtomicityTest {

    @Test
    void rollsBackIndexAndChannelWhenMidListWriteFails(@TempDir Path dir) throws Exception {
        var path = dir.resolve("raft.log");
        var realChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        // Fail on the 3rd call to write(ByteBuffer) — append() loops with
        // write-until-drained inside writeFrame, but each frame is a single
        // write-ByteBuffer call for small payloads, so entry 0 succeeds,
        // entry 1 succeeds, entry 2 throws.
        var channel = new FailOnNthWriteChannel(realChannel, 3);
        var log = FileRaftLog.openWithChannel(channel);

        var entries = List.of(
                new LogEntry(1, new Term(1), LogEntry.Type.NORMAL, new byte[] {1}),
                new LogEntry(2, new Term(1), LogEntry.Type.NORMAL, new byte[] {2}),
                new LogEntry(3, new Term(1), LogEntry.Type.NORMAL, new byte[] {3}));

        assertThatThrownBy(() -> log.append(entries)).isInstanceOf(IllegalStateException.class);

        assertThat(log.lastIndex()).isEqualTo(0L);
        assertThat(channel.size()).isEqualTo(0L);

        log.close();
    }

    private static final class FailOnNthWriteChannel extends FileChannel {
        private final FileChannel delegate;
        private final int failOnCall;
        private int writeCalls;

        FailOnNthWriteChannel(FileChannel delegate, int failOnCall) {
            this.delegate = delegate;
            this.failOnCall = failOnCall;
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            writeCalls++;
            if (writeCalls == failOnCall) {
                throw new IOException("injected write failure on call " + writeCalls);
            }
            return delegate.write(src);
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            return delegate.read(dst);
        }

        @Override
        public long position() throws IOException {
            return delegate.position();
        }

        @Override
        public FileChannel position(long newPosition) throws IOException {
            delegate.position(newPosition);
            return this;
        }

        @Override
        public long size() throws IOException {
            return delegate.size();
        }

        @Override
        public FileChannel truncate(long size) throws IOException {
            delegate.truncate(size);
            return this;
        }

        @Override
        public void force(boolean metaData) throws IOException {
            delegate.force(metaData);
        }

        @Override
        protected void implCloseChannel() throws IOException {
            delegate.close();
        }

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(ByteBuffer dst, long position) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int write(ByteBuffer src, long position) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MappedByteBuffer map(MapMode mode, long position, long size) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileLock lock(long position, long size, boolean shared) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long transferTo(long position, long count, WritableByteChannel target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long transferFrom(ReadableByteChannel src, long position, long count) {
            throw new UnsupportedOperationException();
        }
    }
}
