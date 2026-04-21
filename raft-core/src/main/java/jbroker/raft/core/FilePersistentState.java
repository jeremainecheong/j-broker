package jbroker.raft.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

/**
 * File-backed persistent state. Format: 8-byte term, 4-byte votedFor node id
 * (-1 = none). Rewrites the whole file + fsyncs on every update.
 */
public final class FilePersistentState implements PersistentState, AutoCloseable {

    private static final int SIZE = Long.BYTES + Integer.BYTES;
    private static final int NO_VOTE = -1;

    private final FileChannel channel;
    private Term currentTerm;
    private Optional<NodeId> votedFor;

    private FilePersistentState(FileChannel channel, Term term, Optional<NodeId> vote) {
        this.channel = channel;
        this.currentTerm = term;
        this.votedFor = vote;
    }

    public static FilePersistentState open(Path path) throws IOException {
        var channel =
                FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        Term term = Term.ZERO;
        Optional<NodeId> vote = Optional.empty();
        if (channel.size() >= SIZE) {
            var buf = ByteBuffer.allocate(SIZE);
            channel.position(0);
            channel.read(buf);
            buf.flip();
            long t = buf.getLong();
            int v = buf.getInt();
            term = new Term(t);
            vote = v == NO_VOTE ? Optional.empty() : Optional.of(new NodeId(v));
        }
        return new FilePersistentState(channel, term, vote);
    }

    @Override
    public synchronized Term currentTerm() {
        return currentTerm;
    }

    @Override
    public synchronized Optional<NodeId> votedFor() {
        return votedFor;
    }

    @Override
    public synchronized void update(Term term, Optional<NodeId> vote) {
        try {
            var buf = ByteBuffer.allocate(SIZE);
            buf.putLong(term.value());
            buf.putInt(vote.map(NodeId::value).orElse(NO_VOTE));
            buf.flip();
            channel.position(0);
            while (buf.hasRemaining()) {
                channel.write(buf);
            }
            channel.force(true);
            this.currentTerm = term;
            this.votedFor = vote;
        } catch (IOException e) {
            throw new IllegalStateException("failed to persist state", e);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        channel.close();
    }
}
