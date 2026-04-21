package jbroker.broker;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import jbroker.raft.core.LogEntry;
import jbroker.raft.core.StateMachine;

/**
 * Wraps a {@link StateMachine} and lets callers wait for a specific payload
 * to be applied. Used by {@link AdminHandler} to turn the fire-and-forget
 * {@code propose} path into a synchronous {@code createTopic} RPC.
 *
 * <p>Keyed on the full payload bytes — fine for our domain where metadata
 * records are content-unique (topic names, creation timestamps) but not a
 * general solution. A future iteration should thread an opaque request ID
 * through so replays don't collide.
 */
public final class WaitingStateMachine implements StateMachine {

    private final StateMachine delegate;
    private final ConcurrentHashMap<ByteBuffer, CompletableFuture<Void>> waiters = new ConcurrentHashMap<>();

    public WaitingStateMachine(StateMachine delegate) {
        this.delegate = delegate;
    }

    public CompletableFuture<Void> awaitApply(byte[] payload) {
        return waiters.computeIfAbsent(ByteBuffer.wrap(payload), k -> new CompletableFuture<>());
    }

    @Override
    public void apply(LogEntry entry) {
        delegate.apply(entry);
        var key = ByteBuffer.wrap(entry.payload());
        var fut = waiters.remove(key);
        if (fut != null) fut.complete(null);
    }

    @Override
    public void snapshot(OutputStream out) throws IOException {
        delegate.snapshot(out);
    }

    @Override
    public void restore(InputStream in) throws IOException {
        delegate.restore(in);
    }
}
