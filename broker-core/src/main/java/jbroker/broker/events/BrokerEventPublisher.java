package jbroker.broker.events;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * P8.6 — per-broker event publisher. Maintains an in-process ring buffer
 * of the last 2048 events (keyed by monotonic id) plus a list of live
 * subscribers. On {@link #publish} the event is appended to the ring and
 * fanned out to every subscriber's queue.
 *
 * <p>Admin-app's server-streaming {@code SubscribeEvents} RPC translates
 * each subscriber's queue into a gRPC stream; the SSE controller builds on
 * top of that.
 *
 * <p>Thread-safety: {@link AtomicLong} guarantees monotonic id allocation;
 * the ring array is modulo-indexed under the lock held for the brief
 * insert window. Subscribers are a {@link CopyOnWriteArrayList}; each
 * subscriber owns its own concurrent queue so slow subscribers can't block
 * publish.
 */
public final class BrokerEventPublisher {

    private static final int DEFAULT_CAPACITY = 2048;

    public interface Subscription extends AutoCloseable {
        /** Poll the next event; returns {@code null} if the subscriber has been closed. */
        BrokerEvent take() throws InterruptedException;

        @Override
        void close();
    }

    private final int capacity;
    private final BrokerEvent[] ring;
    private final AtomicLong nextId = new AtomicLong(1);
    private final Object ringLock = new Object();
    private final CopyOnWriteArrayList<SubscriptionImpl> subscribers = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<String, Object> nop = new ConcurrentHashMap<>(0);

    public BrokerEventPublisher() {
        this(DEFAULT_CAPACITY);
    }

    public BrokerEventPublisher(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be > 0");
        this.capacity = capacity;
        this.ring = new BrokerEvent[capacity];
    }

    /** Allocate a monotonic id. Callers build the event with this id, then publish. */
    public long allocateId() {
        return nextId.getAndIncrement();
    }

    public void publish(BrokerEvent event) {
        synchronized (ringLock) {
            long id = event.id();
            ring[(int) (id % capacity)] = event;
        }
        for (var sub : subscribers) {
            sub.offer(event);
        }
    }

    /**
     * Replay every event in the ring whose id is strictly greater than
     * {@code lastSeen}. Used by the SSE {@code Last-Event-ID} path. Returns
     * a list sorted by id ascending. Events older than the ring's tail are
     * not recoverable — caller must decide on retry/error.
     */
    public List<BrokerEvent> replayAfter(long lastSeen) {
        var out = new ArrayList<BrokerEvent>();
        synchronized (ringLock) {
            for (var e : ring) {
                if (e == null) continue;
                if (e.id() > lastSeen) out.add(e);
            }
        }
        out.sort((a, b) -> Long.compare(a.id(), b.id()));
        return out;
    }

    /** Current tail id (exclusive). Useful for tests. */
    public long highWatermarkId() {
        return nextId.get();
    }

    /**
     * Open a subscription. Events published from now on are queued for the
     * subscriber. Remember to {@link Subscription#close()} when done.
     */
    public Subscription subscribe() {
        var sub = new SubscriptionImpl();
        subscribers.add(sub);
        return sub;
    }

    private final class SubscriptionImpl implements Subscription {
        private final java.util.concurrent.LinkedBlockingQueue<BrokerEvent> queue =
                new java.util.concurrent.LinkedBlockingQueue<>();
        private volatile boolean closed;

        void offer(BrokerEvent event) {
            if (closed) return;
            queue.offer(event);
        }

        @Override
        public BrokerEvent take() throws InterruptedException {
            while (!closed) {
                var event = queue.poll(250, java.util.concurrent.TimeUnit.MILLISECONDS);
                if (event != null) return event;
            }
            return null;
        }

        @Override
        public void close() {
            closed = true;
            subscribers.remove(this);
            queue.clear();
        }
    }
}
