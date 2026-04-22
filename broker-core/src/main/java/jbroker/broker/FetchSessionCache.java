package jbroker.broker;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import jbroker.proto.common.TopicPartition;

/**
 * Broker-local LRU of active fetch sessions (P7.11). Each session tracks the
 * last {@code (offset, leaderEpoch)} the broker served for every
 * {@code (topic, partition)} under its id. Clients bootstrap with
 * {@code session_id=0}; the broker allocates a fresh id and returns it in
 * the response. Subsequent requests echo the id so the broker can locate
 * (and, on overflow, evict) the cached per-session state.
 *
 * <p>This class is thread-safe — every public mutator is {@code synchronized}
 * on the cache instance so concurrent {@code Fetch} RPCs on the gRPC event
 * loop can't corrupt LRU order or the inner state map.
 *
 * <p>Session ids are positive; {@code 0} is reserved as the "no session"
 * sentinel and is never allocated.
 */
public final class FetchSessionCache {

    public static final int DEFAULT_CAPACITY = 1000;

    private final int capacity;
    private final AtomicInteger idSeq = new AtomicInteger();
    private final LinkedHashMap<Integer, Session> sessions;

    public FetchSessionCache() {
        this(DEFAULT_CAPACITY);
    }

    public FetchSessionCache(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be positive");
        this.capacity = capacity;
        this.sessions = new LinkedHashMap<>(16, 0.75f, /*accessOrder=*/ true);
    }

    /** Allocate a fresh session id (always positive, never 0) and start tracking its state. */
    public synchronized int allocate() {
        int id = idSeq.incrementAndGet();
        if (id == 0) id = idSeq.incrementAndGet(); // skip the sentinel
        sessions.put(id, new Session(id));
        evictIfOverCapacity();
        return id;
    }

    /**
     * Record that the broker served {@code offset} (at {@code leaderEpoch})
     * for {@code tp} on session {@code id}, bumping it to MRU.
     *
     * @return {@code true} on success, {@code false} if the id was evicted or
     *     never allocated — caller should surface {@code FETCH_SESSION_ID_NOT_FOUND}.
     */
    public synchronized boolean update(int id, TopicPartition tp, long offset, int leaderEpoch) {
        var s = sessions.get(id); // access-order map → touches MRU
        if (s == null) return false;
        s.put(tp, offset, leaderEpoch);
        return true;
    }

    /** Lookup without mutating state (access-order map still bumps LRU on read). */
    public synchronized Optional<Session> get(int id) {
        var s = sessions.get(id);
        return Optional.ofNullable(s);
    }

    public synchronized int size() {
        return sessions.size();
    }

    private void evictIfOverCapacity() {
        while (sessions.size() > capacity) {
            var oldest = sessions.entrySet().iterator().next();
            sessions.remove(oldest.getKey());
        }
    }

    /** Snapshot of one session's last-served state. */
    public static final class Session {
        private final int id;
        private final Map<TopicPartition, PartitionState> state = new HashMap<>();

        Session(int id) {
            this.id = id;
        }

        void put(TopicPartition tp, long offset, int leaderEpoch) {
            state.put(tp, new PartitionState(offset, leaderEpoch));
        }

        public int id() {
            return id;
        }

        public long offsetFor(TopicPartition tp) {
            var s = state.get(tp);
            return s == null ? -1L : s.offset;
        }

        public int leaderEpochFor(TopicPartition tp) {
            var s = state.get(tp);
            return s == null ? -1 : s.leaderEpoch;
        }

        public int partitionCount() {
            return state.size();
        }
    }

    private record PartitionState(long offset, int leaderEpoch) {}
}
