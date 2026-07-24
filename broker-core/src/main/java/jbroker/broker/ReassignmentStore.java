package jbroker.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory record of the partition reassignments in flight, replicated on
 * every broker by applying committed {@code PartitionReassignmentRecord}
 * entries. Keyed by (topic, partition); a record with a non-empty target
 * sets the pending reassignment, an empty target clears it. Re-applying the
 * same record overwrites the entry, so the store is idempotent under Raft
 * replay.
 *
 * <p>Only the controller's reassignment driver acts on this state, but every
 * broker keeps the cache so a newly elected controller can resume an
 * in-flight reassignment without losing the target. {@code original} is the
 * replica set that was current when the reassignment began, kept so a cancel
 * can revert to it.
 */
public final class ReassignmentStore {

    /** A pending reassignment for one partition. */
    public record Pending(String topic, int partition, List<Integer> target, List<Integer> original) {
        public Pending {
            if (topic == null || topic.isBlank()) {
                throw new IllegalArgumentException("topic must be non-blank");
            }
            target = List.copyOf(target);
            original = List.copyOf(original);
            if (target.isEmpty()) {
                throw new IllegalArgumentException("target must be non-empty; an empty target clears the reassignment");
            }
        }
    }

    private record Key(String topic, int partition) {}

    private final Map<Key, Pending> pending = new ConcurrentHashMap<>();

    /** Set (or overwrite) the pending reassignment for its partition. */
    public void put(Pending p) {
        pending.put(new Key(p.topic(), p.partition()), p);
    }

    /** Clear any pending reassignment for the partition. Unknown keys are a no-op. */
    public void clear(String topic, int partition) {
        pending.remove(new Key(topic, partition));
    }

    public Optional<Pending> get(String topic, int partition) {
        return Optional.ofNullable(pending.get(new Key(topic, partition)));
    }

    /** Every pending reassignment, in no particular order. */
    public List<Pending> list() {
        return new ArrayList<>(pending.values());
    }

    /** Drop every entry. Snapshot restore starts from a clean slate. */
    public void clearAll() {
        pending.clear();
    }

    public int size() {
        return pending.size();
    }
}
