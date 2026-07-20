package jbroker.broker.auth;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory ACL cache, replicated on every broker by applying committed
 * {@code AclRecord} / {@code RemoveAclRecord} metadata entries. Entries
 * are keyed by (principal, resourceType, resourceName, prefix, operation);
 * re-adding an existing key overwrites its allow flag, so the store is
 * idempotent under Raft replay.
 *
 * <p>Matching follows Kafka's shape: an entry matches a request when the
 * principal is equal, the resource type is equal, the operation is equal
 * or the entry's operation is {@code *}, and the resource name is equal —
 * or, for prefix entries, is a name prefix; the exact name {@code *}
 * matches every resource. Among matching entries an explicit deny wins
 * over any allow. {@link #allows} says nothing about what happens when no
 * entry matches — default-deny vs. open is the authorizer's call, made
 * from {@code auth.mode}.
 */
public final class AclStore {

    /** One access-control entry. The first five fields form the identity key. */
    public record Entry(
            String principal,
            String resourceType,
            String resourceName,
            boolean prefix,
            String operation,
            boolean allow) {

        public Entry {
            if (principal == null || principal.isBlank()) {
                throw new IllegalArgumentException("principal must be non-blank");
            }
            if (resourceType == null || resourceType.isBlank()) {
                throw new IllegalArgumentException("resourceType must be non-blank");
            }
            if (resourceName == null || resourceName.isBlank()) {
                throw new IllegalArgumentException("resourceName must be non-blank");
            }
            if (operation == null || operation.isBlank()) {
                throw new IllegalArgumentException("operation must be non-blank");
            }
        }

        private Key key() {
            return new Key(principal, resourceType, resourceName, prefix, operation);
        }
    }

    private record Key(String principal, String resourceType, String resourceName, boolean prefix, String operation) {}

    private final Map<Key, Entry> entries = new ConcurrentHashMap<>();

    /** Add or overwrite the entry with the same identity key. */
    public void put(Entry entry) {
        entries.put(entry.key(), entry);
    }

    /** Remove by identity key. Unknown keys are a no-op (idempotent under replay). */
    public void remove(String principal, String resourceType, String resourceName, boolean prefix, String operation) {
        entries.remove(new Key(principal, resourceType, resourceName, prefix, operation));
    }

    /** Every stored entry, in no particular order. */
    public List<Entry> list() {
        return new ArrayList<>(entries.values());
    }

    /** Drop every entry. Snapshot restore starts from a clean slate. */
    public void clear() {
        entries.clear();
    }

    public int size() {
        return entries.size();
    }

    /**
     * Three-valued decision: {@code TRUE} when a matching allow exists and
     * no matching deny does, {@code FALSE} when a matching deny exists,
     * {@code null} when nothing matches (the authorizer applies the
     * default).
     */
    public Boolean allows(String principal, String resourceType, String resourceName, String operation) {
        boolean anyAllow = false;
        for (var e : entries.values()) {
            if (!matches(e, principal, resourceType, resourceName, operation)) continue;
            if (!e.allow()) return Boolean.FALSE; // explicit deny wins immediately
            anyAllow = true;
        }
        return anyAllow ? Boolean.TRUE : null;
    }

    private static boolean matches(
            Entry e, String principal, String resourceType, String resourceName, String operation) {
        if (!e.principal().equals(principal)) return false;
        if (!e.resourceType().equals(resourceType)) return false;
        if (!e.operation().equals("*") && !e.operation().equals(operation)) return false;
        if (e.prefix()) return resourceName.startsWith(e.resourceName());
        return e.resourceName().equals("*") || e.resourceName().equals(resourceName);
    }
}
