package jbroker.broker.auth;

import java.util.Set;

/**
 * The single yes/no gate handlers consult before touching a resource.
 * Combines {@code auth.mode}, the replicated {@link AclStore}, and the
 * {@code super.users} list:
 *
 * <ul>
 *   <li>{@code auth.mode=none} — everything is allowed; the cluster is
 *       open and ACLs are inert (matching Kafka with no authorizer).
 *   <li>super-users bypass ACL matching entirely. Inter-broker
 *       identities live here so replication and admin tooling keep
 *       working the moment enforcement turns on, before any ACL exists.
 *   <li>otherwise the store decides; no matching entry means deny.
 * </ul>
 *
 * <p>Resource-type / operation vocabulary: produce and consume act on
 * {@code topic} names; group-coordinator RPCs act on {@code group} names
 * with operation {@code consume}; admin mutations act on {@code topic}
 * names or the singleton {@code cluster} resource named
 * {@value #CLUSTER_RESOURCE} with operation {@code admin}.
 */
public final class Authorizer {

    /** The resource name every cluster-scoped check uses. */
    public static final String CLUSTER_RESOURCE = "cluster";

    /** Allows everything — the {@code auth.mode=none} gate and the test-harness default. */
    public static final Authorizer OPEN = new Authorizer(AuthMode.NONE, new AclStore(), Set.of());

    private final AuthMode mode;
    private final AclStore store;
    private final Set<String> superUsers;

    public Authorizer(AuthMode mode, AclStore store, Set<String> superUsers) {
        this.mode = mode;
        this.store = store;
        this.superUsers = Set.copyOf(superUsers);
    }

    public boolean allowed(String principal, String resourceType, String resourceName, String operation) {
        if (mode == AuthMode.NONE) return true;
        if (superUsers.contains(principal)) return true;
        Boolean decision = store.allows(principal, resourceType, resourceName, operation);
        return Boolean.TRUE.equals(decision);
    }

    /** Convenience for the current request's principal ({@link AuthContext}). */
    public boolean allowsCurrent(String resourceType, String resourceName, String operation) {
        return allowed(AuthContext.principalOrAnonymous(), resourceType, resourceName, operation);
    }
}
