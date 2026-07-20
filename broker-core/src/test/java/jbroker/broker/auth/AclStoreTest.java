package jbroker.broker.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class AclStoreTest {

    private static AclStore.Entry allow(String principal, String type, String name, String op) {
        return new AclStore.Entry(principal, type, name, false, op, true);
    }

    @Test
    void exactMatchAllows() {
        var store = new AclStore();
        store.put(allow("alice", "topic", "orders", "produce"));

        assertThat(store.allows("alice", "topic", "orders", "produce")).isTrue();
    }

    @Test
    void noMatchingEntryIsUndecided() {
        var store = new AclStore();
        store.put(allow("alice", "topic", "orders", "produce"));

        assertThat(store.allows("alice", "topic", "orders", "consume")).isNull();
        assertThat(store.allows("bob", "topic", "orders", "produce")).isNull();
        assertThat(store.allows("alice", "group", "orders", "produce")).isNull();
        assertThat(store.allows("alice", "topic", "payments", "produce")).isNull();
    }

    @Test
    void explicitDenyBeatsAllow() {
        var store = new AclStore();
        store.put(allow("alice", "topic", "*", "produce"));
        store.put(new AclStore.Entry("alice", "topic", "orders", false, "produce", false));

        assertThat(store.allows("alice", "topic", "orders", "produce")).isFalse();
        assertThat(store.allows("alice", "topic", "payments", "produce")).isTrue();
    }

    @Test
    void prefixEntryMatchesEveryNameUnderIt() {
        var store = new AclStore();
        store.put(new AclStore.Entry("svc", "topic", "invoices-", true, "produce", true));

        assertThat(store.allows("svc", "topic", "invoices-2026", "produce")).isTrue();
        assertThat(store.allows("svc", "topic", "invoices-eu", "produce")).isTrue();
        assertThat(store.allows("svc", "topic", "orders", "produce")).isNull();
    }

    @Test
    void wildcardOperationCoversAllOperations() {
        var store = new AclStore();
        store.put(allow("ops", "cluster", "*", "*"));

        assertThat(store.allows("ops", "cluster", "cluster", "admin")).isTrue();
        assertThat(store.allows("ops", "cluster", "cluster", "produce")).isTrue();
    }

    @Test
    void reAddingAKeyOverwritesItsAllowFlag() {
        var store = new AclStore();
        store.put(allow("alice", "topic", "orders", "produce"));
        store.put(new AclStore.Entry("alice", "topic", "orders", false, "produce", false));

        assertThat(store.allows("alice", "topic", "orders", "produce")).isFalse();
        assertThat(store.size()).isEqualTo(1);
    }

    @Test
    void removeIsIdempotent() {
        var store = new AclStore();
        store.put(allow("alice", "topic", "orders", "produce"));

        store.remove("alice", "topic", "orders", false, "produce");
        store.remove("alice", "topic", "orders", false, "produce");

        assertThat(store.allows("alice", "topic", "orders", "produce")).isNull();
        assertThat(store.list()).isEmpty();
    }

    @Test
    void prefixAndExactEntriesWithTheSameNameAreDistinctKeys() {
        var store = new AclStore();
        store.put(new AclStore.Entry("alice", "topic", "orders", true, "produce", true));
        store.put(allow("alice", "topic", "orders", "produce"));

        assertThat(store.size()).isEqualTo(2);

        store.remove("alice", "topic", "orders", false, "produce");
        assertThat(store.allows("alice", "topic", "orders-eu", "produce"))
                .as("prefix entry survives removing the exact one")
                .isTrue();
    }

    @Test
    void blankFieldsAreRejected() {
        assertThatThrownBy(() -> new AclStore.Entry("", "topic", "orders", false, "produce", true))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AclStore.Entry("alice", "topic", " ", false, "produce", true))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
