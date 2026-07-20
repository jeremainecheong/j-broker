package jbroker.broker.auth;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import org.junit.jupiter.api.Test;

class AuthorizerTest {

    @Test
    void noneModeAllowsEverythingEvenWithDenyEntries() {
        var store = new AclStore();
        store.put(new AclStore.Entry("alice", "topic", "orders", false, "produce", false));
        var authorizer = new Authorizer(AuthMode.NONE, store, Set.of());

        assertThat(authorizer.allowed("alice", "topic", "orders", "produce")).isTrue();
        assertThat(authorizer.allowed("anonymous", "cluster", "cluster", "admin"))
                .isTrue();
    }

    @Test
    void mtlsModeDefaultDenies() {
        var authorizer = new Authorizer(AuthMode.MTLS, new AclStore(), Set.of());

        assertThat(authorizer.allowed("alice", "topic", "orders", "produce")).isFalse();
    }

    @Test
    void mtlsModeFollowsTheStore() {
        var store = new AclStore();
        store.put(new AclStore.Entry("alice", "topic", "orders", false, "produce", true));
        var authorizer = new Authorizer(AuthMode.MTLS, store, Set.of());

        assertThat(authorizer.allowed("alice", "topic", "orders", "produce")).isTrue();
        assertThat(authorizer.allowed("alice", "topic", "orders", "consume")).isFalse();
        assertThat(authorizer.allowed("bob", "topic", "orders", "produce")).isFalse();
    }

    @Test
    void superUsersBypassTheStoreEntirely() {
        var store = new AclStore();
        store.put(new AclStore.Entry("broker1", "topic", "orders", false, "produce", false));
        var authorizer = new Authorizer(AuthMode.MTLS, store, Set.of("broker1"));

        assertThat(authorizer.allowed("broker1", "topic", "orders", "produce")).isTrue();
        assertThat(authorizer.allowed("broker1", "cluster", "cluster", "admin")).isTrue();
    }

    @Test
    void allowsCurrentUsesTheRequestPrincipal() throws Exception {
        var store = new AclStore();
        store.put(new AclStore.Entry("alice", "topic", "orders", false, "produce", true));
        var authorizer = new Authorizer(AuthMode.MTLS, store, Set.of());

        boolean asAlice = AuthContext.callAs("alice", () -> authorizer.allowsCurrent("topic", "orders", "produce"));
        boolean asNobody = authorizer.allowsCurrent("topic", "orders", "produce");

        assertThat(asAlice).isTrue();
        assertThat(asNobody).as("anonymous outside a call context").isFalse();
    }
}
