package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class ReassignmentStoreTest {

    @Test
    void putThenGetReturnsTargetAndOriginal() {
        var store = new ReassignmentStore();
        store.put(new ReassignmentStore.Pending("t", 0, List.of(2, 3, 4), List.of(1, 2, 3)));

        var got = store.get("t", 0).orElseThrow();
        assertThat(got.target()).containsExactly(2, 3, 4);
        assertThat(got.original()).containsExactly(1, 2, 3);
        assertThat(store.size()).isEqualTo(1);
    }

    @Test
    void putOverwritesSameKeyIdempotently() {
        var store = new ReassignmentStore();
        store.put(new ReassignmentStore.Pending("t", 0, List.of(2, 3, 4), List.of(1, 2, 3)));
        store.put(new ReassignmentStore.Pending("t", 0, List.of(5, 6), List.of(1, 2, 3)));

        assertThat(store.size()).isEqualTo(1);
        assertThat(store.get("t", 0).orElseThrow().target()).containsExactly(5, 6);
    }

    @Test
    void clearRemovesTheEntryAndIsANoOpWhenAbsent() {
        var store = new ReassignmentStore();
        store.put(new ReassignmentStore.Pending("t", 0, List.of(2, 3), List.of(1, 2)));
        store.clear("t", 0);
        assertThat(store.get("t", 0)).isEmpty();
        // Idempotent under replay: clearing an unknown key does nothing.
        store.clear("t", 0);
        store.clear("other", 9);
        assertThat(store.size()).isZero();
    }

    @Test
    void distinctPartitionsAreTrackedSeparately() {
        var store = new ReassignmentStore();
        store.put(new ReassignmentStore.Pending("t", 0, List.of(2, 3), List.of(1, 2)));
        store.put(new ReassignmentStore.Pending("t", 1, List.of(3, 4), List.of(2, 3)));
        assertThat(store.list()).hasSize(2);
        assertThat(store.get("t", 0).orElseThrow().target()).containsExactly(2, 3);
        assertThat(store.get("t", 1).orElseThrow().target()).containsExactly(3, 4);
    }

    @Test
    void emptyTargetIsRejected() {
        assertThatThrownBy(() -> new ReassignmentStore.Pending("t", 0, List.of(), List.of(1, 2)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void pendingCopiesItsListsDefensively() {
        var target = new java.util.ArrayList<>(List.of(2, 3));
        var pending = new ReassignmentStore.Pending("t", 0, target, List.of(1, 2));
        target.add(99);
        assertThat(pending.target()).containsExactly(2, 3);
    }
}
