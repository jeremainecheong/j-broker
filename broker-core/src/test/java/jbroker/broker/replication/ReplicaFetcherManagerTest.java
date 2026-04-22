package jbroker.broker.replication;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import jbroker.broker.BrokerRegistry;
import jbroker.broker.TopicManager;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReplicaFetcherManagerTest {

    private static LogManager lm(Path dir) throws Exception {
        return new LogManager(
                dir,
                new LogManager.Config(
                        128L * 1024 * 1024,
                        Long.MAX_VALUE,
                        LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        TimeUnit.MINUTES.toMillis(5)));
    }

    /** Captures per-partition start + stop so assertions can inspect them. */
    private static final class FakeFetcher {
        boolean stopped = false;
    }

    private ReplicaFetcherManager.FetcherFactory capturingFactory(ConcurrentHashMap<String, FakeFetcher> handles) {
        return (topic, partition, leaderBrokerId, leaderAddr) -> {
            var fake = new FakeFetcher();
            handles.put(topic + "-" + partition, fake);
            return () -> fake.stopped = true;
        };
    }

    @Test
    void reconcileStartsFetcherForFollowerPartitionWhenLeaderAddressKnown(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, /*leader*/ 1, List.of(1, 2, 3), List.of(1, 2, 3), 5, 0);
        var reg = new BrokerRegistry();
        reg.onBrokerRegistration(1, "h1", 9001);
        try (var lmgr = lm(dir)) {
            var handles = new ConcurrentHashMap<String, FakeFetcher>();
            var mgr = new ReplicaFetcherManager(/*selfId*/ 2, tm, reg, lmgr, capturingFactory(handles));

            mgr.reconcile();
            assertThat(handles).containsKey("orders-0");
            assertThat(handles.get("orders-0").stopped).isFalse();
        }
    }

    @Test
    void reconcileIsIdempotent(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, 1, List.of(1, 2, 3), List.of(1, 2, 3), 5, 0);
        var reg = new BrokerRegistry();
        reg.onBrokerRegistration(1, "h1", 9001);
        try (var lmgr = lm(dir)) {
            var handles = new ConcurrentHashMap<String, FakeFetcher>();
            var mgr = new ReplicaFetcherManager(2, tm, reg, lmgr, capturingFactory(handles));

            mgr.reconcile();
            mgr.reconcile();
            assertThat(handles).hasSize(1);
        }
    }

    @Test
    void reconcileStopsFetcherWhenSelfBecomesLeader(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, 1, List.of(1, 2, 3), List.of(1, 2, 3), 5, 0);
        var reg = new BrokerRegistry();
        reg.onBrokerRegistration(1, "h1", 9001);
        reg.onBrokerRegistration(2, "h2", 9002);
        try (var lmgr = lm(dir)) {
            var handles = new ConcurrentHashMap<String, FakeFetcher>();
            var mgr = new ReplicaFetcherManager(2, tm, reg, lmgr, capturingFactory(handles));

            mgr.reconcile();
            tm.onPartitionChange("orders", 0, /*now leader*/ 2, List.of(2, 3), List.of(1, 2, 3), 6, 0);
            mgr.reconcile();

            assertThat(handles.get("orders-0").stopped).isTrue();
        }
    }

    @Test
    void reconcileStopsFetcherWhenSelfLeavesReplicas(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, 1, List.of(1, 2, 3), List.of(1, 2, 3), 5, 0);
        var reg = new BrokerRegistry();
        reg.onBrokerRegistration(1, "h1", 9001);
        try (var lmgr = lm(dir)) {
            var handles = new ConcurrentHashMap<String, FakeFetcher>();
            var mgr = new ReplicaFetcherManager(2, tm, reg, lmgr, capturingFactory(handles));

            mgr.reconcile();
            // Reassignment removes self (2) from replica set.
            tm.onPartitionChange("orders", 0, 1, List.of(1, 3), List.of(1, 3), 5, 1);
            mgr.reconcile();

            assertThat(handles.get("orders-0").stopped).isTrue();
        }
    }

    @Test
    void reconcileDeferredUntilLeaderAddressRegistered(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, 1, List.of(1, 2, 3), List.of(1, 2, 3), 5, 0);
        var reg = new BrokerRegistry(); // empty
        try (var lmgr = lm(dir)) {
            var handles = new ConcurrentHashMap<String, FakeFetcher>();
            var mgr = new ReplicaFetcherManager(2, tm, reg, lmgr, capturingFactory(handles));

            mgr.reconcile();
            assertThat(handles).isEmpty();

            reg.onBrokerRegistration(1, "h1", 9001);
            mgr.reconcile();
            assertThat(handles).containsKey("orders-0");
        }
    }

    @Test
    void listenerCallbacksTriggerReconcile(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        var reg = new BrokerRegistry();
        try (var lmgr = lm(dir)) {
            var handles = new ConcurrentHashMap<String, FakeFetcher>();
            try (var mgr = new ReplicaFetcherManager(2, tm, reg, lmgr, capturingFactory(handles))) {
                // Initial state: no partitions known, no fetchers.
                mgr.reconcile();
                assertThat(handles).isEmpty();

                // Simulate the state machine firing PartitionChangeListener
                // after an applied CreateTopicRecord.
                tm.onPartitionChange("orders", 0, 1, List.of(1, 2, 3), List.of(1, 2, 3), 5, 0);
                mgr.onPartitionChange(
                        "orders", 0, tm.partitionState("orders", 0).orElseThrow());
                // Async reconcile; wait for it to be observable. Leader
                // address still unregistered so the fetcher stays pending.
                awaitAsync(() -> handles.isEmpty(), "expected still no fetcher before registration");

                reg.onBrokerRegistration(1, "h1", 9001);
                mgr.onBrokerRegistration(1, "h1", 9001);
                awaitAsync(() -> handles.containsKey("orders-0"), "expected fetcher spawn after registration");
            }
        }
    }

    @Test
    void scheduleReconcileCoalescesRapidFires(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, 1, List.of(1, 2, 3), List.of(1, 2, 3), 5, 0);
        var reg = new BrokerRegistry();
        reg.onBrokerRegistration(1, "h1", 9001);
        try (var lmgr = lm(dir)) {
            var startCount = new java.util.concurrent.atomic.AtomicInteger();
            ReplicaFetcherManager.FetcherFactory counting = (topic, partition, leaderBrokerId, leaderAddr) -> {
                startCount.incrementAndGet();
                return () -> {};
            };
            try (var mgr = new ReplicaFetcherManager(2, tm, reg, lmgr, counting)) {
                // Fire 100 events in quick succession; they coalesce to
                // one queued reconcile + at most one in-flight reconcile
                // — start gets called at most once for this stable
                // partition state.
                for (int i = 0; i < 100; i++) {
                    mgr.scheduleReconcile();
                }
                awaitAsync(() -> startCount.get() >= 1, "expected at least one reconcile to run");
                Thread.sleep(50); // let any trailing reconciles drain
                assertThat(startCount.get())
                        .as("start should be called at most twice for coalesced fires (1 initial + at most 1 queued)")
                        .isLessThanOrEqualTo(2);
            }
        }
    }

    private static void awaitAsync(java.util.function.BooleanSupplier cond, String msg) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 2_000;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return;
            Thread.sleep(10);
        }
        throw new AssertionError(msg);
    }

    @Test
    void reconcileDoesNotSpawnWhenSelfIsLeader(@TempDir Path dir) throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 3, 0L);
        tm.onPartitionChange("orders", 0, /*leader*/ 2, List.of(1, 2, 3), List.of(1, 2, 3), 5, 0);
        var reg = new BrokerRegistry();
        reg.onBrokerRegistration(1, "h1", 9001);
        reg.onBrokerRegistration(2, "h2", 9002);
        reg.onBrokerRegistration(3, "h3", 9003);
        try (var lmgr = lm(dir)) {
            var handles = new ConcurrentHashMap<String, FakeFetcher>();
            var mgr = new ReplicaFetcherManager(/*self*/ 2, tm, reg, lmgr, capturingFactory(handles));

            mgr.reconcile();
            assertThat(handles).isEmpty();
        }
    }
}
