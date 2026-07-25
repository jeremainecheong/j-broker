package jbroker.broker.txn;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import jbroker.broker.TopicManager;
import jbroker.broker.replication.FollowerStateTracker;
import jbroker.storage.LogManager;

/**
 * The acks=all wait, shared by every transaction-side append that must be
 * durable before the caller may proceed: coordinator state records
 * (append-before-answer) and control-batch markers (a marker confirmed to
 * the coordinator but lost with its leader would leave the partition's
 * data undecided forever). Same loop as the produce path's replication
 * wait: poll until the partition HWM passes {@code lastOffset} with the
 * ISR still at or above the {@code min.insync.replicas} floor, bounded by
 * {@code timeoutMillis}; leadership loss, an ISR below the floor, and
 * timeout all answer {@code false} — retriable, the bytes are on disk and
 * a retry re-runs only the wait.
 */
public final class IsrReplicationWait {

    /** Poll cadence, matching the produce path. */
    private static final long POLL_MS = 10L;

    private IsrReplicationWait() {}

    public static boolean await(
            TopicManager topicManager,
            LogManager logManager,
            FollowerStateTracker followerTracker,
            int selfBrokerId,
            int clusterMinInsyncReplicas,
            String topic,
            int partition,
            long lastOffset,
            long timeoutMillis)
            throws IOException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            var state = topicManager.partitionState(topic, partition);
            if (state.isEmpty() || state.get().leader() != selfBrokerId) return false;
            int floor = topicManager
                    .describe(topic)
                    .map(t -> t.effectiveMinInsyncReplicas(clusterMinInsyncReplicas))
                    .orElse(1);
            int isrSize = state.get().isr().size();
            long leaderLeo = logManager.logFor(topic, partition).nextOffset();
            long hwm = followerTracker.computeHwm(topic, partition, state.get().isr(), selfBrokerId, leaderLeo);
            // HWM is the first offset NOT yet durably replicated, so the
            // append is covered once HWM > lastOffset — but only while the
            // ISR that produced that HWM still satisfies the floor.
            if (hwm > lastOffset) return isrSize >= floor;
            if (isrSize < floor) return false;
            if (System.nanoTime() >= deadline) return false;
            try {
                Thread.sleep(POLL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }
}
