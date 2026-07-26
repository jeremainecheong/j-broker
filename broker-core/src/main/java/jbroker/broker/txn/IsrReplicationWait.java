package jbroker.broker.txn;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import jbroker.broker.TopicManager;
import jbroker.broker.replication.FollowerStateTracker;
import jbroker.broker.replication.ReplicationSignals;
import jbroker.storage.LogManager;

/**
 * The acks=all wait, shared by every transaction-side append that must be
 * durable before the caller may proceed: coordinator state records
 * (append-before-answer) and control-batch markers (a marker confirmed to
 * the coordinator but lost with its leader would leave the partition's
 * data undecided forever). Same loop as the produce path's replication
 * wait: park on the partition's {@link ReplicationSignals} monitor and
 * re-check on every wakeup until the partition HWM passes
 * {@code lastOffset} with the ISR still at or above the
 * {@code min.insync.replicas} floor, bounded by {@code timeoutMillis};
 * leadership loss, an ISR below the floor, and timeout all answer
 * {@code false} — retriable, the bytes are on disk and a retry re-runs
 * only the wait.
 */
public final class IsrReplicationWait {

    /**
     * Await slice, matching the produce path: waits normally wake on the
     * signal; the slice bounds what a lost wakeup can cost before every
     * predicate re-runs.
     */
    private static final long AWAIT_SLICE_NANOS = TimeUnit.MILLISECONDS.toNanos(100);

    /**
     * Fallback hub for callers wired without the broker's signals: nothing
     * ever signals it, so the wait degrades to a sliced poll.
     */
    private static final ReplicationSignals UNSIGNALED = new ReplicationSignals();

    private IsrReplicationWait() {}

    /** Legacy signature — waits without a signal feed (sliced poll). */
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
        return await(
                UNSIGNALED,
                topicManager,
                logManager,
                followerTracker,
                selfBrokerId,
                clusterMinInsyncReplicas,
                topic,
                partition,
                lastOffset,
                timeoutMillis);
    }

    public static boolean await(
            ReplicationSignals signals,
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
            // Stamp precedes the predicates so replication finishing
            // between the HWM check and the await bumps the generation and
            // the await returns without parking.
            long stamp = signals.stamp(topic, partition);
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
                long remaining = deadline - System.nanoTime();
                signals.await(topic, partition, stamp, Math.min(remaining, AWAIT_SLICE_NANOS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }
}
