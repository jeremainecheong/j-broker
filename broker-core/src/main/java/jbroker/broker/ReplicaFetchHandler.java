package jbroker.broker;

import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.function.LongSupplier;
import jbroker.broker.replication.FollowerStateTracker;
import jbroker.proto.broker.ReplicaFetchRequest;
import jbroker.proto.broker.ReplicaFetchResponse;
import jbroker.storage.LogManager;

/**
 * Serves the broker-internal {@code ReplicaFetch} RPC: a follower pulls
 * record batches from the partition leader. Validates {@code leader_epoch}
 * and ISR membership, records the follower's LEO in the shared
 * {@link FollowerStateTracker}, then returns records + advanced HWM.
 *
 * <p>HWM is computed as {@code min(LEO across ISR)}; the follower uses it
 * to know what's safe for consumers (enforced on the
 * Consumer.Fetch path).
 *
 * <p>A fetch carrying {@code max_wait_ms} that finds no data long-polls:
 * the handler parks on the partition's
 * {@link jbroker.broker.replication.ReplicationSignals} monitor until an
 * append lands, a partition-change record applies, the wait budget runs
 * out, or the broker shuts down — then re-runs the full gate chain and
 * serves. Follower liveness state is recorded once, at request arrival,
 * exactly as an immediate fetch would record it.
 */
public final class ReplicaFetchHandler {

    /** Minimum lag (records) for ReplicationLagEvent emission. */
    private static final long LAG_EVENT_THRESHOLD_RECORDS = 10L;

    /** Ceiling on a caller-requested long-poll park. */
    private static final long MAX_WAIT_CAP_MS = 10_000L;

    /**
     * Park slice: a wakeup lost to a stale stamp costs at most this before
     * the gates re-run. No park ever holds a lock — the slice re-enters
     * {@link #serve} from scratch.
     */
    private static final long PARK_SLICE_NANOS = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(100);

    private final LogManager logManager;
    private final TopicManager topicManager;
    private final int selfBrokerId;
    private final FollowerStateTracker tracker;
    private final LongSupplier clock;
    private final jbroker.broker.replication.ReplicationSignals signals;

    public ReplicaFetchHandler(
            LogManager logManager,
            TopicManager topicManager,
            int selfBrokerId,
            FollowerStateTracker tracker,
            LongSupplier clock) {
        this(logManager, topicManager, selfBrokerId, tracker, clock, null);
    }

    /**
     * Constructor with the broker's {@link
     * jbroker.broker.replication.ReplicationSignals} hub. Null disables
     * long-polling: every fetch answers immediately, exactly the
     * pre-long-poll behavior (and what a follower's rolling-upgrade guard
     * expects from an old leader).
     */
    public ReplicaFetchHandler(
            LogManager logManager,
            TopicManager topicManager,
            int selfBrokerId,
            FollowerStateTracker tracker,
            LongSupplier clock,
            jbroker.broker.replication.ReplicationSignals signals) {
        this.logManager = logManager;
        this.topicManager = topicManager;
        this.selfBrokerId = selfBrokerId;
        this.tracker = tracker;
        this.clock = clock;
        this.signals = signals;
    }

    public ReplicaFetchResponse handle(ReplicaFetchRequest req) {
        long maxWaitMs = Math.min(req.getMaxWaitMs(), MAX_WAIT_CAP_MS);
        if (maxWaitMs <= 0 || signals == null) {
            return serve(req, true);
        }
        long deadlineNanos = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(maxWaitMs);
        // Stamp precedes the first gate run: an append racing serve() bumps
        // the generation, and the await below returns without parking.
        long stamp = signals.stamp(req.getTopic(), req.getPartition());
        var resp = serve(req, true);
        // Only a clean-empty answer parks. Errors (NOT_LEADER, FENCED_EPOCH,
        // IO) and data-bearing answers return immediately, byte-identical to
        // an immediate fetch.
        while (isCleanEmpty(resp) && !signals.isShutdown()) {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) break;
            try {
                signals.await(req.getTopic(), req.getPartition(), stamp, Math.min(remaining, PARK_SLICE_NANOS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            stamp = signals.stamp(req.getTopic(), req.getPartition());
            // Full gate re-run on every wakeup — a leadership or epoch flip
            // mid-park is answered exactly as a fresh fetch would see it.
            // Follower LEO / lastFetchMillis stay as recorded at arrival:
            // a parked follower is waiting, not fetching.
            resp = serve(req, false);
        }
        return resp;
    }

    private static boolean isCleanEmpty(ReplicaFetchResponse resp) {
        return (!resp.hasError() || resp.getError().getCode() == ErrorCodes.NONE)
                && resp.getRecords().isEmpty();
    }

    private ReplicaFetchResponse serve(ReplicaFetchRequest req, boolean recordFollowerState) {
        var state = topicManager.partitionState(req.getTopic(), req.getPartition());
        if (state.isEmpty() || state.get().leader() != selfBrokerId) {
            int currentEpoch = state.map(PartitionState::leaderEpoch).orElse(-1);
            return ReplicaFetchResponse.newBuilder()
                    .setCurrentLeaderEpoch(currentEpoch)
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.NOT_LEADER)
                            .setMessage("not leader for " + req.getTopic() + "-" + req.getPartition())
                            .build())
                    .build();
        }
        // Reject impostor brokers not in the replica set. Out-of-ISR
        // replicas still pass — they need to fetch in order to catch up
        // and have the IsrManager expand ISR back to them.
        if (!state.get().replicas().contains(req.getFollowerBrokerId())) {
            return ReplicaFetchResponse.newBuilder()
                    .setCurrentLeaderEpoch(state.get().leaderEpoch())
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.NOT_LEADER)
                            .setMessage("broker " + req.getFollowerBrokerId() + " is not a replica of " + req.getTopic()
                                    + "-" + req.getPartition())
                            .build())
                    .build();
        }
        int currentEpoch = state.get().leaderEpoch();
        if (req.getLeaderEpoch() != currentEpoch) {
            return ReplicaFetchResponse.newBuilder()
                    .setCurrentLeaderEpoch(currentEpoch)
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.FENCED_EPOCH)
                            .setMessage("follower epoch " + req.getLeaderEpoch() + " != current " + currentEpoch)
                            .build())
                    .build();
        }
        // Lineage validation: a matching metadata epoch does not prove the
        // follower's LOG matches. Compare the epoch owning the follower's
        // last local batch against this leader's lineage at that offset —
        // a diverged follower (its tail written under a leadership the
        // committed history rejected) must truncate before it may glue our
        // records on top and report a healthy LEO for bytes it does not
        // hold. Skipped for legacy fetchers, empty follower logs, and
        // offsets our checkpoint cannot attribute.
        if (req.hasLastFetchedEpoch() && req.getFetchOffset() > 0) {
            try {
                var lineage = logManager
                        .leaderEpochCheckpoint(req.getTopic(), req.getPartition())
                        .epochFor(req.getFetchOffset() - 1);
                if (lineage.isPresent() && lineage.get().epoch() != req.getLastFetchedEpoch()) {
                    return ReplicaFetchResponse.newBuilder()
                            .setCurrentLeaderEpoch(currentEpoch)
                            .setError(jbroker.proto.broker.Error.newBuilder()
                                    .setCode(ErrorCodes.FENCED_EPOCH)
                                    .setMessage("log lineage divergence at offset " + (req.getFetchOffset() - 1)
                                            + ": follower's last batch epoch " + req.getLastFetchedEpoch()
                                            + " != leader lineage epoch "
                                            + lineage.get().epoch())
                                    .build())
                            .build();
                }
            } catch (IOException e) {
                return ReplicaFetchResponse.newBuilder()
                        .setCurrentLeaderEpoch(currentEpoch)
                        .setError(jbroker.proto.broker.Error.newBuilder()
                                .setCode(ErrorCodes.IO_ERROR)
                                .setMessage(e.toString())
                                .build())
                        .build();
            }
        }
        // Record the follower's LEO — the fetch_offset is the first offset
        // it still needs, which equals its local LEO. Only recorded for
        // accepted fetches so a fenced / impostor peer can't drag HWM down,
        // and only at request arrival so a long-polled fetch cannot refresh
        // its own liveness while merely waiting.
        if (recordFollowerState) {
            tracker.record(
                    req.getTopic(),
                    req.getPartition(),
                    req.getFollowerBrokerId(),
                    req.getFetchOffset(),
                    clock.getAsLong());
        }
        try {
            var log = logManager.logFor(req.getTopic(), req.getPartition());
            long leaderLeo = log.nextOffset();
            var baos = new ByteArrayOutputStream();
            if (req.getFetchOffset() < leaderLeo) {
                int maxBytes = req.getMaxBytes() > 0 ? req.getMaxBytes() : 1024 * 1024;
                log.transferTo(req.getFetchOffset(), maxBytes, baos);
            }
            long priorFloor = tracker.hwmFloor(req.getTopic(), req.getPartition());
            long hwm = tracker.computeHwm(
                    req.getTopic(), req.getPartition(), state.get().isr(), selfBrokerId, leaderLeo);
            // Wake acks=all waiters only when the floor actually moved —
            // signaling on every recompute would let two parked followers
            // wake each other in a loop.
            if (signals != null && hwm > priorFloor) {
                signals.signalHwmOrState(req.getTopic(), req.getPartition());
            }
            // Emit ReplicationLag when the follower is meaningfully
            // behind the leader. Leader-side because only the leader has the
            // authoritative leaderLeo; suppressed below a small threshold to
            // keep JFR volume manageable at steady state.
            long lag = Math.max(0L, leaderLeo - req.getFetchOffset());
            if (lag >= LAG_EVENT_THRESHOLD_RECORDS) {
                var jfr = new jbroker.broker.jfr.ReplicationLagEvent();
                if (jfr.shouldCommit()) {
                    jfr.topic = req.getTopic();
                    jfr.partition = req.getPartition();
                    jfr.followerBrokerId = req.getFollowerBrokerId();
                    jfr.lagRecords = lag;
                    jfr.commit();
                }
            }
            return ReplicaFetchResponse.newBuilder()
                    .setRecords(ByteString.copyFrom(baos.toByteArray()))
                    .setHighWatermark(hwm)
                    .setCurrentLeaderEpoch(currentEpoch)
                    .build();
        } catch (IOException e) {
            return ReplicaFetchResponse.newBuilder()
                    .setCurrentLeaderEpoch(currentEpoch)
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(ErrorCodes.IO_ERROR)
                            .setMessage(e.toString())
                            .build())
                    .build();
        }
    }
}
