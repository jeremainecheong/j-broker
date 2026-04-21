package jbroker.sim;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import jbroker.raft.core.DefaultRaftCore;
import jbroker.raft.core.LogEntry;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.RaftConfig;
import jbroker.raft.core.RaftEffect;
import jbroker.raft.core.RaftEvent;
import jbroker.raft.core.Role;
import jbroker.raft.core.StateMachine;

/**
 * Deterministic Raft simulator. Given a seed, advances a virtual clock and
 * drives {@link DefaultRaftCore} instances through an in-memory message
 * queue. All randomness (election jitter, delivery delay, later: delivery
 * chaos) goes through a seeded {@link Random} so the same seed replays bit
 * for bit.
 *
 * <p>Scope for P3.1: happy-path only — every effect-derived message is
 * delivered exactly once after a bounded random delay. Chaos (drop/reorder/
 * duplicate), crash injection, invariant checkers, and shrinking come in
 * later phases.
 */
public final class Simulator {

    public static final class Node {
        public final NodeId id;
        public final DefaultRaftCore core;
        public final InMemoryRaftLog log;
        public final InMemoryPersistentState state;
        public final RecordingStateMachine sm;

        Node(NodeId id, DefaultRaftCore core, InMemoryRaftLog log, InMemoryPersistentState state) {
            this.id = id;
            this.core = core;
            this.log = log;
            this.state = state;
            this.sm = new RecordingStateMachine();
        }
    }

    /** Trivial state machine that records applied payloads for invariant checks. */
    public static final class RecordingStateMachine implements StateMachine {
        public final List<LogEntry> applied = new ArrayList<>();

        @Override
        public void apply(LogEntry entry) {
            applied.add(entry);
        }
    }

    private static final long TICK_NANOS = TimeUnit.MILLISECONDS.toNanos(10);
    private static final long MIN_DELIVERY_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final long MAX_DELIVERY_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

    private final long seed;
    private final Random random;
    private final Map<NodeId, Node> nodes = new HashMap<>();
    private final PriorityQueue<ScheduledEvent> queue = new PriorityQueue<>();
    private long now;
    private long eventCounter;

    public Simulator(long seed, int clusterSize) {
        this.seed = seed;
        this.random = new Random(seed);
        var ids = new ArrayList<NodeId>();
        for (int i = 1; i <= clusterSize; i++) ids.add(new NodeId(i));
        for (var id : ids) {
            var log = new InMemoryRaftLog();
            var state = new InMemoryPersistentState();
            var config = new RaftConfig(
                    id,
                    ids,
                    TimeUnit.MILLISECONDS.toNanos(200),
                    TimeUnit.MILLISECONDS.toNanos(100),
                    TimeUnit.MILLISECONDS.toNanos(40),
                    100);
            // Each node gets its own seeded random derived from the master seed
            // + node id so per-node jitter is reproducible without coupling.
            var nodeRandom = new Random(seed ^ ((long) id.value() << 32));
            var core = new DefaultRaftCore(config, log, state, 0L, nodeRandom);
            var node = new Node(id, core, log, state);
            nodes.put(id, node);
            // Prime each node with a Tick at time 0 so it arms its election deadline.
            enqueueTickFor(id, 0L);
        }
    }

    public long seed() {
        return seed;
    }

    public long now() {
        return now;
    }

    public Map<NodeId, Node> nodes() {
        return nodes;
    }

    public Node leader() {
        for (var n : nodes.values()) {
            if (n.core.role() == Role.LEADER) return n;
        }
        return null;
    }

    /**
     * Inject a {@link RaftEvent.ClientPropose} on the current leader (if any)
     * at the current virtual time. Returns true if delivered, false if no
     * leader exists yet.
     */
    public boolean propose(byte[] payload) {
        var leader = leader();
        if (leader == null) return false;
        deliver(leader.id, new RaftEvent.ClientPropose(payload));
        return true;
    }

    /**
     * Advance the clock until {@code untilNanos}, processing every message
     * whose deliver-time is {@code <=} that. Tick events for each node are
     * scheduled every {@link #TICK_NANOS}.
     */
    public void advanceTo(long untilNanos) {
        while (!queue.isEmpty() && queue.peek().deliverAt <= untilNanos) {
            var e = queue.poll();
            now = e.deliverAt;
            deliver(e.to, e.event);
            if (e.reschedule) {
                // The reschedule flag is only set for Tick events.
                enqueueTickFor(e.to, now + TICK_NANOS);
            }
        }
        if (now < untilNanos) now = untilNanos;
    }

    public void runFor(long durationNanos) {
        advanceTo(now + durationNanos);
    }

    private void deliver(NodeId to, RaftEvent event) {
        var node = nodes.get(to);
        if (node == null) return;
        var effects = node.core.step(event);
        handleEffects(node, effects);
    }

    private void handleEffects(Node from, List<RaftEffect> effects) {
        for (var eff : effects) {
            switch (eff) {
                case RaftEffect.SendAppendEntries s -> scheduleMessage(
                        s.to(),
                        new RaftEvent.AppendEntriesReq(
                                s.term(),
                                s.leaderId(),
                                s.prevLogIndex(),
                                s.prevLogTerm(),
                                s.entries(),
                                s.leaderCommit(),
                                now,
                                s.heartbeatSeq()));
                case RaftEffect.SendAppendEntriesResp r -> scheduleMessage(
                        r.to(),
                        new RaftEvent.AppendEntriesResp(
                                from.id,
                                r.term(),
                                r.success(),
                                r.conflictIndex(),
                                r.conflictTerm(),
                                r.matchIndex(),
                                r.heartbeatSeq()));
                case RaftEffect.SendVoteReq v -> scheduleMessage(
                        v.to(),
                        new RaftEvent.VoteReq(v.term(), v.candidateId(), v.lastLogIndex(), v.lastLogTerm(), now));
                case RaftEffect.SendVoteResp v -> scheduleMessage(
                        v.to(), new RaftEvent.VoteResp(from.id, v.term(), v.granted()));
                case RaftEffect.SendPreVoteReq v -> scheduleMessage(
                        v.to(),
                        new RaftEvent.PreVoteReq(
                                v.hypotheticalTerm(), v.candidateId(), v.lastLogIndex(), v.lastLogTerm(), now));
                case RaftEffect.SendPreVoteResp v -> scheduleMessage(
                        v.to(), new RaftEvent.PreVoteResp(from.id, v.term(), v.granted()));
                case RaftEffect.SendTimeoutNow t -> scheduleMessage(
                        t.to(), new RaftEvent.TimeoutNow(from.id, t.term(), now));
                case RaftEffect.SendInstallSnapshot s -> scheduleMessage(
                        s.to(),
                        new RaftEvent.InstallSnapshotReq(
                                s.term(),
                                s.leaderId(),
                                s.lastIncludedIndex(),
                                s.lastIncludedTerm(),
                                s.snapshot(),
                                now));
                case RaftEffect.SendInstallSnapshotResp r -> scheduleMessage(
                        r.to(), new RaftEvent.InstallSnapshotResp(from.id, r.term()));
                case RaftEffect.ApplyCommitted a -> from.sm.apply(a.entry());
                case RaftEffect.ApplySnapshot a -> {
                    /* SM restore — test SM is payload-list only; snapshot bytes unused. */
                }
                case RaftEffect.PersistLog ignored -> {
                    /* In-memory log already persisted. */
                }
                case RaftEffect.PersistState ignored -> {
                    /* In-memory state already persisted. */
                }
                case RaftEffect.TruncateLog ignored -> {
                    /* In-memory log handles inline. */
                }
                case RaftEffect.RejectClientPropose ignored -> {
                    /* Proposal routed to wrong node; ignore in happy-path sim. */
                }
                case RaftEffect.DuplicateClientPropose ignored -> {
                    /* No dedup in sim yet. */
                }
                case RaftEffect.ServeClientRead ignored -> {
                    /* No ClientRead in sim yet. */
                }
                case RaftEffect.RejectClientRead ignored -> {
                    /* No ClientRead in sim yet. */
                }
                case RaftEffect.RejectConfigChange ignored -> {
                    /* No config change injection in P3.1. */
                }
            }
        }
    }

    private void scheduleMessage(NodeId to, RaftEvent event) {
        long delay = MIN_DELIVERY_NANOS + (long) (random.nextDouble() * (MAX_DELIVERY_NANOS - MIN_DELIVERY_NANOS));
        queue.add(new ScheduledEvent(now + delay, eventCounter++, to, event, false));
    }

    private void enqueueTickFor(NodeId to, long at) {
        queue.add(new ScheduledEvent(at, eventCounter++, to, new RaftEvent.Tick(at), true));
    }

    private record ScheduledEvent(long deliverAt, long seq, NodeId to, RaftEvent event, boolean reschedule)
            implements Comparable<ScheduledEvent> {
        @Override
        public int compareTo(ScheduledEvent o) {
            int c = Long.compare(deliverAt, o.deliverAt);
            return c != 0 ? c : Long.compare(seq, o.seq);
        }
    }
}
