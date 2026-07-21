package jbroker.raft.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public final class DefaultRaftCore implements RaftCore {

    private final RaftConfig config;
    private final RaftLog log;
    private final PersistentState persistentState;
    /** Null means fall back to {@link ThreadLocalRandom}; the simulator injects a seeded instance. */
    private final Random random;

    // volatile: written only by the driver's event-pump thread, but read
    // cross-thread via RaftDriver.role() (BrokerFencer ticker, balancer,
    // forceElection, tests). Without it those reads are a JMM data race.
    private volatile Role role = Role.FOLLOWER;
    // volatile for the same reason as role: read cross-thread via
    // RaftCore.currentLeader() (admin /cluster's controllerId).
    private volatile Optional<NodeId> leaderId = Optional.empty();
    private long commitIndex;
    private long lastApplied;
    private long electionDeadlineNanos;
    private long lastHeartbeatNanos;

    private final Set<NodeId> votesReceived = new HashSet<>();
    private final Set<NodeId> preVotesReceived = new HashSet<>();
    private final Map<NodeId, Long> nextIndex = new HashMap<>();
    private final Map<NodeId, Long> matchIndex = new HashMap<>();
    /** Last observed timestamp from any timestamp-bearing event (Tick / AE / VoteReq / PreVoteReq). */
    private long lastKnownNowNanos;
    /**
     * Per-client highest {@code clientSeq} accepted by this leader. Used to
     * short-circuit duplicate proposals. In-memory only; rebuilt when this
     * node next becomes leader. {@code clientId == 0} is treated as "no dedup".
     */
    private final Map<Long, Long> highestAcceptedSeq = new HashMap<>();

    /**
     * In-flight read-index requests awaiting heartbeat-quorum confirmation and
     * {@code lastApplied >= readIndex}. Mutated only while {@code role == LEADER};
     * entries are drained on serve or aborted with {@link RaftEffect.RejectClientRead}
     * on step-down.
     */
    private final List<PendingRead> pendingReads = new ArrayList<>();

    private static final class PendingRead {
        final long clientId;
        final long requestId;
        final long readIndex;
        /** Leader heartbeat seq at registration; only responses with seq > this count. */
        final long barrierSeq;

        final Set<NodeId> ackSet = new HashSet<>();

        PendingRead(long clientId, long requestId, long readIndex, long barrierSeq) {
            this.clientId = clientId;
            this.requestId = requestId;
            this.readIndex = readIndex;
            this.barrierSeq = barrierSeq;
        }
    }

    /**
     * Active voter set. Begins as {@code config.voters()}; replaced when a
     * {@link LogEntry.Type#CONFIG_CHANGE} entry is appended (Raft §4.2). The
     * change takes effect on append, not on commit. Only voters count toward
     * {@code quorum()}, vote, and are counted for commit.
     */
    private List<NodeId> activeVoters;

    /**
     * Active learner set: non-voting members that receive replication and
     * catch up to the leader's log, but never count toward quorum, vote, or
     * campaign. A newcomer joins here first and is promoted into
     * {@code activeVoters} by a later config change once caught up
     * (Raft dissertation §4.2.1). Tracked alongside {@code activeVoters} from
     * the same CONFIG_CHANGE payload.
     */
    private List<NodeId> activeLearners = List.of();

    /** Voters and learners — every member the leader replicates to. */
    private List<NodeId> replicationMembers() {
        if (activeLearners.isEmpty()) return activeVoters;
        var all = new java.util.ArrayList<NodeId>(activeVoters.size() + activeLearners.size());
        all.addAll(activeVoters);
        all.addAll(activeLearners);
        return all;
    }

    /**
     * Index of the most recently appended {@code CONFIG_CHANGE} that has not
     * yet been committed. {@code 0} means none in flight. Leader rejects new
     * config changes while non-zero.
     */
    private long inflightConfigChangeIndex;

    /**
     * Bytes of the most recent state-machine snapshot the driver has handed to
     * us. Used when a peer lags past the compaction point and must be caught
     * up via {@link RaftEffect.SendInstallSnapshot}. {@code null} until the
     * driver (or a test) calls {@link #installSnapshotBytesForTesting}.
     *
     * <p><b>Caller responsibility:</b> the bytes must correspond to the
     * state machine as of {@code log.lastIncludedIndex()} / {@code
     * lastIncludedTerm()} at the time of use. If the driver seeds stale bytes
     * the lagging peer will install inconsistent state. A hardening TODO is
     * to pair the bytes with the index/term they were produced for and
     * refuse to emit {@code SendInstallSnapshot} when they disagree.
     */
    private byte[] currentSnapshotBytes;

    /**
     * Monotonic counter incremented each time the leader sends an AE. Echoed
     * back in {@link RaftEvent.AppendEntriesResp#heartbeatSeq()} so a
     * {@link PendingRead} can distinguish responses to heartbeats sent after
     * the read arrived from in-flight responses to earlier heartbeats —
     * critical for read-index linearizability (Raft §6.4).
     */
    private long leaderHeartbeatSeq;

    /**
     * Constructs a new {@code DefaultRaftCore}.
     *
     * @param nowNanos the current monotonic timestamp in nanoseconds, used to
     *                 set the initial election deadline.  Pass
     *                 {@link Long#MAX_VALUE} to defer the first election: the
     *                 deadline is held at {@code Long.MAX_VALUE} until the first
     *                 {@link RaftEvent.Tick} arrives, at which point the timeout
     *                 is reset from that real timestamp.  This is useful for
     *                 integration tests where network servers need time to start
     *                 before any elections should fire.
     */
    public DefaultRaftCore(RaftConfig config, RaftLog log, PersistentState persistentState, long nowNanos) {
        this(config, log, persistentState, nowNanos, null);
    }

    /**
     * Deterministic-simulator-friendly constructor: pass a seeded
     * {@link Random} so election-timeout jitter is reproducible under a fixed
     * seed. Production callers should use the 4-arg constructor, which falls
     * back to {@link ThreadLocalRandom}.
     */
    public DefaultRaftCore(
            RaftConfig config, RaftLog log, PersistentState persistentState, long nowNanos, Random random) {
        this.config = Objects.requireNonNull(config, "config");
        this.log = Objects.requireNonNull(log, "log");
        this.persistentState = Objects.requireNonNull(persistentState, "persistentState");
        this.random = random;
        // Long.MAX_VALUE is a sentinel meaning "defer the first election until the
        // first real Tick arrives" (the onTick sentinel path handles this).
        this.electionDeadlineNanos =
                (nowNanos == Long.MAX_VALUE) ? Long.MAX_VALUE : nowNanos + randomisedElectionTimeout();
        this.lastKnownNowNanos = (nowNanos == Long.MAX_VALUE) ? 0L : nowNanos;
        this.activeVoters = List.copyOf(config.voters());
        reloadActiveVotersFromLog();
    }

    /**
     * Recompute {@code activeVoters} from the log. The latest CONFIG_CHANGE
     * entry present in the log wins; if none exist, fall back to the static
     * bootstrap {@code config.voters()}. Invoked from the constructor (restart
     * recovery) and after {@link RaftLog#truncateFrom} in
     * {@link #onAppendEntriesReq} to roll back a membership change whose
     * entry was truncated by a new leader.
     */
    private void reloadActiveVotersFromLog() {
        var rebuilt = Membership.ofVoters(config.voters());
        long first = log.firstIndex();
        long last = log.lastIndex();
        for (long i = first; i <= last; i++) {
            var existing = log.read(i, 1);
            if (existing.isEmpty()) break;
            var entry = existing.get(0);
            if (entry.type() == LogEntry.Type.CONFIG_CHANGE) {
                rebuilt = MembershipCodec.decodeMembership(entry.payload());
            }
        }
        this.activeVoters = rebuilt.voters();
        this.activeLearners = rebuilt.learners();
    }

    public List<NodeId> activeVoters() {
        return activeVoters;
    }

    private int quorum() {
        return activeVoters.size() / 2 + 1;
    }

    @Override
    public Role role() {
        return role;
    }

    @Override
    public Term currentTerm() {
        return persistentState.currentTerm();
    }

    @Override
    public Optional<NodeId> currentLeader() {
        return leaderId;
    }

    @Override
    public Observability observability() {
        long lastIdx = log.lastIndex();
        Term lastTerm = Term.ZERO;
        if (lastIdx > 0) {
            try {
                var entries = log.read(lastIdx, 1);
                if (!entries.isEmpty()) lastTerm = entries.get(0).term();
            } catch (Exception ignored) {
                // Observability-only: a missing/truncated last entry falls
                // back to Term.ZERO rather than throwing up the stack.
            }
        }
        return new Observability(
                persistentState.currentTerm().value(),
                commitIndex,
                lastApplied,
                lastIdx,
                lastTerm.value(),
                persistentState.votedFor().map(NodeId::value).orElse(-1));
    }

    @Override
    public List<RaftEffect> step(RaftEvent event) {
        var effects = new ArrayList<RaftEffect>();
        switch (event) {
            case RaftEvent.Tick tick -> onTick(tick, effects);
            case RaftEvent.ClientPropose cp -> onClientPropose(cp, effects);
            case RaftEvent.AppendEntriesReq req -> onAppendEntriesReq(req, effects);
            case RaftEvent.AppendEntriesResp resp -> onAppendEntriesResp(resp, effects);
            case RaftEvent.VoteReq req -> onVoteReq(req, effects);
            case RaftEvent.VoteResp resp -> onVoteResp(resp, effects);
            case RaftEvent.PreVoteReq req -> onPreVoteReq(req, effects);
            case RaftEvent.PreVoteResp resp -> onPreVoteResp(resp, effects);
            case RaftEvent.TransferLeadership t -> onTransferLeadership(t, effects);
            case RaftEvent.TimeoutNow t -> onTimeoutNow(t, effects);
            case RaftEvent.ClientRead r -> onClientRead(r, effects);
            case RaftEvent.ProposeConfigChange c -> onProposeConfigChange(c, effects);
            case RaftEvent.InstallSnapshotReq r -> onInstallSnapshotReq(r, effects);
            case RaftEvent.InstallSnapshotResp r -> onInstallSnapshotResp(r, effects);
            case RaftEvent.TakeSnapshot t -> onTakeSnapshot(t, effects);
        }
        return effects;
    }

    private void onTransferLeadership(RaftEvent.TransferLeadership ev, List<RaftEffect> effects) {
        if (role != Role.LEADER) {
            return;
        }
        if (ev.target().equals(config.selfId())) {
            return;
        }
        effects.add(new RaftEffect.SendTimeoutNow(ev.target(), persistentState.currentTerm()));
    }

    private void onTimeoutNow(RaftEvent.TimeoutNow ev, List<RaftEffect> effects) {
        lastKnownNowNanos = ev.nowNanos();
        var currentTerm = persistentState.currentTerm();
        if (ev.term().compareTo(currentTerm) < 0) {
            return; // stale TimeoutNow from a former leader
        }
        // Catch up to the sender's term first so our next election lands at
        // sender-term + 1 (beating any concurrent candidate at sender-term).
        if (ev.term().compareTo(currentTerm) > 0) {
            becomeFollower(ev.term(), Optional.empty(), effects);
        }
        // A learner cannot win an election — never campaign, even when a
        // (mistaken) leader transfers to it.
        if (!activeVoters.contains(config.selfId())) {
            return;
        }
        // Skip pre-vote — the incumbent leader has already vouched for us.
        startElection(ev.nowNanos(), effects);
    }

    private void onProposeConfigChange(RaftEvent.ProposeConfigChange ev, List<RaftEffect> effects) {
        if (role != Role.LEADER) {
            effects.add(new RaftEffect.RejectConfigChange("not leader"));
            return;
        }
        if (inflightConfigChangeIndex != 0L) {
            effects.add(new RaftEffect.RejectConfigChange("another config change is in flight"));
            return;
        }
        long nextIdx = log.lastIndex() + 1;
        var payload = MembershipCodec.encode(ev.membership());
        var entry = new LogEntry(nextIdx, persistentState.currentTerm(), LogEntry.Type.CONFIG_CHANGE, payload);
        log.append(List.of(entry));
        effects.add(new RaftEffect.PersistLog(List.of(entry)));
        activateMembership(ev.membership(), nextIdx);
        matchIndex.put(config.selfId(), nextIdx);
        for (var peer : replicationMembers()) {
            if (!peer.equals(config.selfId())) {
                nextIndex.putIfAbsent(peer, nextIdx);
                matchIndex.putIfAbsent(peer, 0L);
                sendAppendEntriesTo(peer, effects);
            }
        }
    }

    /**
     * Apply a CONFIG_CHANGE payload to the active membership immediately
     * (append-time semantics). Invoked by the leader on its own propose, and
     * by every node when an AE batch brings a CONFIG_CHANGE entry in.
     */
    private void activateMembership(Membership membership, long entryIndex) {
        this.activeVoters = membership.voters();
        this.activeLearners = membership.learners();
        this.inflightConfigChangeIndex = entryIndex;
    }

    private void onClientRead(RaftEvent.ClientRead ev, List<RaftEffect> effects) {
        if (role != Role.LEADER) {
            effects.add(new RaftEffect.RejectClientRead(ev.clientId(), ev.requestId(), leaderId));
            return;
        }
        // Snapshot the heartbeat-seq barrier BEFORE sending fresh heartbeats.
        // Subsequent sends bump leaderHeartbeatSeq above this value, so only
        // responses to heartbeats sent after the read arrived can satisfy the
        // barrier — ruling out stale in-flight acks (Raft §6.4).
        long barrier = leaderHeartbeatSeq;
        var pending = new PendingRead(ev.clientId(), ev.requestId(), commitIndex, barrier);
        pending.ackSet.add(config.selfId());
        pendingReads.add(pending);
        sendHeartbeats(effects);
        maybeServePendingReads(effects);
    }

    private void maybeServePendingReads(List<RaftEffect> effects) {
        var it = pendingReads.iterator();
        while (it.hasNext()) {
            var pr = it.next();
            if (pr.ackSet.size() >= quorum() && lastApplied >= pr.readIndex) {
                effects.add(new RaftEffect.ServeClientRead(pr.clientId, pr.requestId, pr.readIndex));
                it.remove();
            }
        }
    }

    private void onTick(RaftEvent.Tick tick, List<RaftEffect> effects) {
        long now = tick.nowNanos();
        lastKnownNowNanos = now;
        if (role == Role.LEADER) {
            if (now - lastHeartbeatNanos >= config.heartbeatIntervalNanos()) {
                sendHeartbeats(effects);
                lastHeartbeatNanos = now;
            }
            return;
        }
        if (electionDeadlineNanos == Long.MAX_VALUE) {
            electionDeadlineNanos = now + randomisedElectionTimeout();
            return;
        }
        if (now >= electionDeadlineNanos) {
            // A learner (self not in the voter set) never campaigns — it has
            // no vote and cannot win. Defer the deadline so it keeps waiting
            // for a leader's heartbeats while it catches up.
            if (!activeVoters.contains(config.selfId())) {
                electionDeadlineNanos = now + randomisedElectionTimeout();
                return;
            }
            startPreVote(now, effects);
        }
    }

    private void startPreVote(long now, List<RaftEffect> effects) {
        role = Role.PRE_CANDIDATE;
        leaderId = Optional.empty();
        preVotesReceived.clear();
        preVotesReceived.add(config.selfId());
        electionDeadlineNanos = now + randomisedElectionTimeout();

        long lastIdx = log.lastIndex();
        Term lastTerm = log.termAt(lastIdx).orElse(Term.ZERO);
        Term hypotheticalTerm = persistentState.currentTerm().next();
        for (var peer : activeVoters) {
            if (!peer.equals(config.selfId())) {
                effects.add(new RaftEffect.SendPreVoteReq(peer, hypotheticalTerm, config.selfId(), lastIdx, lastTerm));
            }
        }
        maybeFinishPreVote(now, effects);
    }

    private void maybeFinishPreVote(long now, List<RaftEffect> effects) {
        if (preVotesReceived.size() >= quorum() && role == Role.PRE_CANDIDATE) {
            // Pre-vote succeeded — now do the real election at term+1.
            startElection(now, effects);
        }
    }

    private void startElection(long now, List<RaftEffect> effects) {
        role = Role.CANDIDATE;
        leaderId = Optional.empty();
        var newTerm = persistentState.currentTerm().next();
        persistentState.update(newTerm, Optional.of(config.selfId()));
        effects.add(new RaftEffect.PersistState(newTerm, Optional.of(config.selfId())));

        votesReceived.clear();
        votesReceived.add(config.selfId());
        electionDeadlineNanos = now + randomisedElectionTimeout();

        long lastIdx = log.lastIndex();
        Term lastTerm = log.termAt(lastIdx).orElse(Term.ZERO);
        for (var peer : activeVoters) {
            if (!peer.equals(config.selfId())) {
                effects.add(new RaftEffect.SendVoteReq(peer, newTerm, config.selfId(), lastIdx, lastTerm));
            }
        }
        maybeFinishElection(effects);
    }

    private void sendHeartbeats(List<RaftEffect> effects) {
        for (var peer : replicationMembers()) {
            if (!peer.equals(config.selfId())) {
                sendAppendEntriesTo(peer, effects);
            }
        }
    }

    private void maybeFinishElection(List<RaftEffect> effects) {
        if (votesReceived.size() >= quorum() && role == Role.CANDIDATE) {
            role = Role.LEADER;
            long lastIdx = log.lastIndex();
            for (var peer : replicationMembers()) {
                if (!peer.equals(config.selfId())) {
                    nextIndex.put(peer, lastIdx + 1);
                    matchIndex.put(peer, 0L);
                }
            }
            // Append a NO_OP at the new leader's term (Raft §8 / §5.4.2).
            // Without this, previous-term entries can never commit on a
            // restarted cluster — the commit-safety rule forbids counting
            // replicas for entries whose term != currentTerm. The NO_OP's
            // own commit transitively advances commitIndex past every
            // preceding entry, unlocking apply.
            long noOpIdx = lastIdx + 1;
            var noOp = new LogEntry(noOpIdx, persistentState.currentTerm(), LogEntry.Type.NO_OP, new byte[0]);
            log.append(List.of(noOp));
            effects.add(new RaftEffect.PersistLog(List.of(noOp)));
            matchIndex.put(config.selfId(), noOpIdx);
            // Force an immediate heartbeat from the next Tick: we don't have
            // 'now' here, so park lastHeartbeatNanos far enough in the past
            // that (now - lastHeartbeatNanos) >= heartbeatInterval regardless
            // of the nowNanos the Tick carries. Using MIN_VALUE/2 keeps the
            // delta from overflowing if now is itself negative.
            lastHeartbeatNanos = Long.MIN_VALUE / 2;
            // Single-node: no peers will ack the NO_OP, so advance commit
            // now. Multi-node: this is a no-op (no peer matchIndex yet).
            maybeAdvanceLeaderCommit(effects);
        }
    }

    private void onClientPropose(RaftEvent.ClientPropose event, List<RaftEffect> effects) {
        if (role != Role.LEADER) {
            effects.add(new RaftEffect.RejectClientPropose(leaderId));
            return;
        }
        // Dedup: clientId == 0 opts out. Otherwise, if this (clientId, seq)
        // has already been accepted on this leader, short-circuit.
        if (event.clientId() != 0L) {
            Long prev = highestAcceptedSeq.get(event.clientId());
            if (prev != null && event.clientSeq() <= prev) {
                effects.add(new RaftEffect.DuplicateClientPropose(event.clientId(), event.clientSeq()));
                return;
            }
        }
        long nextIdx = log.lastIndex() + 1;
        var entry = new LogEntry(nextIdx, persistentState.currentTerm(), LogEntry.Type.NORMAL, event.payload());
        log.append(List.of(entry));
        effects.add(new RaftEffect.PersistLog(List.of(entry)));
        matchIndex.put(config.selfId(), nextIdx);
        if (event.clientId() != 0L) {
            highestAcceptedSeq.merge(event.clientId(), event.clientSeq(), Math::max);
        }
        for (var peer : replicationMembers()) {
            if (!peer.equals(config.selfId())) {
                sendAppendEntriesTo(peer, effects);
            }
        }
        // Single-node clusters have no peers to return AE responses, so
        // maybeAdvanceLeaderCommit would otherwise never fire. Invoke it
        // inline so the entry commits immediately — in multi-node this is
        // harmless (no peer matchIndex has advanced yet, so count == 1 <
        // quorum and nothing changes).
        maybeAdvanceLeaderCommit(effects);
    }

    private void onAppendEntriesReq(RaftEvent.AppendEntriesReq req, List<RaftEffect> effects) {
        lastKnownNowNanos = req.nowNanos();
        var currentTerm = persistentState.currentTerm();

        if (req.term().compareTo(currentTerm) < 0) {
            effects.add(new RaftEffect.SendAppendEntriesResp(
                    req.leaderId(), currentTerm, false, 0L, Term.ZERO, 0L, req.heartbeatSeq()));
            return;
        }

        if (req.term().compareTo(currentTerm) > 0) {
            becomeFollower(req.term(), Optional.empty(), effects);
            currentTerm = req.term();
        } else if (role != Role.FOLLOWER) {
            role = Role.FOLLOWER;
            votesReceived.clear();
            preVotesReceived.clear();
            nextIndex.clear();
            matchIndex.clear();
        }

        leaderId = Optional.of(req.leaderId());
        electionDeadlineNanos = req.nowNanos() + randomisedElectionTimeout();

        if (req.prevLogIndex() > 0) {
            var localTerm = log.termAt(req.prevLogIndex());
            if (localTerm.isEmpty()) {
                effects.add(new RaftEffect.SendAppendEntriesResp(
                        req.leaderId(), currentTerm, false, log.lastIndex() + 1, Term.ZERO, 0L, req.heartbeatSeq()));
                return;
            }
            if (!localTerm.get().equals(req.prevLogTerm())) {
                Term conflictTerm = localTerm.get();
                long firstIdx = firstIndexOfTerm(conflictTerm, req.prevLogIndex());
                effects.add(new RaftEffect.SendAppendEntriesResp(
                        req.leaderId(), currentTerm, false, firstIdx, conflictTerm, 0L, req.heartbeatSeq()));
                return;
            }
        }

        if (!req.entries().isEmpty()) {
            boolean truncated = false;
            for (var entry : req.entries()) {
                var existingTerm = log.termAt(entry.index());
                if (existingTerm.isPresent() && !existingTerm.get().equals(entry.term())) {
                    log.truncateFrom(entry.index());
                    effects.add(new RaftEffect.TruncateLog(entry.index()));
                    truncated = true;
                    break;
                }
            }
            // If the truncated suffix contained an active CONFIG_CHANGE, our
            // in-memory activeVoters / inflightConfigChangeIndex now lie about
            // what the log says. Rebuild from whatever survived the truncate.
            if (truncated) {
                reloadActiveVotersFromLog();
                inflightConfigChangeIndex = 0L;
            }
            var toAppend = req.entries().stream()
                    .filter(e -> e.index() > log.lastIndex())
                    .toList();
            if (!toAppend.isEmpty()) {
                log.append(toAppend);
                effects.add(new RaftEffect.PersistLog(toAppend));
                // Append-time membership: any CONFIG_CHANGE in this batch
                // replaces the active voter set immediately. The last one wins.
                for (var e : toAppend) {
                    if (e.type() == LogEntry.Type.CONFIG_CHANGE) {
                        activateMembership(MembershipCodec.decodeMembership(e.payload()), e.index());
                    }
                }
            }
        }

        long matchIdx = req.prevLogIndex() + req.entries().size();

        if (req.leaderCommit() > commitIndex) {
            long newCommit = Math.min(req.leaderCommit(), matchIdx);
            advanceCommit(newCommit, effects);
        }

        effects.add(new RaftEffect.SendAppendEntriesResp(
                req.leaderId(), currentTerm, true, 0L, Term.ZERO, matchIdx, req.heartbeatSeq()));
    }

    private long firstIndexOfTerm(Term term, long upperBound) {
        for (long i = 1; i <= upperBound; i++) {
            if (log.termAt(i).map(t -> t.equals(term)).orElse(false)) {
                return i;
            }
        }
        return 1L;
    }

    /** Returns the highest index whose entry has the given term, or 0 if none. */
    private long lastIndexOfTerm(Term term) {
        for (long i = log.lastIndex(); i >= 1; i--) {
            if (log.termAt(i).map(t -> t.equals(term)).orElse(false)) {
                return i;
            }
        }
        return 0L;
    }

    private void advanceCommit(long newCommit, List<RaftEffect> effects) {
        while (lastApplied < newCommit) {
            lastApplied++;
            var entry = log.read(lastApplied, 1).get(0);
            effects.add(new RaftEffect.ApplyCommitted(entry));
        }
        commitIndex = newCommit;
        maybeResolveConfigChange(effects);
    }

    /**
     * Called whenever {@code commitIndex} advances. If the in-flight config
     * change has now committed, clear the marker. If the committed change
     * removed this node from the voter set, step down — Raft §4.2.2 says a
     * leader removing itself must wait until the change commits before
     * relinquishing leadership, but should then step down.
     */
    private void maybeResolveConfigChange(List<RaftEffect> effects) {
        if (inflightConfigChangeIndex == 0L || commitIndex < inflightConfigChangeIndex) {
            return;
        }
        inflightConfigChangeIndex = 0L;
        if (role == Role.LEADER && !activeVoters.contains(config.selfId())) {
            becomeFollower(persistentState.currentTerm(), persistentState.votedFor(), effects);
        }
    }

    private void onAppendEntriesResp(RaftEvent.AppendEntriesResp resp, List<RaftEffect> effects) {
        if (role != Role.LEADER) {
            return;
        }
        var currentTerm = persistentState.currentTerm();
        if (resp.term().compareTo(currentTerm) > 0) {
            becomeFollower(resp.term(), Optional.empty(), effects);
            return;
        }
        if (!resp.term().equals(currentTerm)) {
            return;
        }

        if (resp.success()) {
            matchIndex.put(resp.from(), resp.matchIndex());
            nextIndex.put(resp.from(), resp.matchIndex() + 1);
            // Only responses to heartbeats sent AFTER a pending read was
            // registered count toward its quorum. Without this, a stale
            // in-flight AE response predating the ClientRead could falsely
            // confirm leadership while a new leader has already taken over —
            // breaking linearizability (Raft §6.4).
            for (var pr : pendingReads) {
                if (resp.heartbeatSeq() > pr.barrierSeq) {
                    pr.ackSet.add(resp.from());
                }
            }
            maybeAdvanceLeaderCommit(effects);
            maybeServePendingReads(effects);
        } else {
            long newNext;
            if (resp.conflictTerm().equals(Term.ZERO)) {
                // Follower's log is too short; use the conflict index directly.
                newNext = Math.max(1, resp.conflictIndex());
            } else {
                // Figure-8 optimisation: if the leader has entries at the
                // follower's conflictTerm, jump past the last one rather than
                // backing off one entry at a time.  If it doesn't, defer to
                // the follower's conflictIndex.
                long lastIdxOfTerm = lastIndexOfTerm(resp.conflictTerm());
                newNext = lastIdxOfTerm > 0 ? lastIdxOfTerm + 1 : Math.max(1, resp.conflictIndex());
            }
            nextIndex.put(resp.from(), newNext);
            sendAppendEntriesTo(resp.from(), effects);
        }
    }

    private void maybeAdvanceLeaderCommit(List<RaftEffect> effects) {
        var currentTerm = persistentState.currentTerm();
        long lastIdx = log.lastIndex();
        // Iterate from highest index downward; the first index at which a
        // majority has matched is necessarily the largest such index, so we
        // can stop the scan immediately after advancing commitIndex.
        for (long n = lastIdx; n > commitIndex; n--) {
            Term entryTerm = log.termAt(n).orElse(Term.ZERO);
            if (!entryTerm.equals(currentTerm)) {
                continue;
            }
            // Count the leader only if it's still a voter. A leader that has
            // removed itself from the config (Raft §4.2.2) must not contribute
            // to its own majority — otherwise self-removal commits prematurely
            // on one follower ack instead of the new quorum.
            int count = activeVoters.contains(config.selfId()) ? 1 : 0;
            for (var peer : activeVoters) {
                if (peer.equals(config.selfId())) continue;
                if (matchIndex.getOrDefault(peer, 0L) >= n) {
                    count++;
                }
            }
            if (count >= quorum()) {
                advanceCommit(n, effects);
                break;
            }
        }
    }

    private void sendAppendEntriesTo(NodeId peer, List<RaftEffect> effects) {
        long next = nextIndex.getOrDefault(peer, log.lastIndex() + 1);
        // If the peer has fallen behind past our compaction point, AE can't
        // catch it up — the entries it needs no longer exist. Fall back to
        // InstallSnapshot (Raft §7).
        if (next <= log.lastIncludedIndex() && log.lastIncludedIndex() > 0) {
            if (currentSnapshotBytes != null) {
                effects.add(new RaftEffect.SendInstallSnapshot(
                        peer,
                        persistentState.currentTerm(),
                        config.selfId(),
                        log.lastIncludedIndex(),
                        log.lastIncludedTerm(),
                        currentSnapshotBytes));
            }
            // If no snapshot bytes are cached yet, the driver should call
            // installSnapshotBytesForTesting / the production equivalent
            // before the next tick. Dropping this heartbeat is acceptable;
            // the peer will retry.
            return;
        }
        long prevIndex = next - 1;
        Term prevTerm = prevIndex == 0 ? Term.ZERO : log.termAt(prevIndex).orElse(Term.ZERO);
        var batch = log.read(next, config.maxEntriesPerAppend());
        long seq = ++leaderHeartbeatSeq;
        effects.add(new RaftEffect.SendAppendEntries(
                peer, persistentState.currentTerm(), config.selfId(), prevIndex, prevTerm, batch, commitIndex, seq));
    }

    /**
     * Hook for tests / drivers to seed the snapshot bytes used by the leader
     * when a peer falls behind the log-compaction point. In production, the
     * driver calls this after taking a state-machine snapshot.
     */
    public void installSnapshotBytesForTesting(byte[] bytes) {
        this.currentSnapshotBytes = bytes;
    }

    private void onInstallSnapshotReq(RaftEvent.InstallSnapshotReq req, List<RaftEffect> effects) {
        lastKnownNowNanos = req.nowNanos();
        var currentTerm = persistentState.currentTerm();
        if (req.term().compareTo(currentTerm) < 0) {
            effects.add(new RaftEffect.SendInstallSnapshotResp(req.leaderId(), currentTerm));
            return;
        }
        if (req.term().compareTo(currentTerm) > 0) {
            becomeFollower(req.term(), Optional.empty(), effects);
            currentTerm = req.term();
        } else if (role != Role.FOLLOWER) {
            role = Role.FOLLOWER;
            votesReceived.clear();
            preVotesReceived.clear();
            nextIndex.clear();
            matchIndex.clear();
        }
        leaderId = Optional.of(req.leaderId());
        electionDeadlineNanos = req.nowNanos() + randomisedElectionTimeout();

        long snapIdx = req.lastIncludedIndex();
        Term snapTerm = req.lastIncludedTerm();
        // Protective guard: if we've already applied past the snapshot, it's
        // stale relative to our progress — ack but don't rewind state. The
        // guard here is what makes the "drop the entire log" branch below
        // safe: by the time we reach the drop, snapIdx > lastApplied, so any
        // entries we throw away at indices in (snapIdx, lastIndex] were only
        // ever locally appended, never committed cluster-wide (otherwise the
        // new leader's election would have required those entries too).
        if (snapIdx <= lastApplied) {
            effects.add(new RaftEffect.SendInstallSnapshotResp(req.leaderId(), currentTerm));
            return;
        }
        // Truncate log prefix up to the snapshot point; if our log has an
        // entry at snapIdx with matching term, we keep the suffix. Otherwise
        // drop everything (the snapshot supersedes our divergent log).
        var ourTermAtSnap = log.termAt(snapIdx);
        if (ourTermAtSnap.isPresent() && ourTermAtSnap.get().equals(snapTerm)) {
            log.truncatePrefix(snapIdx + 1, snapTerm);
        } else {
            if (log.lastIndex() >= log.firstIndex()) {
                log.truncateFrom(log.firstIndex());
            }
            log.truncatePrefix(snapIdx + 1, snapTerm);
        }
        commitIndex = Math.max(commitIndex, snapIdx);
        lastApplied = Math.max(lastApplied, snapIdx);

        effects.add(new RaftEffect.ApplySnapshot(snapIdx, snapTerm, req.snapshot()));
        effects.add(new RaftEffect.SendInstallSnapshotResp(req.leaderId(), currentTerm));
    }

    private void onTakeSnapshot(RaftEvent.TakeSnapshot ev, List<RaftEffect> effects) {
        // No-op if nothing is committed yet; there's nothing to compact.
        if (commitIndex <= 0) {
            this.currentSnapshotBytes = ev.bytes();
            return;
        }
        var termAtCommit = log.termAt(commitIndex);
        if (termAtCommit.isEmpty()) {
            // The commitIndex has already been compacted by an earlier
            // snapshot; just refresh the bytes.
            this.currentSnapshotBytes = ev.bytes();
            return;
        }
        log.truncatePrefix(commitIndex + 1, termAtCommit.get());
        this.currentSnapshotBytes = ev.bytes();
    }

    private void onInstallSnapshotResp(RaftEvent.InstallSnapshotResp resp, List<RaftEffect> effects) {
        if (role != Role.LEADER) {
            return;
        }
        var currentTerm = persistentState.currentTerm();
        if (resp.term().compareTo(currentTerm) > 0) {
            becomeFollower(resp.term(), Optional.empty(), effects);
            return;
        }
        if (!resp.term().equals(currentTerm)) {
            return;
        }
        // Peer now has everything up through our lastIncludedIndex; advance
        // nextIndex and keep going with normal AE.
        long lastIncluded = log.lastIncludedIndex();
        nextIndex.put(resp.from(), lastIncluded + 1);
        matchIndex.put(resp.from(), lastIncluded);
        sendAppendEntriesTo(resp.from(), effects);
    }

    private void onVoteReq(RaftEvent.VoteReq req, List<RaftEffect> effects) {
        lastKnownNowNanos = req.nowNanos();
        var currentTerm = persistentState.currentTerm();
        if (req.term().compareTo(currentTerm) < 0) {
            effects.add(new RaftEffect.SendVoteResp(req.candidateId(), currentTerm, false));
            return;
        }
        if (req.term().compareTo(currentTerm) > 0) {
            becomeFollower(req.term(), Optional.empty(), effects);
            currentTerm = req.term();
        } else if (role == Role.PRE_CANDIDATE) {
            // Same-term real vote arriving — abandon pre-vote attempt.
            role = Role.FOLLOWER;
            preVotesReceived.clear();
        }

        var votedFor = persistentState.votedFor();
        boolean alreadyVotedForSomeoneElse =
                votedFor.isPresent() && !votedFor.get().equals(req.candidateId());
        boolean logUpToDate = candidateLogUpToDate(req.lastLogIndex(), req.lastLogTerm());

        if (!alreadyVotedForSomeoneElse && logUpToDate) {
            persistentState.update(currentTerm, Optional.of(req.candidateId()));
            effects.add(new RaftEffect.PersistState(currentTerm, Optional.of(req.candidateId())));
            effects.add(new RaftEffect.SendVoteResp(req.candidateId(), currentTerm, true));
            electionDeadlineNanos = req.nowNanos() + randomisedElectionTimeout();
        } else {
            effects.add(new RaftEffect.SendVoteResp(req.candidateId(), currentTerm, false));
        }
    }

    private boolean candidateLogUpToDate(long candidateLastIdx, Term candidateLastTerm) {
        long localLast = log.lastIndex();
        Term localLastTerm = log.termAt(localLast).orElse(Term.ZERO);
        int cmp = candidateLastTerm.compareTo(localLastTerm);
        if (cmp != 0) {
            return cmp > 0;
        }
        return candidateLastIdx >= localLast;
    }

    private void onPreVoteReq(RaftEvent.PreVoteReq req, List<RaftEffect> effects) {
        lastKnownNowNanos = req.nowNanos();
        var currentTerm = persistentState.currentTerm();

        // Deny if we are the leader (we clearly have a current leader — ourselves).
        // Deny if the hypothetical term is not newer than what we've already seen.
        // Deny if the candidate's log is not at least as up-to-date as ours.
        // "No current leader" check: either we've never heard of one (leaderId
        // empty at startup, or after becomeFollower), or we have but our election
        // deadline has elapsed since the last heartbeat (so we'd time out
        // ourselves soon anyway). The initial cluster bring-up case — all nodes
        // start with non-elapsed deadlines but leaderId.isEmpty() — falls into
        // the first branch so the first pre-vote isn't denied purely on timing.
        boolean termOk = req.term().compareTo(currentTerm) > 0;
        boolean noCurrentLeader = leaderId.isEmpty()
                || (electionDeadlineNanos != Long.MAX_VALUE && req.nowNanos() >= electionDeadlineNanos);
        boolean logUpToDate = candidateLogUpToDate(req.lastLogIndex(), req.lastLogTerm());
        boolean grant = role != Role.LEADER && termOk && noCurrentLeader && logUpToDate;
        effects.add(new RaftEffect.SendPreVoteResp(req.candidateId(), currentTerm, grant));
        // Critically, no state mutation — no term bump, no votedFor
        // write, no deadline reset. That's the whole point of pre-vote:
        // a disruptive node that fails pre-vote has not perturbed the
        // cluster. A tempting "reset deadline on grant" optimisation
        // introduces a liveness bug in exactly this scenario: if the
        // pre-voter's responses keep getting lost (one-way network
        // fault, OS socket-buffer drop), the granting node keeps
        // resetting its own deadline and never runs its own election.
        // Observed concretely on CI: broker 2 did not start pre-vote
        // for >5s after the Raft leader died because it kept granting
        // the dead peer's retries while its own responses never
        // reached the caller.
    }

    private void onPreVoteResp(RaftEvent.PreVoteResp resp, List<RaftEffect> effects) {
        if (role != Role.PRE_CANDIDATE) {
            return;
        }
        var currentTerm = persistentState.currentTerm();
        if (resp.term().compareTo(currentTerm) > 0) {
            // Peer is ahead; give up pre-vote and step down to catch up.
            becomeFollower(resp.term(), Optional.empty(), effects);
            return;
        }
        if (resp.granted()) {
            preVotesReceived.add(resp.from());
            maybeFinishPreVote(lastKnownNowNanos, effects);
        }
    }

    private void onVoteResp(RaftEvent.VoteResp resp, List<RaftEffect> effects) {
        if (role != Role.CANDIDATE) {
            return;
        }
        var currentTerm = persistentState.currentTerm();
        if (resp.term().compareTo(currentTerm) > 0) {
            becomeFollower(resp.term(), Optional.empty(), effects);
            return;
        }
        if (!resp.term().equals(currentTerm)) {
            return;
        }
        if (resp.granted()) {
            votesReceived.add(resp.from());
            maybeFinishElection(effects);
        }
    }

    private void becomeFollower(Term newTerm, Optional<NodeId> newVote, List<RaftEffect> effects) {
        var oldTerm = persistentState.currentTerm();
        role = Role.FOLLOWER;
        leaderId = Optional.empty();
        votesReceived.clear();
        preVotesReceived.clear();
        nextIndex.clear();
        matchIndex.clear();
        // Leader-side config-change bookkeeping resets; if we become leader
        // again later we'll rediscover any uncommitted CONFIG_CHANGE from the
        // log when processing it.
        inflightConfigChangeIndex = 0L;
        for (var pr : pendingReads) {
            effects.add(new RaftEffect.RejectClientRead(pr.clientId, pr.requestId, Optional.empty()));
        }
        pendingReads.clear();
        persistentState.update(newTerm, newVote);
        effects.add(new RaftEffect.PersistState(newTerm, newVote));
        electionDeadlineNanos = Long.MAX_VALUE;
        emitTermChange(oldTerm, newTerm, "becomeFollower");
    }

    private void emitTermChange(Term oldTerm, Term newTerm, String reason) {
        if (oldTerm.value() == newTerm.value()) return;
        var event = new jbroker.raft.core.jfr.RaftTermChangeEvent();
        if (event.shouldCommit()) {
            event.oldTerm = oldTerm.value();
            event.newTerm = newTerm.value();
            event.reason = reason;
            event.commit();
        }
    }

    private long randomisedElectionTimeout() {
        long bound = config.electionJitterNanos() + 1;
        long jitter = (random == null) ? ThreadLocalRandom.current().nextLong(0, bound) : nextLongBounded(bound);
        return config.electionTimeoutNanos() + jitter;
    }

    private long nextLongBounded(long bound) {
        // java.util.Random has nextInt(bound) but not nextLong(bound); emulate
        // for long bounds we expect (well under Integer.MAX_VALUE for normal
        // timeouts, but cast defensively).
        if (bound <= Integer.MAX_VALUE) {
            return random.nextInt((int) bound);
        }
        return (random.nextLong() & Long.MAX_VALUE) % bound;
    }

    long commitIndex() {
        return commitIndex;
    }

    long lastApplied() {
        return lastApplied;
    }
}
