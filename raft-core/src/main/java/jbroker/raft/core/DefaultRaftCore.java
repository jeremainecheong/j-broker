package jbroker.raft.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public final class DefaultRaftCore implements RaftCore {

    private final RaftConfig config;
    private final RaftLog log;
    private final PersistentState persistentState;

    private Role role = Role.FOLLOWER;
    private Optional<NodeId> leaderId = Optional.empty();
    private long commitIndex;
    private long lastApplied;
    private long electionDeadlineNanos;
    private long lastHeartbeatNanos;

    private final Set<NodeId> votesReceived = new HashSet<>();
    private final Map<NodeId, Long> nextIndex = new HashMap<>();
    private final Map<NodeId, Long> matchIndex = new HashMap<>();

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
        this.config = Objects.requireNonNull(config, "config");
        this.log = Objects.requireNonNull(log, "log");
        this.persistentState = Objects.requireNonNull(persistentState, "persistentState");
        // Long.MAX_VALUE is a sentinel meaning "defer the first election until the
        // first real Tick arrives" (the onTick sentinel path handles this).
        this.electionDeadlineNanos =
                (nowNanos == Long.MAX_VALUE) ? Long.MAX_VALUE : nowNanos + randomisedElectionTimeout();
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
    public List<RaftEffect> step(RaftEvent event) {
        var effects = new ArrayList<RaftEffect>();
        switch (event) {
            case RaftEvent.Tick tick -> onTick(tick, effects);
            case RaftEvent.ClientPropose cp -> onClientPropose(cp, effects);
            case RaftEvent.AppendEntriesReq req -> onAppendEntriesReq(req, effects);
            case RaftEvent.AppendEntriesResp resp -> onAppendEntriesResp(resp, effects);
            case RaftEvent.VoteReq req -> onVoteReq(req, effects);
            case RaftEvent.VoteResp resp -> onVoteResp(resp, effects);
        }
        return effects;
    }

    private void onTick(RaftEvent.Tick tick, List<RaftEffect> effects) {
        long now = tick.nowNanos();
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
        for (var peer : config.voters()) {
            if (!peer.equals(config.selfId())) {
                effects.add(new RaftEffect.SendVoteReq(peer, newTerm, config.selfId(), lastIdx, lastTerm));
            }
        }
        maybeFinishElection(effects);
    }

    private void sendHeartbeats(List<RaftEffect> effects) {
        for (var peer : config.voters()) {
            if (!peer.equals(config.selfId())) {
                sendAppendEntriesTo(peer, effects);
            }
        }
    }

    private void maybeFinishElection(List<RaftEffect> effects) {
        if (votesReceived.size() >= config.quorum() && role == Role.CANDIDATE) {
            role = Role.LEADER;
            long lastIdx = log.lastIndex();
            for (var peer : config.voters()) {
                if (!peer.equals(config.selfId())) {
                    nextIndex.put(peer, lastIdx + 1);
                    matchIndex.put(peer, 0L);
                }
            }
            lastHeartbeatNanos = Long.MIN_VALUE / 2;
        }
    }

    private void onClientPropose(RaftEvent.ClientPropose event, List<RaftEffect> effects) {
        if (role != Role.LEADER) {
            effects.add(new RaftEffect.RejectClientPropose(leaderId));
            return;
        }
        long nextIdx = log.lastIndex() + 1;
        var entry = new LogEntry(nextIdx, persistentState.currentTerm(), LogEntry.Type.NORMAL, event.payload());
        log.append(List.of(entry));
        effects.add(new RaftEffect.PersistLog(List.of(entry)));
        matchIndex.put(config.selfId(), nextIdx);
        for (var peer : config.voters()) {
            if (!peer.equals(config.selfId())) {
                sendAppendEntriesTo(peer, effects);
            }
        }
    }

    private void onAppendEntriesReq(RaftEvent.AppendEntriesReq req, List<RaftEffect> effects) {
        var currentTerm = persistentState.currentTerm();

        if (req.term().compareTo(currentTerm) < 0) {
            effects.add(new RaftEffect.SendAppendEntriesResp(req.leaderId(), currentTerm, false, 0L, Term.ZERO, 0L));
            return;
        }

        if (req.term().compareTo(currentTerm) > 0) {
            becomeFollower(req.term(), Optional.empty(), effects);
            currentTerm = req.term();
        } else if (role != Role.FOLLOWER) {
            role = Role.FOLLOWER;
            votesReceived.clear();
            nextIndex.clear();
            matchIndex.clear();
        }

        leaderId = Optional.of(req.leaderId());
        electionDeadlineNanos = Long.MAX_VALUE;

        if (req.prevLogIndex() > 0) {
            var localTerm = log.termAt(req.prevLogIndex());
            if (localTerm.isEmpty()) {
                effects.add(new RaftEffect.SendAppendEntriesResp(
                        req.leaderId(), currentTerm, false, log.lastIndex() + 1, Term.ZERO, 0L));
                return;
            }
            if (!localTerm.get().equals(req.prevLogTerm())) {
                Term conflictTerm = localTerm.get();
                long firstIdx = firstIndexOfTerm(conflictTerm, req.prevLogIndex());
                effects.add(new RaftEffect.SendAppendEntriesResp(
                        req.leaderId(), currentTerm, false, firstIdx, conflictTerm, 0L));
                return;
            }
        }

        if (!req.entries().isEmpty()) {
            for (var entry : req.entries()) {
                var existingTerm = log.termAt(entry.index());
                if (existingTerm.isPresent() && !existingTerm.get().equals(entry.term())) {
                    log.truncateFrom(entry.index());
                    effects.add(new RaftEffect.TruncateLog(entry.index()));
                    break;
                }
            }
            var toAppend = req.entries().stream()
                    .filter(e -> e.index() > log.lastIndex())
                    .toList();
            if (!toAppend.isEmpty()) {
                log.append(toAppend);
                effects.add(new RaftEffect.PersistLog(toAppend));
            }
        }

        long matchIdx = req.prevLogIndex() + req.entries().size();

        if (req.leaderCommit() > commitIndex) {
            long newCommit = Math.min(req.leaderCommit(), matchIdx);
            advanceCommit(newCommit, effects);
        }

        effects.add(new RaftEffect.SendAppendEntriesResp(req.leaderId(), currentTerm, true, 0L, Term.ZERO, matchIdx));
    }

    private long firstIndexOfTerm(Term term, long upperBound) {
        for (long i = 1; i <= upperBound; i++) {
            if (log.termAt(i).map(t -> t.equals(term)).orElse(false)) {
                return i;
            }
        }
        return 1L;
    }

    private void advanceCommit(long newCommit, List<RaftEffect> effects) {
        while (lastApplied < newCommit) {
            lastApplied++;
            var entry = log.read(lastApplied, 1).get(0);
            effects.add(new RaftEffect.ApplyCommitted(entry));
        }
        commitIndex = newCommit;
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
            maybeAdvanceLeaderCommit(effects);
        } else {
            long newNext = Math.max(1, resp.conflictIndex());
            nextIndex.put(resp.from(), newNext);
            sendAppendEntriesTo(resp.from(), effects);
        }
    }

    private void maybeAdvanceLeaderCommit(List<RaftEffect> effects) {
        var currentTerm = persistentState.currentTerm();
        long lastIdx = log.lastIndex();
        for (long n = lastIdx; n > commitIndex; n--) {
            Term entryTerm = log.termAt(n).orElse(Term.ZERO);
            if (!entryTerm.equals(currentTerm)) {
                continue;
            }
            int count = 1; // leader
            for (var peer : config.voters()) {
                if (peer.equals(config.selfId())) continue;
                if (matchIndex.getOrDefault(peer, 0L) >= n) {
                    count++;
                }
            }
            if (count >= config.quorum()) {
                advanceCommit(n, effects);
                break;
            }
        }
    }

    private void sendAppendEntriesTo(NodeId peer, List<RaftEffect> effects) {
        long next = nextIndex.getOrDefault(peer, log.lastIndex() + 1);
        long prevIndex = next - 1;
        Term prevTerm = prevIndex == 0 ? Term.ZERO : log.termAt(prevIndex).orElse(Term.ZERO);
        var batch = log.read(next, config.maxEntriesPerAppend());
        effects.add(new RaftEffect.SendAppendEntries(
                peer, persistentState.currentTerm(), config.selfId(), prevIndex, prevTerm, batch, commitIndex));
    }

    private void onVoteReq(RaftEvent.VoteReq req, List<RaftEffect> effects) {
        var currentTerm = persistentState.currentTerm();
        if (req.term().compareTo(currentTerm) < 0) {
            effects.add(new RaftEffect.SendVoteResp(req.candidateId(), currentTerm, false));
            return;
        }
        if (req.term().compareTo(currentTerm) > 0) {
            becomeFollower(req.term(), Optional.empty(), effects);
            currentTerm = req.term();
        }

        var votedFor = persistentState.votedFor();
        boolean alreadyVotedForSomeoneElse =
                votedFor.isPresent() && !votedFor.get().equals(req.candidateId());
        boolean logUpToDate = candidateLogUpToDate(req.lastLogIndex(), req.lastLogTerm());

        if (!alreadyVotedForSomeoneElse && logUpToDate) {
            persistentState.update(currentTerm, Optional.of(req.candidateId()));
            effects.add(new RaftEffect.PersistState(currentTerm, Optional.of(req.candidateId())));
            effects.add(new RaftEffect.SendVoteResp(req.candidateId(), currentTerm, true));
            electionDeadlineNanos = Long.MAX_VALUE;
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
        role = Role.FOLLOWER;
        leaderId = Optional.empty();
        votesReceived.clear();
        nextIndex.clear();
        matchIndex.clear();
        persistentState.update(newTerm, newVote);
        effects.add(new RaftEffect.PersistState(newTerm, newVote));
        electionDeadlineNanos = Long.MAX_VALUE;
    }

    private long randomisedElectionTimeout() {
        long jitter = ThreadLocalRandom.current().nextLong(0, config.electionJitterNanos() + 1);
        return config.electionTimeoutNanos() + jitter;
    }

    long commitIndex() {
        return commitIndex;
    }

    long lastApplied() {
        return lastApplied;
    }
}
