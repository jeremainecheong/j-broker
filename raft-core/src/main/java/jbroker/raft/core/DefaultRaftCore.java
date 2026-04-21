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

    public DefaultRaftCore(RaftConfig config, RaftLog log, PersistentState persistentState, long nowNanos) {
        this.config = Objects.requireNonNull(config, "config");
        this.log = Objects.requireNonNull(log, "log");
        this.persistentState = Objects.requireNonNull(persistentState, "persistentState");
        this.electionDeadlineNanos = nowNanos + randomisedElectionTimeout();
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
        // Task 28/29
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
            lastHeartbeatNanos = Long.MIN_VALUE;
        }
    }

    private void onClientPropose(RaftEvent.ClientPropose event, List<RaftEffect> effects) {
        // Tasks 28/30
    }

    private void onAppendEntriesReq(RaftEvent.AppendEntriesReq req, List<RaftEffect> effects) {
        // Task 27
    }

    private void onAppendEntriesResp(RaftEvent.AppendEntriesResp resp, List<RaftEffect> effects) {
        // Task 28
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
