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
        // Task 25
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
        // Task 26
    }

    private void onVoteResp(RaftEvent.VoteResp resp, List<RaftEffect> effects) {
        // Task 26
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
