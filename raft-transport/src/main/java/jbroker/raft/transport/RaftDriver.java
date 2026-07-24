package jbroker.raft.transport;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import jbroker.proto.raft.AppendEntriesRequest;
import jbroker.proto.raft.AppendEntriesResponse;
import jbroker.proto.raft.EntryType;
import jbroker.proto.raft.RequestVoteRequest;
import jbroker.proto.raft.RequestVoteResponse;
import jbroker.raft.core.Clock;
import jbroker.raft.core.MonotonicClock;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.RaftCore;
import jbroker.raft.core.RaftEffect;
import jbroker.raft.core.RaftEvent;
import jbroker.raft.core.Role;
import jbroker.raft.core.StateMachine;
import jbroker.raft.core.Term;
import jbroker.tls.TlsConfig;
import jbroker.tls.TlsContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hosts a single {@link RaftCore}. Owns: an event-pump virtual thread, a
 * periodic ticker virtual thread, a gRPC server, and a map of peer clients.
 *
 * <p>All state mutations happen on the pump thread; gRPC handlers are
 * synchronous — they submit a request event and wait on a future for the
 * resulting response effect.
 */
public final class RaftDriver implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RaftDriver.class);

    private final NodeId selfId;
    private final RaftCore core;
    private final StateMachine stateMachine;
    private final Map<NodeId, RaftPeerClient> peers;
    private final Clock clock;
    private final long tickIntervalNanos;

    private final BlockingQueue<PendingEvent> queue = new LinkedBlockingQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread pumpThread;
    private Thread tickerThread;
    private Server grpcServer;

    public RaftDriver(
            NodeId selfId,
            RaftCore core,
            StateMachine stateMachine,
            Map<NodeId, RaftPeerClient> peers,
            long tickIntervalNanos) {
        this(selfId, core, stateMachine, peers, tickIntervalNanos, new MonotonicClock());
    }

    RaftDriver(
            NodeId selfId,
            RaftCore core,
            StateMachine stateMachine,
            Map<NodeId, RaftPeerClient> peers,
            long tickIntervalNanos,
            Clock clock) {
        this.selfId = Objects.requireNonNull(selfId);
        this.core = Objects.requireNonNull(core);
        this.stateMachine = Objects.requireNonNull(stateMachine);
        // Concurrent so a broker join can add a peer while the pump thread
        // reads the map to dispatch AppendEntries (R4.3). Reads see a
        // consistent view per get; a newly-added peer is visible on the next
        // dispatch.
        this.peers = new java.util.concurrent.ConcurrentHashMap<>(peers);
        this.tickIntervalNanos = tickIntervalNanos;
        this.clock = clock;
    }

    public void start(int grpcPort) throws IOException {
        start(grpcPort, TlsConfig.DISABLED);
    }

    /**
     * TLS-aware start. When {@code tls.enabled()}, binds the Raft
     * gRPC server with an mTLS-demanding {@link io.grpc.netty.shaded.io.netty.handler.ssl.SslContext}.
     */
    public void start(int grpcPort, TlsConfig tls) throws IOException {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        var b = NettyServerBuilder.forPort(grpcPort).addService(new RaftGrpcService(this));
        try {
            var sslCtx = TlsContexts.serverContext(tls == null ? TlsConfig.DISABLED : tls);
            if (sslCtx != null) b.sslContext(sslCtx);
        } catch (javax.net.ssl.SSLException e) {
            throw new IOException("TLS server context build failed", e);
        }
        grpcServer = b.build().start();
        pumpThread = Thread.ofVirtual().name("raft-pump-" + selfId.value()).start(this::pumpLoop);
        tickerThread = Thread.ofVirtual().name("raft-ticker-" + selfId.value()).start(this::tickerLoop);
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        if (pumpThread != null) pumpThread.interrupt();
        if (tickerThread != null) tickerThread.interrupt();
        try {
            if (pumpThread != null) pumpThread.join(2_000);
            if (tickerThread != null) tickerThread.join(2_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (grpcServer != null) {
            grpcServer.shutdown();
            try {
                grpcServer.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        peers.values().forEach(RaftPeerClient::close);
    }

    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req) throws InterruptedException {
        var event = RaftMessageCodec.fromProto(req, clock.nanoTime());
        var future = new CompletableFuture<RaftEffect.SendAppendEntriesResp>();
        queue.put(new PendingEvent(event, future));
        try {
            // If the pump is backlogged past 3s, this RPC times out and the caller
            // sees UNAVAILABLE; the event is still processed by the pump and the
            // completion is silently dropped. The peer retries, so worst case is a
            // wasted heartbeat, not a correctness problem.
            return RaftMessageCodec.toProto(future.get(3, TimeUnit.SECONDS));
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public RequestVoteResponse handleRequestVote(RequestVoteRequest req) throws InterruptedException {
        if (req.getPreVote()) {
            var event = RaftMessageCodec.preVoteFromProto(req, clock.nanoTime());
            var future = new CompletableFuture<RaftEffect.SendPreVoteResp>();
            queue.put(new PendingEvent(event, future));
            try {
                return RaftMessageCodec.toProto(future.get(3, TimeUnit.SECONDS));
            } catch (ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
        var event = RaftMessageCodec.fromProto(req, clock.nanoTime());
        var future = new CompletableFuture<RaftEffect.SendVoteResp>();
        queue.put(new PendingEvent(event, future));
        try {
            // See handleAppendEntries: timeout here means the RPC caller sees
            // UNAVAILABLE but the vote is still processed locally. Peer retries.
            return RaftMessageCodec.toProto(future.get(3, TimeUnit.SECONDS));
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Forward-chain limit for rejected proposes. One hop reaches the
     * current leader in the steady state; the second absorbs a stale
     * leader hint during churn. Beyond that the payload is dropped and
     * the caller's retry cadence takes over.
     */
    private static final int MAX_PROPOSE_HOPS = 2;

    public void propose(byte[] payload) throws InterruptedException {
        queue.put(new PendingEvent(new RaftEvent.ClientPropose(payload), null, 0));
    }

    /**
     * Propose received over the wire from a peer that was not leader.
     * Carries the forward-chain depth so a stale-leader ping-pong is
     * bounded by {@link #MAX_PROPOSE_HOPS}.
     */
    public void proposeForwarded(byte[] payload, int hops) throws InterruptedException {
        queue.put(new PendingEvent(new RaftEvent.ClientPropose(payload), null, hops));
    }

    public void transferLeadership(NodeId target) throws InterruptedException {
        queue.put(new PendingEvent(new RaftEvent.TransferLeadership(target), null));
    }

    /**
     * Propose a single-server membership change (Raft §4.2). Fire-and-forget:
     * the core rejects it if self is not leader or another change is still in
     * flight, so the caller (the controller's broker-join orchestration)
     * re-derives and retries. Takes effect on append, commits on quorum.
     */
    public void proposeMembership(jbroker.raft.core.Membership membership) throws InterruptedException {
        queue.put(new PendingEvent(new RaftEvent.ProposeConfigChange(membership), null));
    }

    /**
     * Chaos force-election — queues a self-addressed {@code TimeoutNow}
     * so the core short-circuits the pre-vote + election-timeout wait and
     * jumps straight to {@code startElection} at {@code currentTerm + 1}.
     *
     * <p>No-op when self is already LEADER — a leader force-electing itself
     * would gratuitously bump the term and disrupt the cluster.
     *
     * <p>The election may still fail if a healthy leader holds more recent
     * log entries or if peers reject the vote (stale log). That's the
     * intended semantics: "force an election attempt" — not "force a win".
     */
    public void forceElection() throws InterruptedException {
        if (core.role() == jbroker.raft.core.Role.LEADER) return;
        var term = core.currentTerm();
        queue.put(new PendingEvent(new RaftEvent.TimeoutNow(selfId, term, clock.nanoTime()), null));
    }

    public jbroker.proto.raft.TimeoutNowResponse handleTimeoutNow(jbroker.proto.raft.TimeoutNowRequest req)
            throws InterruptedException {
        queue.put(new PendingEvent(
                new RaftEvent.TimeoutNow(new NodeId(req.getLeaderId()), new Term(req.getTerm()), clock.nanoTime()),
                null));
        return jbroker.proto.raft.TimeoutNowResponse.newBuilder()
                .setTerm(core.currentTerm().value())
                .build();
    }

    public jbroker.proto.raft.InstallSnapshotResponse handleInstallSnapshot(
            jbroker.proto.raft.InstallSnapshotRequest req) throws InterruptedException {
        var event = new RaftEvent.InstallSnapshotReq(
                new Term(req.getTerm()),
                new NodeId(req.getLeaderId()),
                req.getLastIncludedIndex(),
                new Term(req.getLastIncludedTerm()),
                req.getData().toByteArray(),
                clock.nanoTime());
        var future = new CompletableFuture<RaftEffect.SendInstallSnapshotResp>();
        queue.put(new PendingEvent(event, future));
        try {
            var resp = future.get(30, TimeUnit.SECONDS);
            return jbroker.proto.raft.InstallSnapshotResponse.newBuilder()
                    .setTerm(resp.term().value())
                    .build();
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Trigger a snapshot: captures the state-machine bytes, hands them to
     * the core, and compacts the log up to the current commitIndex. The core
     * handles this on its pump thread so we don't race with step() calls.
     */
    public void snapshotNow() throws InterruptedException, IOException {
        var baos = new java.io.ByteArrayOutputStream();
        stateMachine.snapshot(baos);
        byte[] bytes = baos.toByteArray();
        queue.put(new PendingEvent(new RaftEvent.TakeSnapshot(bytes), null));
    }

    public Term currentTerm() {
        return core.currentTerm();
    }

    public Role role() {
        return core.role();
    }

    /** See {@link RaftCore#currentLeader()}. Exposed for admin observability. */
    public java.util.Optional<NodeId> currentLeader() {
        return core.currentLeader();
    }

    /** See {@link RaftCore#observability()}. Exposed for admin {@code /raft}. */
    public jbroker.raft.core.RaftCore.Observability observability() {
        return core.observability();
    }

    /** The current voting members (for membership observability + broker-join orchestration). */
    public List<NodeId> activeVoters() {
        return core.activeVoters();
    }

    /** The current non-voting learners (empty when none). */
    public List<NodeId> activeLearners() {
        return core.activeLearners();
    }

    /** Leader-side replication progress; drives catch-up detection during a broker join. */
    public jbroker.raft.core.RaftCore.ReplicationProgress replicationProgress() {
        return core.replicationProgress();
    }

    public java.util.Collection<RaftPeerClient> peers() {
        return peers.values();
    }

    /**
     * Register a dial channel to a broker joining the cluster (R4.3). Existing
     * members call this before the controller proposes the add-learner change,
     * so their pump threads can send it AppendEntries the moment it is a
     * replication target. Idempotent; replaces any prior client for the id
     * (closing the old one).
     */
    public void addPeer(NodeId id, RaftPeerClient client) {
        var prior = peers.put(id, client);
        if (prior != null && prior != client) {
            prior.close();
        }
    }

    /**
     * Whether a dial channel to {@code id} already exists. Lets callers driven
     * by a re-firing registration listener skip rebuilding a channel they
     * already hold, rather than churning it through {@link #addPeer}.
     */
    public boolean hasPeer(NodeId id) {
        return peers.containsKey(id);
    }

    private void pumpLoop() {
        while (running.get()) {
            try {
                var pending = queue.take();
                var effects = core.step(pending.event);
                dispatch(effects, pending);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                LOG.warn("pump loop error", e);
            }
        }
    }

    private void tickerLoop() {
        while (running.get()) {
            try {
                TimeUnit.NANOSECONDS.sleep(tickIntervalNanos);
                queue.put(new PendingEvent(new RaftEvent.Tick(clock.nanoTime()), null));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void dispatch(List<RaftEffect> effects, PendingEvent pending) {
        for (var effect : effects) {
            switch (effect) {
                case RaftEffect.SendAppendEntriesResp r -> {
                    if (pending.future != null) {
                        ((CompletableFuture<RaftEffect.SendAppendEntriesResp>) pending.future).complete(r);
                    }
                }
                case RaftEffect.SendVoteResp r -> {
                    if (pending.future != null) {
                        ((CompletableFuture<RaftEffect.SendVoteResp>) pending.future).complete(r);
                    }
                }
                case RaftEffect.SendPreVoteResp r -> {
                    if (pending.future != null) {
                        ((CompletableFuture<RaftEffect.SendPreVoteResp>) pending.future).complete(r);
                    }
                }
                case RaftEffect.SendAppendEntries s -> dispatchAppend(s);
                case RaftEffect.SendVoteReq v -> dispatchVote(v);
                case RaftEffect.SendPreVoteReq v -> dispatchPreVote(v);
                case RaftEffect.ApplyCommitted a -> stateMachine.apply(a.entry());
                case RaftEffect.PersistLog ignored -> {
                    /* FileRaftLog already fsynced */
                }
                case RaftEffect.PersistState ignored -> {
                    /* FilePersistentState already fsynced */
                }
                case RaftEffect.TruncateLog ignored -> {
                    /* handled inline by log */
                }
                case RaftEffect.RejectClientPropose r -> {
                    // Not leader: forward the payload to the known leader
                    // instead of dropping it. Background proposers (ISR
                    // flips, leadership drains) live on whatever broker
                    // owns the partition — routinely a Raft follower — and
                    // dropping froze their metadata until leadership
                    // happened to coincide again (#124). Fire-and-forget:
                    // callers own retries, and duplicate commits are safe
                    // (partition changes are CAS-guarded).
                    if (pending.event instanceof RaftEvent.ClientPropose cp
                            && r.knownLeader().isPresent()
                            && !r.knownLeader().get().equals(selfId)
                            && pending.hops() < MAX_PROPOSE_HOPS) {
                        var leader = r.knownLeader().get();
                        var peer = peers.get(leader);
                        if (peer != null) {
                            int nextHops = pending.hops() + 1;
                            Thread.ofVirtual()
                                    .name("raft-propose-fwd-" + selfId.value() + "->" + leader.value())
                                    .start(() -> {
                                        try {
                                            peer.propose(cp.payload(), nextHops);
                                        } catch (Exception e) {
                                            LOG.debug(
                                                    "propose forward to {} failed: {}", leader.value(), e.getMessage());
                                        }
                                    });
                        }
                    } else {
                        LOG.debug(
                                "propose dropped: not leader (known leader {}, hops {})",
                                r.knownLeader().map(NodeId::value).orElse(-1),
                                pending.hops());
                    }
                }
                case RaftEffect.DuplicateClientPropose ignored -> {
                    /* cached-response path to the client not yet implemented */
                }
                case RaftEffect.SendTimeoutNow t -> dispatchTimeoutNow(t);
                case RaftEffect.ServeClientRead s -> LOG.warn(
                        "ServeClientRead dropped — client RPC not wired (clientId={}, requestId={}, readIndex={})",
                        s.clientId(),
                        s.requestId(),
                        s.readIndex());
                case RaftEffect.RejectClientRead r -> LOG.warn(
                        "RejectClientRead dropped — client RPC not wired (clientId={}, requestId={})",
                        r.clientId(),
                        r.requestId());
                case RaftEffect.RejectConfigChange r -> LOG.warn(
                        "RejectConfigChange dropped — admin RPC not wired (reason={})", r.reason());
                case RaftEffect.SendInstallSnapshot s -> dispatchInstallSnapshot(s);
                case RaftEffect.SendInstallSnapshotResp r -> {
                    if (pending.future != null) {
                        ((CompletableFuture<RaftEffect.SendInstallSnapshotResp>) pending.future).complete(r);
                    }
                }
                case RaftEffect.ApplySnapshot a -> {
                    try {
                        stateMachine.restore(new java.io.ByteArrayInputStream(a.snapshot()));
                    } catch (java.io.IOException e) {
                        LOG.warn("state-machine restore failed", e);
                    }
                }
            }
        }
    }

    private void dispatchInstallSnapshot(RaftEffect.SendInstallSnapshot eff) {
        var peer = peers.get(eff.to());
        if (peer == null) return;
        Thread.ofVirtual()
                .name("raft-installsnapshot-" + selfId.value() + "->" + eff.to().value())
                .start(() -> {
                    try {
                        var proto = jbroker.proto.raft.InstallSnapshotRequest.newBuilder()
                                .setTerm(eff.term().value())
                                .setLeaderId(eff.leaderId().value())
                                .setLastIncludedIndex(eff.lastIncludedIndex())
                                .setLastIncludedTerm(eff.lastIncludedTerm().value())
                                .setOffset(0)
                                .setData(ByteString.copyFrom(eff.snapshot()))
                                .setDone(true)
                                .build();
                        var resp = peer.installSnapshot(proto);
                        queue.put(new PendingEvent(
                                new RaftEvent.InstallSnapshotResp(eff.to(), new Term(resp.getTerm())), null));
                    } catch (Exception e) {
                        LOG.debug("installSnapshot to {} failed: {}", eff.to(), e.getMessage());
                    }
                });
    }

    private void dispatchTimeoutNow(RaftEffect.SendTimeoutNow eff) {
        var peer = peers.get(eff.to());
        if (peer == null) return;
        Thread.ofVirtual()
                .name("raft-timeoutnow-" + selfId.value() + "->" + eff.to().value())
                .start(() -> {
                    try {
                        var proto = jbroker.proto.raft.TimeoutNowRequest.newBuilder()
                                .setTerm(eff.term().value())
                                .setLeaderId(selfId.value())
                                .build();
                        peer.timeoutNow(proto);
                    } catch (Exception e) {
                        LOG.debug("timeoutNow to {} failed: {}", eff.to(), e.getMessage());
                    }
                });
    }

    private void dispatchAppend(RaftEffect.SendAppendEntries eff) {
        var peer = peers.get(eff.to());
        if (peer == null) return;
        // Run the blocking RPC on a virtual thread so the pump remains free to
        // process incoming events (e.g. vote requests) concurrently.
        Thread.ofVirtual()
                .name("raft-append-" + selfId.value() + "->" + eff.to().value())
                .start(() -> {
                    try {
                        var proto = AppendEntriesRequest.newBuilder()
                                .setTerm(eff.term().value())
                                .setLeaderId(eff.leaderId().value())
                                .setPrevLogIndex(eff.prevLogIndex())
                                .setPrevLogTerm(eff.prevLogTerm().value())
                                .setLeaderCommit(eff.leaderCommit())
                                .setHeartbeatSeq(eff.heartbeatSeq());
                        for (var e : eff.entries()) {
                            proto.addEntries(jbroker.proto.raft.LogEntry.newBuilder()
                                    .setIndex(e.index())
                                    .setTerm(e.term().value())
                                    .setType(EntryType.forNumber(e.type().ordinal()))
                                    .setPayload(ByteString.copyFrom(e.payload()))
                                    .build());
                        }
                        var resp = peer.appendEntries(proto.build());
                        long matchHint = eff.prevLogIndex() + eff.entries().size();
                        queue.put(new PendingEvent(RaftMessageCodec.fromProto(resp, eff.to(), matchHint), null));
                    } catch (Exception e) {
                        LOG.debug("append to {} failed: {}", eff.to(), e.getMessage());
                    }
                });
    }

    private void dispatchVote(RaftEffect.SendVoteReq eff) {
        var peer = peers.get(eff.to());
        if (peer == null) return;
        // Run the blocking RPC on a virtual thread so the pump remains free to
        // process incoming events (e.g. vote requests from other candidates).
        Thread.ofVirtual()
                .name("raft-vote-" + selfId.value() + "->" + eff.to().value())
                .start(() -> {
                    try {
                        var proto = RequestVoteRequest.newBuilder()
                                .setTerm(eff.term().value())
                                .setCandidateId(eff.candidateId().value())
                                .setLastLogIndex(eff.lastLogIndex())
                                .setLastLogTerm(eff.lastLogTerm().value())
                                .setPreVote(false)
                                .build();
                        var resp = peer.requestVote(proto);
                        queue.put(new PendingEvent(RaftMessageCodec.fromProto(resp, eff.to()), null));
                    } catch (Exception e) {
                        LOG.debug("vote to {} failed: {}", eff.to(), e.getMessage());
                    }
                });
    }

    private void dispatchPreVote(RaftEffect.SendPreVoteReq eff) {
        var peer = peers.get(eff.to());
        if (peer == null) return;
        Thread.ofVirtual()
                .name("raft-prevote-" + selfId.value() + "->" + eff.to().value())
                .start(() -> {
                    try {
                        var proto = RequestVoteRequest.newBuilder()
                                .setTerm(eff.hypotheticalTerm().value())
                                .setCandidateId(eff.candidateId().value())
                                .setLastLogIndex(eff.lastLogIndex())
                                .setLastLogTerm(eff.lastLogTerm().value())
                                .setPreVote(true)
                                .build();
                        var resp = peer.requestVote(proto);
                        queue.put(new PendingEvent(RaftMessageCodec.preVoteRespFromProto(resp, eff.to()), null));
                    } catch (Exception e) {
                        LOG.debug("prevote to {} failed: {}", eff.to(), e.getMessage());
                    }
                });
    }

    private record PendingEvent(RaftEvent event, CompletableFuture<?> future, int hops) {
        PendingEvent(RaftEvent event, CompletableFuture<?> future) {
            this(event, future, 0);
        }
    }
}
