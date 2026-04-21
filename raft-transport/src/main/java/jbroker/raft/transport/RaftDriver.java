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
        this.peers = Map.copyOf(peers);
        this.tickIntervalNanos = tickIntervalNanos;
        this.clock = clock;
    }

    public void start(int grpcPort) throws IOException {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        grpcServer = NettyServerBuilder.forPort(grpcPort)
                .addService(new RaftGrpcService(this))
                .build()
                .start();
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
        var event = RaftMessageCodec.fromProto(req);
        var future = new CompletableFuture<RaftEffect.SendAppendEntriesResp>();
        queue.put(new PendingEvent(event, future));
        try {
            return RaftMessageCodec.toProto(future.get(3, TimeUnit.SECONDS));
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public RequestVoteResponse handleRequestVote(RequestVoteRequest req) throws InterruptedException {
        var event = RaftMessageCodec.fromProto(req);
        var future = new CompletableFuture<RaftEffect.SendVoteResp>();
        queue.put(new PendingEvent(event, future));
        try {
            return RaftMessageCodec.toProto(future.get(3, TimeUnit.SECONDS));
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public void propose(byte[] payload) throws InterruptedException {
        queue.put(new PendingEvent(new RaftEvent.ClientPropose(payload), null));
    }

    public Term currentTerm() {
        return core.currentTerm();
    }

    public Role role() {
        return core.role();
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
                case RaftEffect.SendAppendEntries s -> dispatchAppend(s);
                case RaftEffect.SendVoteReq v -> dispatchVote(v);
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
                case RaftEffect.RejectClientPropose ignored -> {
                    /* Phase 5 wires this to client */
                }
            }
        }
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
                                .setLeaderCommit(eff.leaderCommit());
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
                                .build();
                        var resp = peer.requestVote(proto);
                        queue.put(new PendingEvent(RaftMessageCodec.fromProto(resp, eff.to()), null));
                    } catch (Exception e) {
                        LOG.debug("vote to {} failed: {}", eff.to(), e.getMessage());
                    }
                });
    }

    private record PendingEvent(RaftEvent event, CompletableFuture<?> future) {}
}
