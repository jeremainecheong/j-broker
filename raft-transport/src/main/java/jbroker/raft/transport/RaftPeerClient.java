package jbroker.raft.transport;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import jbroker.proto.raft.AppendEntriesRequest;
import jbroker.proto.raft.AppendEntriesResponse;
import jbroker.proto.raft.RaftGrpc;
import jbroker.proto.raft.RequestVoteRequest;
import jbroker.proto.raft.RequestVoteResponse;
import jbroker.raft.core.NodeId;

public final class RaftPeerClient implements AutoCloseable {

    private final NodeId peerId;
    private final ManagedChannel channel;
    private final RaftGrpc.RaftBlockingStub stub;

    public RaftPeerClient(NodeId peerId, String host, int port) {
        this.peerId = peerId;
        this.channel = NettyChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.stub = RaftGrpc.newBlockingStub(channel);
    }

    public NodeId peerId() {
        return peerId;
    }

    /**
     * Requests a connection to the peer eagerly so the first real RPC does not
     * pay the connection-establishment latency.  Safe to call before the peer's
     * server is up; the channel will retry in the background.
     */
    public void warmUp() {
        channel.getState(true); // requestConnection=true triggers IDLE→CONNECTING
    }

    /**
     * Blocks until the channel is in the READY state or the timeout elapses.
     * Returns {@code true} if READY was reached, {@code false} on timeout.
     */
    public boolean waitForReady(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            ConnectivityState state = channel.getState(true);
            if (state == ConnectivityState.READY) {
                return true;
            }
            // Register a one-shot callback that fires when the state changes,
            // then wait at most 50 ms before re-checking.
            var latch = new CountDownLatch(1);
            channel.notifyWhenStateChanged(state, latch::countDown);
            latch.await(50, TimeUnit.MILLISECONDS);
        }
        return channel.getState(false) == ConnectivityState.READY;
    }

    public AppendEntriesResponse appendEntries(AppendEntriesRequest req) {
        return stub.withDeadlineAfter(2, TimeUnit.SECONDS).appendEntries(req);
    }

    public RequestVoteResponse requestVote(RequestVoteRequest req) {
        return stub.withDeadlineAfter(2, TimeUnit.SECONDS).requestVote(req);
    }

    public jbroker.proto.raft.TimeoutNowResponse timeoutNow(jbroker.proto.raft.TimeoutNowRequest req) {
        return stub.withDeadlineAfter(2, TimeUnit.SECONDS).timeoutNow(req);
    }

    public jbroker.proto.raft.InstallSnapshotResponse installSnapshot(jbroker.proto.raft.InstallSnapshotRequest req) {
        // Snapshots can be large; allow 30s for the single-shot transfer.
        return stub.withDeadlineAfter(30, TimeUnit.SECONDS).installSnapshot(req);
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
