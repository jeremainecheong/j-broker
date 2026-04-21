package jbroker.raft.transport;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
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

    public AppendEntriesResponse appendEntries(AppendEntriesRequest req) {
        return stub.withDeadlineAfter(2, TimeUnit.SECONDS).appendEntries(req);
    }

    public RequestVoteResponse requestVote(RequestVoteRequest req) {
        return stub.withDeadlineAfter(2, TimeUnit.SECONDS).requestVote(req);
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
