package jbroker.raft.transport;

import io.grpc.stub.StreamObserver;
import jbroker.proto.raft.AppendEntriesRequest;
import jbroker.proto.raft.AppendEntriesResponse;
import jbroker.proto.raft.InstallSnapshotRequest;
import jbroker.proto.raft.InstallSnapshotResponse;
import jbroker.proto.raft.RaftGrpc;
import jbroker.proto.raft.RequestVoteRequest;
import jbroker.proto.raft.RequestVoteResponse;
import jbroker.proto.raft.TimeoutNowRequest;
import jbroker.proto.raft.TimeoutNowResponse;

public final class RaftGrpcService extends RaftGrpc.RaftImplBase {

    private final RaftDriver driver;

    public RaftGrpcService(RaftDriver driver) {
        this.driver = driver;
    }

    @Override
    public void appendEntries(AppendEntriesRequest req, StreamObserver<AppendEntriesResponse> out) {
        try {
            var resp = driver.handleAppendEntries(req);
            out.onNext(resp);
            out.onCompleted();
        } catch (Exception e) {
            out.onError(e);
        }
    }

    @Override
    public void requestVote(RequestVoteRequest req, StreamObserver<RequestVoteResponse> out) {
        try {
            var resp = driver.handleRequestVote(req);
            out.onNext(resp);
            out.onCompleted();
        } catch (Exception e) {
            out.onError(e);
        }
    }

    @Override
    public void installSnapshot(InstallSnapshotRequest req, StreamObserver<InstallSnapshotResponse> out) {
        // Milestone 2 feature; stub returns default response for now.
        out.onNext(InstallSnapshotResponse.newBuilder().build());
        out.onCompleted();
    }

    @Override
    public void timeoutNow(TimeoutNowRequest req, StreamObserver<TimeoutNowResponse> out) {
        // Milestone 2 feature; stub.
        out.onNext(TimeoutNowResponse.newBuilder().build());
        out.onCompleted();
    }
}
