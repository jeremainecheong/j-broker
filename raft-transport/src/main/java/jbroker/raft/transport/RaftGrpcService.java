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
        try {
            var resp = driver.handleInstallSnapshot(req);
            out.onNext(resp);
            out.onCompleted();
        } catch (Exception e) {
            out.onError(e);
        }
    }

    @Override
    public void timeoutNow(TimeoutNowRequest req, StreamObserver<TimeoutNowResponse> out) {
        try {
            var resp = driver.handleTimeoutNow(req);
            out.onNext(resp);
            out.onCompleted();
        } catch (Exception e) {
            out.onError(e);
        }
    }

    @Override
    public void propose(jbroker.proto.raft.ProposeRequest req, StreamObserver<jbroker.proto.raft.ProposeResponse> out) {
        try {
            // Enqueue on this node. If it turns out not to be leader either
            // (leadership moved since the sender's view), the reject path
            // re-forwards, bounded by the request's hop count.
            driver.proposeForwarded(req.getPayload().toByteArray(), req.getHops());
            out.onNext(jbroker.proto.raft.ProposeResponse.newBuilder()
                    .setAccepted(true)
                    .build());
            out.onCompleted();
        } catch (Exception e) {
            out.onError(e);
        }
    }
}
