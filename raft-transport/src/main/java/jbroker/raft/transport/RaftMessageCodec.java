package jbroker.raft.transport;

import com.google.protobuf.ByteString;
import jbroker.proto.raft.AppendEntriesRequest;
import jbroker.proto.raft.AppendEntriesResponse;
import jbroker.proto.raft.EntryType;
import jbroker.proto.raft.RequestVoteRequest;
import jbroker.proto.raft.RequestVoteResponse;
import jbroker.raft.core.LogEntry;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.RaftEffect;
import jbroker.raft.core.RaftEvent;
import jbroker.raft.core.Term;

public final class RaftMessageCodec {

    private RaftMessageCodec() {}

    public static AppendEntriesRequest toProto(RaftEvent.AppendEntriesReq req) {
        var b = AppendEntriesRequest.newBuilder()
                .setTerm(req.term().value())
                .setLeaderId(req.leaderId().value())
                .setPrevLogIndex(req.prevLogIndex())
                .setPrevLogTerm(req.prevLogTerm().value())
                .setLeaderCommit(req.leaderCommit());
        for (var e : req.entries()) {
            b.addEntries(toProto(e));
        }
        return b.build();
    }

    public static RaftEvent.AppendEntriesReq fromProto(AppendEntriesRequest p, long nowNanos) {
        return new RaftEvent.AppendEntriesReq(
                new Term(p.getTerm()),
                new NodeId(p.getLeaderId()),
                p.getPrevLogIndex(),
                new Term(p.getPrevLogTerm()),
                p.getEntriesList().stream().map(RaftMessageCodec::fromProto).toList(),
                p.getLeaderCommit(),
                nowNanos);
    }

    public static AppendEntriesResponse toProto(RaftEffect.SendAppendEntriesResp eff) {
        return AppendEntriesResponse.newBuilder()
                .setTerm(eff.term().value())
                .setSuccess(eff.success())
                .setConflictIndex(eff.conflictIndex())
                .setConflictTerm(eff.conflictTerm().value())
                .build();
    }

    public static RaftEvent.AppendEntriesResp fromProto(AppendEntriesResponse p, NodeId from, long matchIndexHint) {
        return new RaftEvent.AppendEntriesResp(
                from,
                new Term(p.getTerm()),
                p.getSuccess(),
                p.getConflictIndex(),
                new Term(p.getConflictTerm()),
                matchIndexHint);
    }

    public static RequestVoteRequest toProto(RaftEffect.SendVoteReq eff) {
        return RequestVoteRequest.newBuilder()
                .setTerm(eff.term().value())
                .setCandidateId(eff.candidateId().value())
                .setLastLogIndex(eff.lastLogIndex())
                .setLastLogTerm(eff.lastLogTerm().value())
                .setPreVote(false)
                .build();
    }

    public static RequestVoteRequest toProto(RaftEffect.SendPreVoteReq eff) {
        return RequestVoteRequest.newBuilder()
                .setTerm(eff.hypotheticalTerm().value())
                .setCandidateId(eff.candidateId().value())
                .setLastLogIndex(eff.lastLogIndex())
                .setLastLogTerm(eff.lastLogTerm().value())
                .setPreVote(true)
                .build();
    }

    public static RaftEvent.VoteReq fromProto(RequestVoteRequest p, long nowNanos) {
        return new RaftEvent.VoteReq(
                new Term(p.getTerm()),
                new NodeId(p.getCandidateId()),
                p.getLastLogIndex(),
                new Term(p.getLastLogTerm()),
                nowNanos);
    }

    public static RaftEvent.PreVoteReq preVoteFromProto(RequestVoteRequest p, long nowNanos) {
        return new RaftEvent.PreVoteReq(
                new Term(p.getTerm()),
                new NodeId(p.getCandidateId()),
                p.getLastLogIndex(),
                new Term(p.getLastLogTerm()),
                nowNanos);
    }

    public static RequestVoteResponse toProto(RaftEffect.SendVoteResp eff) {
        return RequestVoteResponse.newBuilder()
                .setTerm(eff.term().value())
                .setVoteGranted(eff.granted())
                .build();
    }

    public static RequestVoteResponse toProto(RaftEffect.SendPreVoteResp eff) {
        return RequestVoteResponse.newBuilder()
                .setTerm(eff.term().value())
                .setVoteGranted(eff.granted())
                .build();
    }

    public static RaftEvent.VoteResp fromProto(RequestVoteResponse p, NodeId from) {
        return new RaftEvent.VoteResp(from, new Term(p.getTerm()), p.getVoteGranted());
    }

    public static RaftEvent.PreVoteResp preVoteRespFromProto(RequestVoteResponse p, NodeId from) {
        return new RaftEvent.PreVoteResp(from, new Term(p.getTerm()), p.getVoteGranted());
    }

    private static jbroker.proto.raft.LogEntry toProto(LogEntry e) {
        return jbroker.proto.raft.LogEntry.newBuilder()
                .setIndex(e.index())
                .setTerm(e.term().value())
                .setType(EntryType.forNumber(e.type().ordinal()))
                .setPayload(ByteString.copyFrom(e.payload()))
                .build();
    }

    private static LogEntry fromProto(jbroker.proto.raft.LogEntry p) {
        return new LogEntry(
                p.getIndex(),
                new Term(p.getTerm()),
                LogEntry.Type.values()[p.getType().getNumber()],
                p.getPayload().toByteArray());
    }
}
