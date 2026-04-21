package jbroker.raft.transport;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import jbroker.proto.raft.AppendEntriesRequest;
import jbroker.raft.core.LogEntry;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.RaftEvent;
import jbroker.raft.core.Term;
import org.junit.jupiter.api.Test;

class RaftMessageCodecTest {

    @Test
    void roundtripsAppendEntries() {
        var entry = new LogEntry(1, new Term(1), LogEntry.Type.NORMAL, new byte[] {9, 9});
        var req = new RaftEvent.AppendEntriesReq(new Term(2), new NodeId(3), 0L, Term.ZERO, List.of(entry), 0L);
        AppendEntriesRequest proto = RaftMessageCodec.toProto(req);
        RaftEvent.AppendEntriesReq roundtrip = RaftMessageCodec.fromProto(proto);
        assertThat(roundtrip).isEqualTo(req);
    }
}
