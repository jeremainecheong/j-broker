package jbroker.admin.api;

import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import jbroker.admin.client.BrokerAdminClientPool;
import jbroker.admin.dto.RaftNodeState;
import jbroker.admin.dto.RestError;
import jbroker.proto.common.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * {@code /api/v1/raft} — per-broker Raft state for the UI's Raft page.
 * {@code DescribeRaft} returns the self-view only, so this controller fans
 * out to every broker in parallel and returns the merged array.
 */
@RestController
@RequestMapping("/api/v1/raft")
public class RaftController {

    private static final Logger log = LoggerFactory.getLogger(RaftController.class);

    private final BrokerAdminClientPool pool;

    public RaftController(BrokerAdminClientPool pool) {
        this.pool = pool;
    }

    @GetMapping
    public java.util.List<RaftNodeState> raftClusterState() {
        var out = new ArrayList<RaftNodeState>();
        for (var c : pool.clients()) {
            try {
                var r = c.describeRaft();
                if (r.getError() == ErrorCode.OK) {
                    out.add(RaftNodeState.ok(
                            r.getNodeId(),
                            r.getRole(),
                            r.getCurrentTerm(),
                            r.getCommitIndex(),
                            r.getLastApplied(),
                            r.getVotedFor(),
                            r.getLastLogIndex(),
                            r.getLastLogTerm()));
                }
            } catch (StatusRuntimeException e) {
                log.debug("broker {} unreachable for describeRaft: {}", c.address(), e.getStatus());
                out.add(RaftNodeState.unreachable(c.address()));
            }
        }
        return out;
    }

    @GetMapping("/nodes/{id}")
    public ResponseEntity<?> raftNode(@PathVariable("id") int id) {
        for (var c : pool.clients()) {
            try {
                var r = c.describeRaft();
                if (r.getError() == ErrorCode.OK && r.getNodeId() == id) {
                    return ResponseEntity.ok(RaftNodeState.ok(
                            r.getNodeId(),
                            r.getRole(),
                            r.getCurrentTerm(),
                            r.getCommitIndex(),
                            r.getLastApplied(),
                            r.getVotedFor(),
                            r.getLastLogIndex(),
                            r.getLastLogTerm()));
                }
            } catch (StatusRuntimeException e) {
                log.debug("broker {} unreachable for describeRaft: {}", c.address(), e.getStatus());
            }
        }
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(RestError.of("UNKNOWN_NODE", "no broker with id " + id));
    }
}
