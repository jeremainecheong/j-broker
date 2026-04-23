package jbroker.admin.api;

import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import jbroker.admin.client.BrokerAdminClient;
import jbroker.admin.client.BrokerAdminClientPool;
import jbroker.admin.dto.RaftNodeState;
import jbroker.admin.dto.RestError;
import jbroker.proto.broker.DescribeRaftResponse;
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
        // describeRaft is a blocking gRPC per broker. Fan-out via
        // a virtual-thread-per-task executor turns N * RTT into max(RTT)
        // on the UI's Raft page. Try-with-resources on the executor
        // waits for every fork before returning (structured-concurrency
        // semantics; StructuredTaskScope remains preview in JDK 21).
        var out = new ArrayList<RaftNodeState>();
        var clients = pool.clients();
        var tasks = new ArrayList<Callable<RaftFanOutResult>>(clients.size());
        for (BrokerAdminClient c : clients) {
            tasks.add(() -> {
                try {
                    return new RaftFanOutResult(c.address(), c.describeRaft(), null);
                } catch (StatusRuntimeException e) {
                    return new RaftFanOutResult(c.address(), null, e);
                }
            });
        }
        try (var scope = Executors.newVirtualThreadPerTaskExecutor()) {
            for (var f : scope.invokeAll(tasks)) {
                try {
                    var res = f.get();
                    if (res.error != null) {
                        log.debug("broker {} unreachable for describeRaft: {}", res.address, res.error.getStatus());
                        out.add(RaftNodeState.unreachable(res.address));
                    } else if (res.response.getError() == ErrorCode.OK) {
                        var r = res.response;
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
                } catch (ExecutionException ee) {
                    log.debug("describeRaft subtask failed", ee.getCause());
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        return out;
    }

    private record RaftFanOutResult(String address, DescribeRaftResponse response, StatusRuntimeException error) {}

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
