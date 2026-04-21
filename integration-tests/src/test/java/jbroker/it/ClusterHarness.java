package jbroker.it;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import jbroker.raft.core.DefaultRaftCore;
import jbroker.raft.core.FilePersistentState;
import jbroker.raft.core.FileRaftLog;
import jbroker.raft.core.LogEntry;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.RaftConfig;
import jbroker.raft.core.RaftCore;
import jbroker.raft.core.Role;
import jbroker.raft.core.StateMachine;
import jbroker.raft.transport.RaftDriver;
import jbroker.raft.transport.RaftPeerClient;

public final class ClusterHarness implements AutoCloseable {

    public record Node(NodeId id, int port, RaftDriver driver, RecordingStateMachine sm) {}

    private final List<Node> nodes;

    private ClusterHarness(List<Node> nodes) {
        this.nodes = nodes;
    }

    public static ClusterHarness start(Path tempDir, int size) throws IOException {
        var ports = IntStream.range(0, size).map(i -> freePort()).toArray();
        var ids = IntStream.range(1, size + 1).mapToObj(NodeId::new).toList();

        var nodes = new ArrayList<Node>();
        for (int i = 0; i < size; i++) {
            var self = ids.get(i);
            var port = ports[i];
            var dataDir = tempDir.resolve("node-" + self.value());
            Files.createDirectories(dataDir);

            var log = FileRaftLog.open(dataDir.resolve("log.bin"));
            var state = FilePersistentState.open(dataDir.resolve("state.bin"));

            var config = new RaftConfig(
                    self,
                    ids,
                    TimeUnit.MILLISECONDS.toNanos(150),
                    TimeUnit.MILLISECONDS.toNanos(75),
                    TimeUnit.MILLISECONDS.toNanos(50),
                    100);

            // Pass Long.MAX_VALUE so the election deadline is deferred: the first
            // real Tick resets it to (real_now + randomised_timeout).  This means
            // elections cannot fire before every gRPC server is bound and accepting
            // connections, avoiding split-vote storms during startup.
            RaftCore core = new DefaultRaftCore(config, log, state, Long.MAX_VALUE);
            var sm = new RecordingStateMachine();

            Map<NodeId, RaftPeerClient> peers = new HashMap<>();
            for (int j = 0; j < size; j++) {
                if (j == i) continue;
                peers.put(ids.get(j), new RaftPeerClient(ids.get(j), "127.0.0.1", ports[j]));
            }

            var driver = new RaftDriver(self, core, sm, peers, TimeUnit.MILLISECONDS.toNanos(20));
            driver.start(port);
            // Warm up outbound gRPC connections so the first vote RPC does not
            // pay full connection-establishment latency.
            peers.values().forEach(RaftPeerClient::warmUp);
            nodes.add(new Node(self, port, driver, sm));
        }
        return new ClusterHarness(nodes);
    }

    public List<Node> nodes() {
        return nodes;
    }

    public Node waitForLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (var n : nodes) {
                if (n.driver().role() == Role.LEADER) return n;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("no leader elected within " + timeoutMs + "ms");
    }

    public void killNode(NodeId id) {
        for (var n : nodes) {
            if (n.id().equals(id)) {
                n.driver().close();
                return;
            }
        }
    }

    @Override
    public void close() {
        for (var n : nodes) {
            n.driver().close();
        }
    }

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static final class RecordingStateMachine implements StateMachine {
        public final List<byte[]> applied = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void apply(LogEntry entry) {
            applied.add(entry.payload());
        }
    }
}
