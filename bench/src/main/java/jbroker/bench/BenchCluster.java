package jbroker.bench;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import jbroker.app.Broker;
import jbroker.app.VoterAddress;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;

/**
 * Shared helpers for the in-process multi-broker bench
 * variants. Picking free ports, spinning up an N-broker cluster on a
 * temp data dir, and waiting for a single Raft leader + full broker
 * registry — all three bench modes (acks=all, replication, compaction)
 * need the exact same bring-up.
 */
final class BenchCluster implements AutoCloseable {

    final int[] brokerPorts;
    final List<Broker> brokers;
    final List<Path> tempDirs;
    final int leaderPort;

    private BenchCluster(int[] brokerPorts, List<Broker> brokers, List<Path> tempDirs, int leaderPort) {
        this.brokerPorts = brokerPorts;
        this.brokers = brokers;
        this.tempDirs = tempDirs;
        this.leaderPort = leaderPort;
    }

    static BenchCluster start(int n) throws Exception {
        int[] raftPorts = new int[n];
        int[] brokerPorts = new int[n];
        for (int i = 0; i < n; i++) {
            raftPorts[i] = freePort();
            brokerPorts[i] = freePort();
        }
        var voters = new ArrayList<VoterAddress>();
        for (int i = 0; i < n; i++) {
            voters.add(new VoterAddress(new NodeId(i + 1), "127.0.0.1", raftPorts[i], brokerPorts[i]));
        }
        var dirs = new ArrayList<Path>();
        var brokers = new ArrayList<Broker>();
        for (int i = 0; i < n; i++) {
            Path d = Files.createTempDirectory("jbroker-bench-");
            dirs.add(d);
            brokers.add(Broker.start(new Broker.Config(new NodeId(i + 1), d, raftPorts[i], brokerPorts[i], voters)));
        }
        waitForReady(brokers);
        int leaderPort = -1;
        for (int i = 0; i < n; i++) {
            if (brokers.get(i).role() == Role.LEADER) {
                leaderPort = brokerPorts[i];
                break;
            }
        }
        if (leaderPort < 0) throw new IllegalStateException("no Raft leader after ready");
        return new BenchCluster(brokerPorts, brokers, dirs, leaderPort);
    }

    static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void waitForReady(List<Broker> brokers) throws InterruptedException {
        int n = brokers.size();
        var ids = new ArrayList<Integer>();
        for (int i = 1; i <= n; i++) ids.add(i);
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            int leaders = 0;
            for (var b : brokers) if (b.role() == Role.LEADER) leaders++;
            boolean full = true;
            for (var b : brokers) {
                if (!b.brokerRegistry().knownBrokerIds().containsAll(ids)) {
                    full = false;
                    break;
                }
            }
            if (leaders == 1 && full) return;
            Thread.sleep(50);
        }
        throw new IllegalStateException("cluster did not converge in 15s");
    }

    @Override
    public void close() {
        for (var b : brokers) {
            try {
                b.close();
            } catch (Exception ignored) {
                /* best-effort teardown */
            }
        }
        for (var d : tempDirs) {
            deleteRecursively(d);
        }
    }

    private static void deleteRecursively(Path p) {
        try {
            if (!Files.exists(p)) return;
            try (var stream = Files.walk(p)) {
                stream.sorted(java.util.Comparator.reverseOrder()).forEach(x -> {
                    try {
                        Files.deleteIfExists(x);
                    } catch (IOException ignored) {
                        /* best-effort teardown */
                    }
                });
            }
        } catch (IOException ignored) {
            /* best-effort teardown */
        }
    }
}
