package jbroker.app.testkit;

import java.util.ArrayList;
import java.util.List;
import jbroker.app.Broker;
import jbroker.app.VoterAddress;
import jbroker.raft.core.NodeId;

/**
 * Starts an in-process loopback broker cluster with whole-attempt bind
 * retry (see {@link BindRetry}). Each attempt allocates a fresh port
 * matrix, rebuilds the voter list, and starts brokers sequentially; on a
 * bind race every already-started broker is closed and the attempt is
 * repeated with fresh ports — per-broker retry cannot work because voter
 * lists bake every peer's port into every other broker's config.
 *
 * <p>Port matrix convention: {@code ports[i][0]} = raft port,
 * {@code ports[i][1]} = broker (gRPC) port; extra columns (chaos HTTP,
 * advertised, …) are the test's to interpret. Node ids are 1-based
 * ({@code index + 1}).
 *
 * <pre>{@code
 * try (var cluster = TestBrokerCluster.start(3, 3, (i, voters, ports) ->
 *         withChaos(new Broker.Config(new NodeId(i + 1), dirs[i],
 *                 ports[i][0], ports[i][1], voters), ports[i][2]))) {
 *     var client = new BrokerClient("127.0.0.1", cluster.brokerPort(0));
 *     ...
 * }
 * }</pre>
 */
public final class TestBrokerCluster implements AutoCloseable {

    /** Builds broker {@code index}'s config from the attempt's fresh ports. */
    @FunctionalInterface
    public interface ConfigFactory {
        Broker.Config config(int index, List<VoterAddress> voters, int[][] ports) throws Exception;
    }

    private final List<Broker> brokers;
    private final int[][] ports;

    private TestBrokerCluster(List<Broker> brokers, int[][] ports) {
        this.brokers = List.copyOf(brokers);
        this.ports = ports;
    }

    public static TestBrokerCluster start(int count, int portsPerBroker, ConfigFactory factory) throws Exception {
        if (portsPerBroker < 2) throw new IllegalArgumentException("need at least raft + broker port per broker");
        return BindRetry.startWithBindRetry(() -> {
            var ports = new int[count][];
            for (int i = 0; i < count; i++) ports[i] = BindRetry.freePorts(portsPerBroker);
            var voters = new ArrayList<VoterAddress>(count);
            for (int i = 0; i < count; i++) {
                voters.add(new VoterAddress(new NodeId(i + 1), "127.0.0.1", ports[i][0], ports[i][1]));
            }
            var started = new ArrayList<Broker>(count);
            try {
                for (int i = 0; i < count; i++) {
                    started.add(Broker.start(factory.config(i, List.copyOf(voters), ports)));
                }
            } catch (Exception e) {
                closeAll(started);
                throw e;
            }
            return new TestBrokerCluster(started, ports);
        });
    }

    /** Immutable view; brokers are in start order (index 0 = node id 1). */
    public List<Broker> brokers() {
        return brokers;
    }

    public Broker broker(int index) {
        return brokers.get(index);
    }

    public int port(int index, int col) {
        return ports[index][col];
    }

    public int raftPort(int index) {
        return port(index, 0);
    }

    public int brokerPort(int index) {
        return port(index, 1);
    }

    @Override
    public void close() {
        closeAll(brokers);
    }

    private static void closeAll(List<Broker> toClose) {
        for (var b : toClose) {
            try {
                b.close();
            } catch (Exception ignored) {
                // best-effort teardown; tests that closeAbruptly() a broker
                // mid-test still get the rest cleaned up here.
            }
        }
    }
}
