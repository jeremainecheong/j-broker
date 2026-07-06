package jbroker.app.testkit;

import java.nio.file.Path;
import java.util.List;
import jbroker.app.Broker;
import jbroker.app.VoterAddress;
import jbroker.raft.core.NodeId;

/**
 * Single-node broker starters with whole-attempt bind retry (see
 * {@link BindRetry}). Read the actual ports back from the returned broker
 * ({@link Broker#brokerPort()}) — a retry re-allocates them.
 */
public final class TestBrokers {

    private TestBrokers() {}

    /** Node id 1, no voter list (single-node metadata quorum, 4-arg config). */
    public static Broker startSingleNode(Path dataDir) throws Exception {
        return BindRetry.startWithBindRetry(() ->
                Broker.start(new Broker.Config(new NodeId(1), dataDir, BindRetry.freePort(), BindRetry.freePort())));
    }

    /** Node id 1 with an explicit self-voter list (5-arg config). */
    public static Broker startSingleVoter(Path dataDir) throws Exception {
        return BindRetry.startWithBindRetry(() -> {
            int raftPort = BindRetry.freePort();
            int brokerPort = BindRetry.freePort();
            var voters = List.of(new VoterAddress(new NodeId(1), "127.0.0.1", raftPort, brokerPort));
            return Broker.start(new Broker.Config(new NodeId(1), dataDir, raftPort, brokerPort, voters));
        });
    }

    /** Builds a config from the attempt's fresh (raftPort, brokerPort) pair. */
    @FunctionalInterface
    public interface ConfigFactory {
        Broker.Config config(int raftPort, int brokerPort) throws Exception;
    }

    /**
     * Started single node plus the ports it actually bound — for tests
     * that decorate the config (TLS, offsets-partitions, advertised
     * listeners) or deliberately restart a fresh instance on the same
     * ports afterwards (recovery ITs; the restart itself needs no retry:
     * the test just released those ports).
     */
    public record SingleNode(Broker broker, int raftPort, int brokerPort) implements AutoCloseable {
        @Override
        public void close() {
            broker.close();
        }
    }

    public static SingleNode start(ConfigFactory factory) throws Exception {
        return BindRetry.startWithBindRetry(() -> {
            int raftPort = BindRetry.freePort();
            int brokerPort = BindRetry.freePort();
            return new SingleNode(Broker.start(factory.config(raftPort, brokerPort)), raftPort, brokerPort);
        });
    }
}
