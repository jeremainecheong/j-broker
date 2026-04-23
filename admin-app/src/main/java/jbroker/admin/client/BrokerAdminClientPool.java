package jbroker.admin.client;

import io.grpc.StatusRuntimeException;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import jbroker.proto.broker.DescribeClusterResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Fixed-size pool of {@link BrokerAdminClient} — one per configured broker.
 * REST controllers fan calls across the pool when they need cluster-wide
 * data (e.g. per-node Raft state) or first-success-among-brokers semantics
 * (e.g. {@code DescribeCluster} — any broker's metadata replica will do).
 *
 * <p>Bean lifecycle: constructed eagerly from {@code jbroker.admin.brokers}
 * config (comma-separated {@code host:port}), channels closed on Spring
 * shutdown.
 */
@Component
public class BrokerAdminClientPool {

    private static final Logger log = LoggerFactory.getLogger(BrokerAdminClientPool.class);

    private final List<BrokerAdminClient> clients;

    public BrokerAdminClientPool(@Value("${jbroker.admin.brokers:localhost:9092}") String brokers) {
        var parsed = new ArrayList<BrokerAdminClient>();
        for (var entry : brokers.split(",")) {
            var trimmed = entry.trim();
            if (trimmed.isEmpty()) continue;
            parsed.add(BrokerAdminClient.parse(trimmed));
        }
        if (parsed.isEmpty()) {
            throw new IllegalStateException("jbroker.admin.brokers must list at least one broker host:port");
        }
        this.clients = List.copyOf(parsed);
    }

    public List<BrokerAdminClient> clients() {
        return clients;
    }

    /**
     * Fans {@code op} out to every broker, returning a list of the responses
     * that succeeded. Order of the returned list matches the order of
     * {@link #clients()}. Throws if every broker is unreachable.
     *
     * <p>Used by the topic-detail merge flow (): only the partition
     * leader populates real {@code highWatermark} / {@code logEndOffset} in
     * {@code DescribeTopicPartitions}, so we fan out to every replica and
     * pick the leader's answer per partition.
     */
    public <T> List<T> allSuccessful(Function<BrokerAdminClient, T> op) {
        var out = new java.util.ArrayList<T>();
        RuntimeException last = null;
        for (var c : clients) {
            try {
                out.add(op.apply(c));
            } catch (StatusRuntimeException e) {
                log.debug("broker {} failed with {}; skipping", c.address(), e.getStatus());
                last = e;
            }
        }
        if (out.isEmpty()) {
            throw new IllegalStateException("every broker unreachable", last);
        }
        return out;
    }

    /**
     * Tries {@code op} against each broker in order and returns the first
     * successful result. Swallows {@link StatusRuntimeException} so a single
     * unreachable broker doesn't bring down the admin UI — but does surface
     * the exception if every broker fails.
     */
    public <T> T firstSuccessful(Function<BrokerAdminClient, T> op) {
        RuntimeException last = null;
        for (var c : clients) {
            try {
                return op.apply(c);
            } catch (StatusRuntimeException e) {
                log.debug("broker {} failed with {}; trying next", c.address(), e.getStatus());
                last = e;
            }
        }
        throw new IllegalStateException("every broker unreachable", last);
    }

    /**
     * Convenience wrapper: describe the cluster via whichever broker answers
     * first. Metadata replicas are identical across every voter so first-
     * success is good enough for Milestone 8 reads.
     */
    public DescribeClusterResponse describeCluster() {
        return firstSuccessful(BrokerAdminClient::describeCluster);
    }

    /** Returns the address of the broker that currently acts as Raft leader, if any. */
    public Optional<String> controllerAddress(DescribeClusterResponse resp) {
        int controllerId = resp.getControllerId();
        if (controllerId < 0) return Optional.empty();
        for (var n : resp.getNodesList()) {
            if (n.getBrokerId() == controllerId && !n.getHost().isEmpty() && n.getPort() > 0) {
                return Optional.of(n.getHost() + ":" + n.getPort());
            }
        }
        return Optional.empty();
    }

    /**
     * Mutation variant: iterate every broker, return the first non-NOT_LEADER
     * response. If every broker returns NOT_LEADER (or none is reachable),
     * returns whatever the last broker said — preserving the hint the admin
     * REST layer surfaces to the caller.
     *
     * <p>Admin-app's topic CRUD uses this: any non-leader broker knows who the
     * Raft leader is (via `BrokerRegistry`), so two passes at worst land on
     * the right broker. Milestone 8 scope; fancier hint-following is Milestone 9.
     */
    public <T> T firstNonNotLeader(
            Function<BrokerAdminClient, T> op, Function<T, Integer> errorCode, int notLeaderCode) {
        T last = null;
        RuntimeException lastExc = null;
        for (var c : clients) {
            try {
                last = op.apply(c);
                int code = errorCode.apply(last);
                if (code != notLeaderCode) {
                    return last;
                }
            } catch (StatusRuntimeException e) {
                log.debug("broker {} failed with {}; trying next", c.address(), e.getStatus());
                lastExc = e;
            }
        }
        if (last != null) return last; // all returned NOT_LEADER — propagate the hint upward
        throw new IllegalStateException("every broker unreachable", lastExc);
    }

    @PreDestroy
    public void shutdown() {
        for (var c : clients) {
            try {
                c.close();
            } catch (Exception ignored) {
                // best-effort; next shutdown hook runs regardless
            }
        }
    }
}
