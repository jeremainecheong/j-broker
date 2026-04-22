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
