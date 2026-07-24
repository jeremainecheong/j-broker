package jbroker.broker.client.consumer;

import jbroker.broker.client.ClusterClient;
import jbroker.proto.broker.CommitOffsetsRequest;
import jbroker.proto.broker.CommitOffsetsResponse;
import jbroker.proto.broker.ConsumerGroupHeartbeatRequest;
import jbroker.proto.broker.ConsumerGroupHeartbeatResponse;
import jbroker.proto.broker.FetchOffsetsRequest;
import jbroker.proto.broker.FetchOffsetsResponse;
import jbroker.proto.broker.FetchRequest;
import jbroker.proto.broker.FetchResponse;
import jbroker.proto.broker.ListOffsetsRequest;
import jbroker.proto.broker.ListOffsetsResponse;
import jbroker.proto.broker.ProduceRequest;
import jbroker.proto.broker.ProduceResponse;

/**
 * Cluster-routed {@link ConsumerRpc}: every call goes through a shared
 * {@link ClusterClient}, which resolves the coordinator / partition leader
 * and retries through failover. Per-tick calls (heartbeat, fetch) get a
 * short budget — one poll deadline — and map retry exhaustion to
 * {@code null} so a poll during an election window simply comes back
 * empty; offset-durability calls (commit, committed-offset and log-bounds
 * lookups) keep retrying up to the client's full call deadline before
 * reporting failure, because a dropped commit is worse than a slow one.
 *
 * <p>Does not close the shared {@link ClusterClient} on {@link #close()} —
 * the application owns it and may be feeding a producer over the same
 * channels.
 */
final class ClusterConsumerRpc implements ConsumerRpc {

    private final ClusterClient cluster;
    private final ConsumerConfig cfg;

    ClusterConsumerRpc(ClusterClient cluster, ConsumerConfig cfg) {
        this.cluster = cluster;
        this.cfg = cfg;
    }

    private long tickBudgetMs() {
        return cfg.pollFetchDeadline().toMillis();
    }

    @Override
    public ConsumerGroupHeartbeatResponse heartbeat(ConsumerGroupHeartbeatRequest req) {
        try {
            return cluster.consumerGroupHeartbeat(req, tickBudgetMs());
        } catch (ClusterClient.RetriesExhaustedException e) {
            return null; // no coordinator this tick — next poll retries
        }
    }

    @Override
    public void invalidateCoordinator() {
        cluster.invalidateCoordinator(cfg.groupId());
    }

    @Override
    public CommitOffsetsResponse commitOffsets(CommitOffsetsRequest req) {
        try {
            return cluster.commitOffsets(req, cluster.config().callDeadlineMs());
        } catch (ClusterClient.RetriesExhaustedException e) {
            return null; // consumer raises "coordinator not available"
        }
    }

    @Override
    public FetchOffsetsResponse fetchOffsets(FetchOffsetsRequest req) {
        try {
            return cluster.fetchOffsets(req, cluster.config().callDeadlineMs());
        } catch (ClusterClient.RetriesExhaustedException e) {
            return null; // consumer reports "never committed"
        }
    }

    @Override
    public ListOffsetsResponse listOffsets(ListOffsetsRequest req) {
        try {
            return cluster.listOffsets(req, cluster.config().callDeadlineMs());
        } catch (ClusterClient.RetriesExhaustedException e) {
            return null; // consumer raises "coordinator not available"
        }
    }

    @Override
    public FetchResponse fetch(FetchRequest req) {
        try {
            return cluster.fetch(req, tickBudgetMs());
        } catch (ClusterClient.RetriesExhaustedException e) {
            return null; // partition leaderless this tick — next poll retries
        }
    }

    @Override
    public ProduceResponse produceDlt(ProduceRequest req) {
        return cluster.produce(req, cluster.config().callDeadlineMs());
    }

    @Override
    public void leaveGroup(ConsumerGroupHeartbeatRequest req) {
        try {
            cluster.consumerGroupHeartbeat(req, 2_000);
        } catch (Exception ignored) {
            // best-effort — session timeout evicts the member otherwise
        }
    }

    @Override
    public void close() {
        // The ClusterClient is shared and application-owned; nothing to do.
    }
}
