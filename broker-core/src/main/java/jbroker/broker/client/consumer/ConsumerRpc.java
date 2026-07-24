package jbroker.broker.client.consumer;

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
 * {@link Consumer}'s network seam: every wire call the poll/commit state
 * machine makes, minus the transport and routing decisions. Two
 * implementations exist — {@link SingleEndpointConsumerRpc} preserves the
 * original bootstrap-plus-coordinator channel behavior, and
 * {@link ClusterConsumerRpc} routes each call through a
 * {@link jbroker.broker.client.ClusterClient} so the consumer rides
 * through leader and coordinator failover.
 *
 * <p>Null-return convention: {@code heartbeat} and {@code fetch} return
 * {@code null} when the target broker is unreachable <em>this tick</em> —
 * the consumer skips the tick and retries on the next poll, so transient
 * cluster states never surface to the application. The offset-durability
 * calls ({@code commitOffsets}, {@code fetchOffsets}, {@code listOffsets})
 * return {@code null} only once their implementation has given up on
 * reaching a coordinator; the consumer then raises, because silently
 * dropping a commit would be worse than failing it.
 */
interface ConsumerRpc extends AutoCloseable {

    /** One group heartbeat, or {@code null} when no coordinator is reachable this tick. */
    ConsumerGroupHeartbeatResponse heartbeat(ConsumerGroupHeartbeatRequest req);

    /** Drop the cached coordinator; the next call re-resolves it. */
    void invalidateCoordinator();

    /** Commit offsets via the coordinator, or {@code null} when none is reachable. */
    CommitOffsetsResponse commitOffsets(CommitOffsetsRequest req);

    /** Committed-offset lookup via the coordinator, or {@code null} when none is reachable. */
    FetchOffsetsResponse fetchOffsets(FetchOffsetsRequest req);

    /** Log-bounds lookup, or {@code null} when no broker is reachable. */
    ListOffsetsResponse listOffsets(ListOffsetsRequest req);

    /** One partition fetch, or {@code null} when the partition is unreachable this tick. */
    FetchResponse fetch(FetchRequest req);

    /** Dead-letter-topic produce. Throws on failure — DLT records must not vanish quietly. */
    ProduceResponse produceDlt(ProduceRequest req);

    /** Best-effort leave (member_epoch = -1) during close; must never throw. */
    void leaveGroup(ConsumerGroupHeartbeatRequest req);

    @Override
    void close();
}
