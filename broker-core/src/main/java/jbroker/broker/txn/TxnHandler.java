package jbroker.broker.txn;

import jbroker.broker.BrokerRegistry;
import jbroker.broker.ErrorCodes;
import jbroker.broker.TopicManager;
import jbroker.proto.common.ErrorCode;
import jbroker.proto.txn.AddPartitionsToTxnRequest;
import jbroker.proto.txn.AddPartitionsToTxnResponse;
import jbroker.proto.txn.EndTxnRequest;
import jbroker.proto.txn.EndTxnResponse;
import jbroker.proto.txn.InitTransactionsRequest;
import jbroker.proto.txn.InitTransactionsResponse;

/**
 * Routes the client-facing {@code Txn} RPCs the way the consumer-group
 * surface routes on group id: the coordinator for a transactional_id is
 * the leader of its {@code __transaction_state} partition
 * ({@link TxnStateTopic#partitionFor}). A non-coordinator broker answers
 * {@code NOT_COORDINATOR} with the suggested-coordinator hints filled
 * from the leader's advertised address; while the partition has no leader
 * (topic not yet created, fenced ISR) the answer is
 * {@code COORDINATOR_NOT_AVAILABLE} and the client retries after a
 * backoff. Everything past routing — activation, the append-before-answer
 * contract, marker delivery — lives in {@link TxnCoordinatorRuntime}.
 */
public final class TxnHandler {

    private final TopicManager topicManager;
    private final BrokerRegistry brokerRegistry;
    private final int selfBrokerId;
    private final TxnCoordinatorRuntime runtime;

    /** ACL gate, wired by the broker; {@link jbroker.broker.auth.Authorizer#OPEN} keeps test harnesses open. */
    private jbroker.broker.auth.Authorizer authorizer = jbroker.broker.auth.Authorizer.OPEN;

    public void setAuthorizer(jbroker.broker.auth.Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    public TxnHandler(
            TopicManager topicManager, BrokerRegistry brokerRegistry, int selfBrokerId, TxnCoordinatorRuntime runtime) {
        this.topicManager = topicManager;
        this.brokerRegistry = brokerRegistry;
        this.selfBrokerId = selfBrokerId;
        this.runtime = runtime;
    }

    public InitTransactionsResponse initTransactions(InitTransactionsRequest req) {
        var b = InitTransactionsResponse.newBuilder().setProducerId(-1L).setProducerEpoch(-1);
        if (!authorizer.allowsCurrent("txn", req.getTransactionalId(), "produce")) {
            return b.setError(ErrorCode.UNAUTHORIZED).build();
        }
        var routing = route(req.getTransactionalId());
        if (routing.error() != ErrorCode.OK) {
            b.setError(routing.error());
            fillSuggestedCoordinator(
                    routing,
                    b::setSuggestedCoordinatorId,
                    b::setSuggestedCoordinatorHost,
                    b::setSuggestedCoordinatorPort);
            return b.build();
        }
        var outcome =
                runtime.initTransactions(routing.partition(), req.getTransactionalId(), req.getTransactionTimeoutMs());
        b.setError(toWire(outcome.errorCode()));
        b.setProducerId(outcome.producerId());
        b.setProducerEpoch(outcome.producerEpoch());
        if (outcome.errorCode() == ErrorCodes.NOT_COORDINATOR) {
            // Leadership moved between routing and the append — point the
            // client at wherever it moved to.
            var moved = route(req.getTransactionalId());
            fillSuggestedCoordinator(
                    moved,
                    b::setSuggestedCoordinatorId,
                    b::setSuggestedCoordinatorHost,
                    b::setSuggestedCoordinatorPort);
        }
        return b.build();
    }

    public AddPartitionsToTxnResponse addPartitionsToTxn(AddPartitionsToTxnRequest req) {
        var b = AddPartitionsToTxnResponse.newBuilder();
        if (!authorizer.allowsCurrent("txn", req.getTransactionalId(), "produce")) {
            return b.setError(ErrorCode.UNAUTHORIZED).build();
        }
        var routing = route(req.getTransactionalId());
        if (routing.error() != ErrorCode.OK) {
            b.setError(routing.error());
            fillSuggestedCoordinator(
                    routing,
                    b::setSuggestedCoordinatorId,
                    b::setSuggestedCoordinatorHost,
                    b::setSuggestedCoordinatorPort);
            return b.build();
        }
        int code = runtime.addPartitions(
                routing.partition(),
                req.getTransactionalId(),
                req.getProducerId(),
                req.getProducerEpoch(),
                req.getPartitionsList());
        b.setError(toWire(code));
        if (code == ErrorCodes.NOT_COORDINATOR) {
            var moved = route(req.getTransactionalId());
            fillSuggestedCoordinator(
                    moved,
                    b::setSuggestedCoordinatorId,
                    b::setSuggestedCoordinatorHost,
                    b::setSuggestedCoordinatorPort);
        }
        return b.build();
    }

    public EndTxnResponse endTxn(EndTxnRequest req) {
        var b = EndTxnResponse.newBuilder();
        if (!authorizer.allowsCurrent("txn", req.getTransactionalId(), "produce")) {
            return b.setError(ErrorCode.UNAUTHORIZED).build();
        }
        var routing = route(req.getTransactionalId());
        if (routing.error() != ErrorCode.OK) {
            b.setError(routing.error());
            fillSuggestedCoordinator(
                    routing,
                    b::setSuggestedCoordinatorId,
                    b::setSuggestedCoordinatorHost,
                    b::setSuggestedCoordinatorPort);
            return b.build();
        }
        int code = runtime.endTxn(
                routing.partition(),
                req.getTransactionalId(),
                req.getProducerId(),
                req.getProducerEpoch(),
                req.getCommit());
        b.setError(toWire(code));
        if (code == ErrorCodes.NOT_COORDINATOR) {
            var moved = route(req.getTransactionalId());
            fillSuggestedCoordinator(
                    moved,
                    b::setSuggestedCoordinatorId,
                    b::setSuggestedCoordinatorHost,
                    b::setSuggestedCoordinatorPort);
        }
        return b.build();
    }

    /**
     * {@code AddOffsetsToTxn}: registers the group's
     * {@code __consumer_offsets} partition in the transaction, exactly like
     * {@code AddPartitionsToTxn} would — a thin translation of group id to
     * coordinator partition ({@code floorMod(group_id.hashCode(),
     * partitionCount)}, the {@code FindCoordinator} routing) so the
     * commit/abort marker reaches the partition where {@code
     * TxnOffsetCommit} stages the offsets. Answers
     * {@code COORDINATOR_NOT_AVAILABLE} (retriable) while
     * {@code __consumer_offsets} does not exist yet.
     */
    public jbroker.proto.txn.AddOffsetsToTxnResponse addOffsetsToTxn(jbroker.proto.txn.AddOffsetsToTxnRequest req) {
        var b = jbroker.proto.txn.AddOffsetsToTxnResponse.newBuilder();
        if (!authorizer.allowsCurrent("txn", req.getTransactionalId(), "produce")) {
            return b.setError(ErrorCode.UNAUTHORIZED).build();
        }
        var routing = route(req.getTransactionalId());
        if (routing.error() != ErrorCode.OK) {
            b.setError(routing.error());
            fillSuggestedCoordinator(
                    routing,
                    b::setSuggestedCoordinatorId,
                    b::setSuggestedCoordinatorHost,
                    b::setSuggestedCoordinatorPort);
            return b.build();
        }
        var offsetsDesc = topicManager.describe(jbroker.broker.ConsumerOffsetsTopic.NAME);
        if (offsetsDesc.isEmpty()) {
            return b.setError(ErrorCode.COORDINATOR_NOT_AVAILABLE).build();
        }
        int offsetsPartition =
                Math.floorMod(req.getGroupId().hashCode(), offsetsDesc.get().partitions());
        int code = runtime.addPartitions(
                routing.partition(),
                req.getTransactionalId(),
                req.getProducerId(),
                req.getProducerEpoch(),
                java.util.List.of(jbroker.proto.common.TopicPartition.newBuilder()
                        .setTopic(jbroker.broker.ConsumerOffsetsTopic.NAME)
                        .setPartition(offsetsPartition)
                        .build()));
        b.setError(toWire(code));
        if (code == ErrorCodes.NOT_COORDINATOR) {
            var moved = route(req.getTransactionalId());
            fillSuggestedCoordinator(
                    moved,
                    b::setSuggestedCoordinatorId,
                    b::setSuggestedCoordinatorHost,
                    b::setSuggestedCoordinatorPort);
        }
        return b.build();
    }

    // --- routing ---

    private record Routing(ErrorCode error, int partition, int leaderId, String leaderHost, int leaderPort) {}

    private Routing route(String txnId) {
        var desc = topicManager.describe(TxnStateTopic.NAME);
        if (desc.isEmpty()) {
            return new Routing(ErrorCode.COORDINATOR_NOT_AVAILABLE, -1, -1, "", 0);
        }
        int partition = TxnStateTopic.partitionFor(txnId, desc.get().partitions());
        var ps = topicManager.partitionState(TxnStateTopic.NAME, partition);
        if (ps.isEmpty() || ps.get().leader() <= 0) {
            return new Routing(ErrorCode.COORDINATOR_NOT_AVAILABLE, partition, -1, "", 0);
        }
        int leader = ps.get().leader();
        if (leader == selfBrokerId) {
            return new Routing(ErrorCode.OK, partition, leader, "", 0);
        }
        // Clients dial coordinators from outside the broker network, so
        // the hint carries the advertised address (internal fallback is
        // built into the registry lookup).
        var address = brokerRegistry.advertisedAddressFor(leader);
        if (address.isEmpty()) {
            // Leader known but not registered yet — indistinguishable from
            // "not available" for the client; it retries shortly.
            return new Routing(ErrorCode.COORDINATOR_NOT_AVAILABLE, partition, -1, "", 0);
        }
        return new Routing(
                ErrorCode.NOT_COORDINATOR,
                partition,
                leader,
                address.get().host(),
                address.get().port());
    }

    private static void fillSuggestedCoordinator(
            Routing routing,
            java.util.function.IntConsumer id,
            java.util.function.Consumer<String> host,
            java.util.function.IntConsumer port) {
        if (routing.leaderId() <= 0 || routing.leaderHost().isEmpty()) return;
        id.accept(routing.leaderId());
        host.accept(routing.leaderHost());
        port.accept(routing.leaderPort());
    }

    /**
     * Internal codes → wire enum. Every code the runtime can answer with
     * shares its numeric with {@code common.proto::ErrorCode}.
     */
    private static ErrorCode toWire(int code) {
        if (code == ErrorCodes.NONE) return ErrorCode.OK;
        var wire = ErrorCode.forNumber(code);
        return wire == null ? ErrorCode.UNKNOWN : wire;
    }
}
