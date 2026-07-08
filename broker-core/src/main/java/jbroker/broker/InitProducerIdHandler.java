package jbroker.broker;

import java.util.concurrent.TimeUnit;
import jbroker.proto.broker.Error;
import jbroker.proto.broker.InitProducerIdRequest;
import jbroker.proto.broker.InitProducerIdResponse;
import jbroker.proto.raft.MetadataRecord;
import jbroker.proto.raft.ProducerIdAssignmentRecord;

/**
 * Handles the {@code InitProducerId} RPC: allocates a fresh
 * {@code (producer_id, producer_epoch)} pair and commits a
 * {@code ProducerIdAssignmentRecord} through Raft so the counter survives
 * restart.
 *
 * <p>Only producer_epoch=0 is issued on initial allocation. Epoch
 * bumping (for producer-id re-use on transactional restart) is part of
 * the transactional-producer work, which is out of scope here.
 */
public final class InitProducerIdHandler {

    private final ProducerIdRegistry registry;
    private final AdminHandler.MetadataProposer proposer;

    public InitProducerIdHandler(ProducerIdRegistry registry, AdminHandler.MetadataProposer proposer) {
        this.registry = registry;
        this.proposer = proposer;
    }

    public InitProducerIdResponse initProducerId(InitProducerIdRequest req) {
        // allocateNext is atomic on the proposer side so concurrent
        // InitProducerId calls can't assign the same id even before their
        // Raft records commit. Ids burned on propose failure are safe in
        // single-broker mode because a restart replays the Raft log and
        // the registry advances only on successful apply; multi-broker
        // failover will need the controller to track propose
        // outcomes so a failed InitProducerId on an outgoing leader
        // doesn't collide with a fresh allocation on the new leader.
        long id = registry.allocateNext();
        var record = MetadataRecord.newBuilder()
                .setProducerIdAssignment(ProducerIdAssignmentRecord.newBuilder()
                        .setProducerId(id)
                        .setProducerEpoch(0)
                        .setNextProducerId(id + 1)
                        .build())
                .build();
        try {
            proposer.proposeAndWait(record.toByteArray(), TimeUnit.SECONDS.toMillis(5));
        } catch (Exception e) {
            return InitProducerIdResponse.newBuilder()
                    .setError(Error.newBuilder()
                            .setCode(ErrorCodes.NOT_LEADER)
                            .setMessage(e.getMessage() == null ? e.toString() : e.getMessage())
                            .build())
                    .build();
        }
        return InitProducerIdResponse.newBuilder()
                .setProducerId(id)
                .setProducerEpoch(0)
                .build();
    }
}
