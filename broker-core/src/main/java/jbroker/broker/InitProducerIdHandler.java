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
 * <p>Phase 6.7 only issues producer_epoch=0 on initial allocation. Epoch
 * bumping (for producer-id re-use on transactional restart) is part of
 * the transactional-producer work outside Phase 6 scope.
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
        // Raft records commit. If the Raft propose fails below, the id is
        // leaked — harmless; monotonic ids don't require tight packing.
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
