package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import jbroker.proto.broker.InitProducerIdRequest;
import jbroker.proto.raft.MetadataRecord;
import org.junit.jupiter.api.Test;

class InitProducerIdHandlerTest {

    @Test
    void firstCallReturnsIdZeroAndProposesAssignmentRecord() throws Exception {
        var registry = new ProducerIdRegistry();
        var proposed = new ArrayList<byte[]>();
        var handler = new InitProducerIdHandler(registry, (payload, timeoutMs) -> {
            proposed.add(payload);
            // Simulate the apply path that the state machine would run.
            var record = MetadataRecord.parseFrom(payload).getProducerIdAssignment();
            registry.applyAssignment(record.getNextProducerId());
        });

        var resp = handler.initProducerId(InitProducerIdRequest.newBuilder().build());

        assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NONE);
        assertThat(resp.getProducerId()).isEqualTo(0L);
        assertThat(resp.getProducerEpoch()).isEqualTo(0);
        assertThat(proposed).hasSize(1);
        var record = MetadataRecord.parseFrom(proposed.get(0)).getProducerIdAssignment();
        assertThat(record.getProducerId()).isEqualTo(0L);
        assertThat(record.getNextProducerId()).isEqualTo(1L);
    }

    @Test
    void consecutiveCallsReturnIncrementingIds() throws Exception {
        var registry = new ProducerIdRegistry();
        var handler = new InitProducerIdHandler(
                registry,
                (payload, timeoutMs) -> registry.applyAssignment(MetadataRecord.parseFrom(payload)
                        .getProducerIdAssignment()
                        .getNextProducerId()));

        var ids = new ArrayList<Long>();
        for (int i = 0; i < 3; i++) {
            ids.add(handler.initProducerId(InitProducerIdRequest.newBuilder().build())
                    .getProducerId());
        }

        assertThat(ids).containsExactly(0L, 1L, 2L);
    }

    @Test
    void proposeFailureReturnsNotLeaderError() {
        var registry = new ProducerIdRegistry();
        var handler = new InitProducerIdHandler(registry, (payload, timeoutMs) -> {
            throw new IllegalStateException("not leader");
        });

        var resp = handler.initProducerId(InitProducerIdRequest.newBuilder().build());
        assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NOT_LEADER);
        assertThat(resp.getError().getMessage()).contains("not leader");
    }
}
