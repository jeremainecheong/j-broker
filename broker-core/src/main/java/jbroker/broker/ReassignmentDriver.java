package jbroker.broker;

import java.util.ArrayList;
import java.util.List;

/**
 * Advances every in-flight partition reassignment by one step, ticked on the
 * controller. Mirrors {@link jbroker.broker.replication.IsrManager}: the
 * decision is a pure sweep over replicated state, and the caller proposes
 * whatever bytes it returns.
 *
 * <p>For each pending reassignment it reads the partition's current state and
 * asks {@link ReassignmentPlanner} for the next record — expand, contract, or
 * clear — collecting the serialized proposals. Partitions whose state is not
 * yet known (a race with topic creation) are skipped and retried next tick.
 */
public final class ReassignmentDriver {

    private final TopicManager topicManager;
    private final ReassignmentStore store;

    public ReassignmentDriver(TopicManager topicManager, ReassignmentStore store) {
        this.topicManager = topicManager;
        this.store = store;
    }

    /** Serialized {@code MetadataRecord}s to propose this tick; empty when nothing should change. */
    public List<byte[]> decide() {
        var out = new ArrayList<byte[]>();
        for (var pending : store.list()) {
            var state = topicManager
                    .partitionState(pending.topic(), pending.partition())
                    .orElse(null);
            if (state == null) continue;
            ReassignmentPlanner.plan(state, pending).ifPresent(rec -> out.add(rec.toByteArray()));
        }
        return out;
    }
}
