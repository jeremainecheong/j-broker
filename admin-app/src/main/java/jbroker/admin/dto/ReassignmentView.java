package jbroker.admin.dto;

import java.util.List;
import jbroker.proto.broker.ReassignmentInfo;

/** One pending partition reassignment, as listed by the replicated cache. */
public record ReassignmentView(
        String topic, int partition, List<Integer> targetReplicas, List<Integer> originalReplicas) {

    public static ReassignmentView of(ReassignmentInfo info) {
        return new ReassignmentView(
                info.getTopic(), info.getPartition(), info.getTargetReplicasList(), info.getOriginalReplicasList());
    }
}
