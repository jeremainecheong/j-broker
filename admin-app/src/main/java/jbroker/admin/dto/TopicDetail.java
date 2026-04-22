package jbroker.admin.dto;

import java.util.List;
import java.util.Map;

public record TopicDetail(
        String name,
        int partitions,
        int replicationFactor,
        boolean internal,
        boolean compact,
        long createdMillis,
        Map<String, String> config,
        List<PartitionState> partitionStates) {

    public TopicDetail {
        config = config == null ? Map.of() : Map.copyOf(config);
        partitionStates = partitionStates == null ? List.of() : List.copyOf(partitionStates);
    }

    public record PartitionState(
            int partition,
            int leader,
            List<Integer> isr,
            List<Integer> replicas,
            int leaderEpoch,
            int partitionEpoch,
            long highWatermark,
            long logEndOffset) {

        public PartitionState {
            isr = isr == null ? List.of() : List.copyOf(isr);
            replicas = replicas == null ? List.of() : List.copyOf(replicas);
        }
    }
}
