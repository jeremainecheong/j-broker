package jbroker.admin.dto;

import java.util.List;

public record ConsumerGroupDetail(
        String groupId,
        String state,
        int generation,
        String assignor,
        List<Member> members,
        List<PartitionLag> partitions) {

    public ConsumerGroupDetail {
        members = members == null ? List.of() : List.copyOf(members);
        partitions = partitions == null ? List.of() : List.copyOf(partitions);
    }

    public record Member(
            String memberId,
            String instanceId,
            int memberEpoch,
            List<String> subscribedTopics,
            List<OwnedPartition> ownedPartitions) {
        public Member {
            subscribedTopics = subscribedTopics == null ? List.of() : List.copyOf(subscribedTopics);
            ownedPartitions = ownedPartitions == null ? List.of() : List.copyOf(ownedPartitions);
        }
    }

    public record OwnedPartition(String topic, List<Integer> partitions) {
        public OwnedPartition {
            partitions = partitions == null ? List.of() : List.copyOf(partitions);
        }
    }

    public record PartitionLag(
            String topic, int partition, long committedOffset, long highWatermark, long lag, String ownerMemberId) {}
}
