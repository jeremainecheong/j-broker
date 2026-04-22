package jbroker.admin.dto;

public record ConsumerGroupSummary(
        String groupId, String state, int memberCount, int generation, String assignor, int coordinatorPartition) {}
