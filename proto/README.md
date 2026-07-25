# proto

Single source of truth for every RPC + on-wire record type in the cluster. `src/main/proto/*.proto` → generated Java stubs via the `com.google.protobuf` + `io.grpc:protoc-gen-grpc-java` plugins. Four files: `broker.proto` (data + admin + metadata planes), `txn.proto` (transactions), `raft.proto` (Raft plane + metadata records), `common.proto` (the `ErrorCode` enum, `BrokerEndpoint`, `TopicPartition`).

## Service surface at a glance

```mermaid
flowchart LR
    Client((Client)) --> Producer
    Client --> Consumer
    Client --> Txn
    Client --> TxnOffsets
    Admin_app((admin-app)) --> Admin
    Admin_app --> Metadata
    CLI((j-broker CLI)) --> Producer
    CLI --> Consumer
    CLI --> Admin

    subgraph DataPlane[Data plane — exposed to clients]
        Producer[Producer<br/>InitProducerId · Produce]
        Consumer[Consumer<br/>Fetch · ListOffsets · FindCoordinator ·<br/>ConsumerGroupHeartbeat ·<br/>CommitOffsets · FetchOffsets]
        Txn[Txn<br/>InitTransactions · AddPartitionsToTxn ·<br/>EndTxn · AddOffsetsToTxn]
        TxnOffsets[TxnOffsets<br/>TxnOffsetCommit]
        Admin[Admin<br/>topic + group CRUD · ACLs ·<br/>cluster lifecycle&colon; AddBroker ·<br/>DecommissionBroker · reassignments ·<br/>RebalanceLeadership]
        Metadata[Metadata<br/>DescribeCluster · DescribeTopicPartitions ·<br/>consumer-group describes · DescribeRaft ·<br/>ApiVersions · DescribeMetrics ·<br/>SubscribeEvents]
    end

    subgraph InternalPlane[Internal plane — broker ↔ broker only]
        Cluster[Cluster<br/>BrokerHeartbeat]
        ReplicaConsumer[ReplicaConsumer<br/>ReplicaFetch ·<br/>OffsetsForLeaderEpoch]
        TxnMarkers[TxnMarkers<br/>WriteTxnMarkers]
        RaftSvc[Raft<br/>AppendEntries · RequestVote ·<br/>TimeoutNow · InstallSnapshot · Propose]
    end

    Broker((Broker)) -.heartbeat.-> Broker
    Broker -.ReplicaFetch.-> Broker
    Broker -.WriteTxnMarkers.-> Broker
    Broker -.Raft.-> Broker
```

Public services are reachable from any client; internal services are only used between brokers and require mTLS in production deployments.

## Services

| Service | File | Consumers |
|---|---|---|
| `Producer` | `broker.proto` | Client produce path — `InitProducerId`, `Produce`. |
| `Consumer` | `broker.proto` | Client fetch path — `Fetch`, `ListOffsets`, `FindCoordinator`, `ConsumerGroupHeartbeat`, `CommitOffsets`, `FetchOffsets`. |
| `Txn` | `txn.proto` | Transaction coordinator surface, routed on `transactional_id` — `InitTransactions`, `AddPartitionsToTxn`, `EndTxn`, `AddOffsetsToTxn`. |
| `TxnOffsets` | `txn.proto` | Group-coordinator half of transactional offsets, routed on `group_id` — `TxnOffsetCommit`. |
| `Admin` | `broker.proto` | `CreateTopic`, `DeleteTopic`, `UpdateTopicConfig`, `ListTopics`, `DescribeTopic`, `ForceCompactPartition`, `DeleteConsumerGroup`, `ResetConsumerGroupOffsets`, ACLs (`CreateAcl`, `DeleteAcl`, `ListAcls`), cluster lifecycle (`AddBroker`, `DecommissionBroker`, `DescribeMembership`, `ReassignPartition`, `ListReassignments`, `CancelReassignment`, `RebalanceLeadership`). |
| `Metadata` | `broker.proto` | `DescribeCluster`, `DescribeTopicPartitions`, `ListConsumerGroups`, `DescribeConsumerGroup`, `DescribeRaft`, `ApiVersions`, `DescribeMetrics`, `SubscribeEvents`. |
| `Cluster` | `broker.proto` | Broker ↔ broker: `BrokerHeartbeat`. |
| `ReplicaConsumer` | `broker.proto` | Leader ↔ follower: `ReplicaFetch`, `OffsetsForLeaderEpoch`. |
| `TxnMarkers` | `txn.proto` | Coordinator → data-partition leader: `WriteTxnMarkers` delivers commit/abort control batches. |
| `Raft` | `raft.proto` | Raft plane: `AppendEntries`, `RequestVote`, `TimeoutNow`, `InstallSnapshot`, plus `Propose` — a follower forwards a local client propose to the node it believes is leader instead of silently dropping it. |

## Wire format conventions

- JSON rendered by `admin-app` is **snake_case** (Jackson configured in `admin-app/src/main/resources/application.yml`). Protobuf field names translate 1:1.
- Error envelopes embed an `error_code` int and an optional `hint` map — callers can pull `suggested_leader_id`, `suggested_leader_host`, `suggested_leader_port` for `NOT_LEADER` redirects. Transaction responses carry the same idea as first-class `suggested_coordinator_*` fields for `NOT_COORDINATOR`.
- Offset sentinels: `-1` means "unknown / not yet available" across HWM, LEO, committed_offset, lag. Aggregators that fan out across brokers must merge these explicitly, not mean-average them.

## Build

```bash
./gradlew :proto:generateProto   # one-shot codegen
./gradlew :proto:build           # compile + lint
```

Generated code lives in `proto/build/generated/source/proto/main/java/` — not committed.

## Compat policy

Field numbers never get reused. Bumping a schema field keeps the old number reserved. Breaking changes (renaming services, deleting fields) are avoided in favour of adding new fields and deprecating old ones — see `broker.proto`'s comment blocks for ongoing deprecation markers.
