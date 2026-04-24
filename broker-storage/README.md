# broker-storage

Per-partition durable log. Owns the on-disk representation — segment files, offset/time indexes, leader-epoch checkpoints, compaction, retention. Every broker has one `LogManager` instance; each partition it replicates gets a `Log` handle.

## Layout on disk

```
<data-dir>/topics/<topic>-<partition>/
    00000000000000000000.log         # segment records
    00000000000000000000.index       # offset → byte position
    00000000000000000000.timeindex   # timestamp → byte position
    00000000000000005000.log
    00000000000000005000.index
    00000000000000005000.timeindex
    leader-epoch-checkpoint
```

The segment filename is the base offset, zero-padded to 20 digits. Rolling to a new segment happens when the active one crosses `segmentBytes` (default 256 MiB).

## Key types

| Type | Purpose |
|---|---|
| `LogManager` | Lifecycle for every `Log` handle on a broker. Schedules the retention + compaction cleaner. |
| `Log` | Per-partition interface — `append`, `read`, `nextOffset`, `force`, `compactByKey`, `retain`. |
| `LogSegment` | Single segment file + its offset/time indexes. Immutable after roll. |
| `OffsetIndex` | Sparse mmap'd offset → byte-position index for fast `read(offset)` resolution. |
| `TimeIndex` | Parallel timestamp index supporting `offsetForTimestamp`. |
| `LeaderEpochCheckpoint` | fsync'd mapping `leader_epoch → end_offset` for the `OffsetsForLeaderEpoch` RPC (Milestone 6 fencing). |

## Compaction + sparse offsets

Log compaction keeps the latest value per key (Kafka-style). The subtle part is **sparse-offset preservation** (): a consumer holding a pre-compaction offset must still resolve to the right post-compaction record, even if every intermediate record between its offset and the survivor was tombstoned.

```mermaid
flowchart LR
    subgraph Pre[Pre-compaction log]
        direction TB
        P0[offset 0: k1=v1]
        P1[offset 1: k2=v2]
        P2[offset 2: k1=v3]
        P3[offset 3: k2=v4]
        P4[offset 4: k1=v5]
    end

    subgraph Post[Post-compaction log]
        direction TB
        PC3[offset 3: k2=v4<br/>sparse]
        PC4[offset 4: k1=v5<br/>sparse]
    end

    Pre -->|compactByKey| Post
```

A consumer that last committed offset 2 and resumes after compaction calls `Fetch(topic, partition, offset=2, max_bytes)`. `Log.segmentContaining(2)` falls forward to the segment whose base is above 2 and returns records `[3, 4]` — consumer sees `{k2=v4, k1=v5}` with correct absolute offsets.

Force-compact () lets operators trigger compaction synchronously instead of waiting on the 5-minute cleaner cadence. Hit via `POST /api/v1/topics/{name}/partitions/{p}/compact` or the per-partition "Force compact" button on the admin UI.

## Retention

`LogManager.Config.retentionMillis` (default 7 days) is enforced by the same background cleaner that handles compaction. On each tick (60s by default):

1. `log.retain(cutoff = now - retentionMillis)` — closes and deletes any segment whose last timestamp is older than the cutoff. The active segment is never eligible.
2. For compact-policy topics, also call `log.compactByKey()` — merges segments, sparse-offset preserving.

## Performance

- Append throughput: sustained >200 MB/s on a laptop SSD, floor of 50 MB/s on GitHub Actions shared disks (see `AppendThroughputTest` — best-of-3 trials).
- Zero-copy fetch path: `LogSegment` exposes a `MappedByteBuffer` slice; `ProduceHandler` writes batches through a `FileChannel.transferFrom` where possible.

## Testing

- ~35 tests: append/read/fsync/recovery, segment roll, offset-index binary search, time-index resolution, compaction tombstoning, sparse-offset fetch, retention eviction, crash recovery with torn frames.
- Property tests: offset-index binary-search invariants under random insert orders.
