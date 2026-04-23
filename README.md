# j-broker

Learning project: a log-structured distributed message broker with a hand-rolled Raft implementation, targeting a 3-node combined-mode cluster. See `j-broker-prd.md` (not tracked in git) for the full spec.

## Status

- [x] **Milestone 0** — Gradle multi-module scaffolding, ArchUnit module boundaries, CI
- [x] **Milestone 1** — Raft core: election, log replication, fsync'd persistent state, conflict-index backoff. Unit tests + 4 integration tests + 100-run election stress all green.
- [ ] Milestone 2 — Raft snapshots, membership changes, leadership transfer, pre-vote, read-index
- [ ] Milestone 3–10 — see the spec

## Quick start (Docker)

```
docker compose up
```

Brings up a 3-broker cluster + admin UI in an isolated bridge network:

- **Admin UI** → <http://localhost:15672> (RabbitMQ-management-plugin convention)
- **Broker gRPC** → `localhost:9092` (broker 1), `localhost:9093` (broker 2), `localhost:9094` (broker 3)
- **Chaos HTTP** — disabled by default; `JBROKER_CHAOS_PORT=9100 docker compose up` binds it on host ports 9100/9101/9102

Broker data (Raft state + partition logs) persists in named volumes `broker{1,2,3}-data`. Wipe with `docker compose down -v`.

Prometheus + Grafana live in `docker-compose.monitoring.yml`:

```
docker compose -f docker-compose.yml -f docker-compose.monitoring.yml --profile monitoring up
```

## Build

```
./gradlew build                             # all modules + unit/integration tests
./gradlew :integration-tests:stressTest     # 100-run election stress
./gradlew spotlessApply                     # reformat
```

Requires Java 21 (Temurin). The Gradle wrapper is pinned to 8.7 and verifies the distribution SHA-256.

## Architecture

Modules that exist today:

- `proto/` — shared `.proto` + generated gRPC stubs
- `raft-core/` — pure Java, zero IO/threads/Spring/gRPC. Step-function Raft state machine over sealed `RaftEvent` → `List<RaftEffect>`. Any dependency on Spring/gRPC/Jakarta from this module is blocked by ArchUnit.
- `raft-transport/` — gRPC server, outbound peer client, and the driver loop that runs the core on a virtual thread, fans out AppendEntries / vote RPCs on structured virtual threads, and translates between proto messages and core events
- `integration-tests/` — real 3-node loopback cluster tests

**Milestone 1 design decision: pure step-function core + driver loop.** `RaftCore.step(event)` is deterministic and produces a list of effects (send-this-RPC, persist-this-log, apply-this-entry). A separate `RaftDriver` owns the event loop, runs one event at a time on a virtual thread, and executes effects via IO workers. This is the pattern used by etcd/raft, TiKV/raft-rs, and TigerBeetle's VSR. It keeps the core deterministic (Milestone 3's simulator becomes trivial), sidesteps virtual-thread pinning (no `synchronized` on hot paths), and exercises Java 21 sealed types + pattern matching organically.

## Testing

- **Unit tests:** ~50 across `raft-core` and `raft-transport`, covering log append/read/fsync/recovery, persistent state, election safety, vote acceptance with log-completeness check, AppendEntries log-matching + truncation + commit advance, leader matchIndex majority commit (§5.4.2), conflict-index fast backoff, client proposal rejection on non-leader, and persistence round-trips.
- **Integration tests:** 4 tests in `integration-tests/src/test/java/jbroker/it/ThreeNodeRaftIT.java` — leader election within 1 s, kill-non-leader non-disruption, kill-leader failover within 5 s, replicate 1000 entries. `ClusterHarness.start()` blocks until every outbound gRPC channel reaches `ConnectivityState.READY`, so the first election's vote RPCs are delivered immediately rather than queued behind an HTTP/2 handshake — this eliminated the back-to-back rerun flakiness that affected earlier builds.
- **Stress:** `ElectionStressIT` runs 100 randomized election cycles; `./gradlew :integration-tests:stressTest` completes 100/100 in ~60 s.

## Notable Raft correctness invariants covered

- **Election safety** via `currentTerm`/`votedFor` fsynced before any RequestVote response.
- **Log matching** via `prevLogIndex`/`prevLogTerm` check on every AppendEntries.
- **Commit rule §5.4.2** — leader only advances commitIndex by majority match on entries of its own term.
- **Conflict-index fast backoff** — follower returns the first index of its conflicting term on AppendEntries failure; leader jumps `nextIndex[peer]` directly there.
- **Torn-write recovery** — `FileRaftLog` rehydrates by skipping incomplete trailing frames and hard-caps per-frame payloads at 64 MiB against corrupted length prefixes.
- **Defensive immutability** — `LogEntry.payload()` returns a defensive copy; equality and hash are content-based via `Arrays.equals`/`Arrays.hashCode`.

## Monitoring (Milestone 9)

Bring up Prometheus + Grafana locally against a running admin-app:

```
# with admin-app running on localhost:9090 and brokers registered to it
docker compose -f docker-compose.monitoring.yml --profile monitoring up
```

- **Prometheus** → `http://localhost:9091` (host port 9091 → container 9090; admin-app keeps 9090)
- **Grafana** → `http://localhost:3000` (anonymous admin)
- **Dashboards** — auto-provisioned from `scripts/monitoring/grafana/dashboards/`:
  - *j-broker Cluster Overview* — produce/fetch throughput + latency percentiles + Raft state per broker
  - *j-broker Partitions* — ISR size, HWM, per-follower replication lag per (topic, partition)

Prometheus scrapes `admin-app:9090/actuator/prometheus` which exposes the full `jbroker_*` meter family on the cluster — the admin-app's `MetricsScraper` pulls `DescribeMetrics` from every broker every 5s and republishes tagged by `broker_id`.

## Profiling (Milestone 9)

The broker emits six custom JFR events on hot paths, plus integrates cleanly with `async-profiler`.

**Custom JFR events** (all under category `j-broker`):

| Event | Where | Fields |
|---|---|---|
| `jbroker.RaftTermChange` | `DefaultRaftCore.becomeFollower` | oldTerm, newTerm, reason |
| `jbroker.PartitionLeaderChange` | `TopicManager.onPartitionChange` | topic, partition, oldLeader, newLeader, leaderEpoch |
| `jbroker.FsyncDuration` | `LogSegment.force` | baseOffset, durationNanos, sizeBytes |
| `jbroker.ReplicationLag` | `ReplicaFetchHandler.handle` (leader-side; ≥10 records lag) | topic, partition, followerBrokerId, lagRecords |
| `jbroker.ProduceLatency` | `ProduceHandler.handle` | topic, partition, latencyNanos, bytes, acks |
| `jbroker.FetchLatency` | `FetchHandler.handle` | topic, partition, latencyNanos, bytes |

**Start a JFR recording on a running broker:**

```
jcmd <PID> JFR.start duration=30s filename=broker.jfr settings=profile
```

Open `broker.jfr` in [JDK Mission Control](https://www.oracle.com/java/technologies/jdk-mission-control.html); custom events show up under *Event Browser → j-broker*.

**Flame graph via [async-profiler](https://github.com/jvm-profiling-tools/async-profiler):**

```
asprof -d 30 -f flame.html <PID>                       # CPU flame graph
asprof -d 30 -e alloc -f alloc.html <PID>              # allocation profile
asprof -d 30 -e lock -f lock.html <PID>                # lock contention (watch virtual-thread pinning!)
```

On Linux, ensure `perf_event_paranoid` is ≤ 1 and kernel symbols are accessible. Frame-pointer mode works out of the box; if you see truncated Java stacks, launch the JVM with `-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints`.

## Java 21 virtual-thread model (Milestone 10)

The broker's data plane is virtual-thread-friendly end-to-end:

- **Serialisation primitives** — `Log` and `LogSegment` use `ReentrantLock` (). `synchronized` around blocking `FileChannel.write/force` pins the carrier OS thread; `ReentrantLock.park/unpark` does not. `VirtualThreadPinningIT` drives 200 concurrent produces against a 3-broker cluster under a JFR recording and asserts **zero** `jdk.VirtualThreadPinned` events on the produce path.
- **Structured fan-out** — cluster-wide admin RPCs (`MetricsScraper`, `RaftController`) run per-broker subtasks under `Executors.newVirtualThreadPerTaskExecutor()` wrapped in try-with-resources so the enclosing call waits for every fork before returning — structured-concurrency semantics without the preview `StructuredTaskScope`.
- **Pattern matching** — `MetadataStateMachine.apply` dispatches via `switch (record.getKindCase())` over the proto oneof enum. Compiler enforces exhaustiveness; a new `MetadataRecord` variant without an apply branch is a build failure.

**10k concurrent connections:** exercise locally via `scripts/demo/stress-10k-clients.sh` (not yet committed; see the handoff memory for the queued follow-up). The in-repo regression test drives 200 concurrent clients to keep CI wall-clock predictable — that's sufficient to validate the pinning assertion, which is the acceptance gate-load-bearing claim.
