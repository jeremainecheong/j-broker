# integration-tests

Real 3-node loopback-cluster ITs and heavy scenario tests. Anything that boots multiple `Broker`s and exercises cross-broker interactions lives here; single-broker handler tests live in `broker-core/` and `broker-app/`.

## Scenarios

| Test | What it covers |
|---|---|
| `MultiBrokerStartupIT` | 3-node cluster elects a single leader within 15s, every broker's registry converges to know all 3 peers. |
| `MultiBrokerReplicationIT` | Three brokers replicate identically by byte under steady produce. |
| `MultiBrokerFailoverIT` | Kill the partition leader → new leader elected + produces succeed to new leader within the CI budget (30s). |
| `MultiBrokerHeartbeatIT` | Point-to-point heartbeats converge across all 3 brokers. |
| `OffsetCommitFetchEndToEndIT` | Consumer-group commit + fetch round-trip; commit survives broker restart (E2E-7-6). |
| `GroupCoordinatorFailoverIT` | Coordinator broker killed → group re-coordinates on a survivor. |
| `BalancerRebalanceIT` | After a failover, preferred-leader balancer moves leadership back to `replicas[0]` within the compressed 2-3s window. |
| `ForceCompactAcrossClusterIT` | `Admin.ForceCompactPartition` fans to every replica and returns `records_kept` summed. |
| `HighVolumeConcurrentClientsIT` | 10k concurrent clients (CI variant); heavier @Tag("slow") version runs 1M records. |
| `ChaosKillBrokerIT` | Kills random brokers via the chaos HTTP endpoint; cluster survives. |
| `E2E_Audit08_PerfGateIT` | Perf regression floor on a 3-node cluster — produces + consumes with rps assertions. |

## Running

```bash
# Normal ITs (fast — ~60 tests in 2 min)
./gradlew :integration-tests:test

# Stress — 100 randomized election cycles against a real 3-node cluster
./gradlew :integration-tests:stressTest

# Include @Tag("slow") heavy tests (1M records, Testcontainers-Redis, 10k concurrent clients)
JBROKER_RUN_SLOW_TESTS=1 ./gradlew :integration-tests:test
```

## CI behaviour

GitHub Actions runs the standard IT set + both perf-gate jobs on every PR. Slow tests are opt-in; nightly or manual. The `JBROKER_CI=1` env var makes deadline-based assertions use CI budgets (2-3× longer) automatically.

## Flake policy

Per the project's `feedback_no_flaky_tests` rule: every CI failure gets a root-cause fix, not a rerun. Common flake root causes fixed in this tree:

- **Port-0-then-bind TOCTOU** — `freePort()` returns a port just before something else grabs it. Fixes: retry bind on failure, or use `ServerSocket(0)` and read back the actual port instead of re-binding.
- **Single-trial perf tests** on shared CI disks — replaced with best-of-N (see `AppendThroughputTest`).
- **Insufficient convergence budgets** under noisy-neighbour load — documented worst-case latency per scenario and widened the budget accordingly (see `MultiBrokerFailoverIT`'s 20s→30s bump).
