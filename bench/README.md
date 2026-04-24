# bench

HdrHistogram-backed perf harness with CSV append. `PerfMain` is the CLI entrypoint; `ProducerPerfTest` and `ConsumerPerfTest` are the two scenarios.

## Usage

```bash
./bench/build/install/bench/bin/bench producer \
  --broker localhost:9092 --topic bench --partition 0 \
  --records 5000 --payload-size 1024 \
  --csv docs/bench/results.csv

./bench/build/install/bench/bin/bench consumer \
  --broker localhost:9092 --topic bench --partition 0 \
  --records 5000 --csv docs/bench/results.csv
```

Each run prints a percentile table (p50/p99/p999/max) + records/s + bytes/s, and appends a row to the CSV for regression tracking.

Regenerate the README snapshot: `scripts/bench/run-readme-bench.sh`.

## Current snapshot

From `scripts/bench/run-readme-bench.sh` on Apple-silicon laptop (Darwin 24.2, M-series, SSD). End-to-end gRPC path; producer bench is single-record-per-RPC with `acks=1`, consumer bench uses `Consumer.Fetch`. Multi-broker `acks=all` numbers will differ.

### Producer

| Payload | Records | rps | MiB/s | p50 | p99 | p999 |
|---|---|---|---|---|---|---|
| 256B | 5,000 | 5,362 | 1.31 | 0.14ms | 0.60ms | 1.21ms |
| 1024B | 5,000 | 5,597 | 5.47 | 0.13ms | 0.56ms | 1.18ms |
| 4096B | 5,000 | 5,494 | 21.46 | 0.13ms | 0.58ms | 2.89ms |

### Consumer

| Payload | Records | rps | MiB/s | p50 | p99 | p999 |
|---|---|---|---|---|---|---|
| 256B | 5,100 | 37,057 | 9.05 | 5.40ms | 131.92ms | 131.92ms |
| 1024B | 5,100 | 34,922 | 34.10 | 4.34ms | 124.72ms | 124.72ms |
| 4096B | 5,020 | 25,189 | 98.40 | 3.37ms | 130.55ms | 130.55ms |

Raw CSV in `docs/bench/results.csv`.

## Batched produce (P13 perf work)

`BrokerClient.produceBatch(topic, partition, List<Record>)` issues ONE RPC with N records instead of N RPCs. ~150× throughput improvement on a laptop (PR #99). Use this when you care about throughput more than exact per-record commit visibility.

## CI perf-gate

`.github/workflows/ci.yml` runs two perf-gate jobs on every PR with generous regression floors (~3-5× over observed dev numbers):

- `E2E_Audit08_PerfGateIT` — end-to-end produce+consume with min-rps assertions.
- `AppendThroughputTest` — log-append MB/s with a 50 MB/s floor, best-of-3 trials (PR #107 deflake).

These catch catastrophic regressions (VT pinning, missing batching, serialisation blowup) without flaking on GitHub Actions shared-disk noise.
