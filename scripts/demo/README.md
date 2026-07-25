# Full demo

One command brings up the 3-broker cluster, runs plain and transactional
workloads against it, kills a partition leader mid-flow, and finishes with an
exactly-once audit. The cluster stays up afterwards so the admin UI and
dashboards can be explored.

```bash
scripts/demo/full-demo.sh
```

Add Prometheus + Grafana:

```bash
DEMO_MONITORING=1 scripts/demo/full-demo.sh
```

## Prerequisites

- Docker with the compose plugin (the daemon must be running)
- Java 21 (the script builds the CLI with `./gradlew :broker-app:installDist`)
- `curl` and `python3` (used to talk to the admin REST API)

The first run builds the Docker images and downloads Gradle dependencies —
expect a few minutes. Later runs reuse the caches and start in seconds.

## What happens, act by act

**Setup.** Builds the CLI, starts the cluster (`docker compose up -d --build`),
waits for the admin UI and all three brokers, then resets demo state from any
previous run: topics `demo-events`, `demo-source`, `demo-sink` are deleted and
recreated, and the demo consumer groups are deleted. The demo is rerunnable.

**Act 1 — steady flow.** A producer feeds `demo-events` (3 partitions, rf=3)
with idempotent `acks=all` batches, and consumer group `demo-consumers` reads
it back, committing offsets as it goes. Both run in the background for the
rest of the demo and log throughput lines once a second.

**Act 2 — transactional pipeline.** Records are fed into `demo-source`, and a
consume-transform-produce pipeline moves them to `demo-sink`: a
`read_committed` consumer polls the source, each batch is rewritten to the
sink through a `TransactionalProducer`, and the source offsets are committed
inside the same transaction. The loop body contains no error handling —
aborts and retries belong to the transactional client contract.

**Act 3 — kill a broker.** The script asks the admin API which broker leads
`demo-sink-0` (the partition the transactions are writing to) and runs
`docker kill` on that container — SIGKILL, no graceful handoff. The surviving
two brokers elect a new leader, `acks=all` keeps committing on the remaining
in-sync replicas, and both workloads resume without any client-side
intervention. The script then restarts the broker and waits for it to catch
up and rejoin the partition's ISR. Depending on the run, the victim may also
be the Raft leader or the transaction coordinator — the pipeline survives
either way.

The Act 1 workload makes the contrast concrete: the plain consumer group is
at-least-once, so after the failover its total can exceed the produced count
(re-reads past its last commit are counted twice), while the transactional
pipeline is audited to exactly-once in Act 4.

**Act 4 — the audit.** After the pipeline drains all records, `demo verify`
re-reads both topics end to end and checks the exactly-once contract:

- the sink's records under `read_committed` equal the transformed source
  records exactly once, **in order** (list equality — duplicates, losses, and
  aborted-transaction leakage would all fail it), and
- the pipeline group's committed offset on the source equals the source
  record count (offsets moved atomically with the data).

The script exits non-zero if the audit fails.

## Expected output (excerpts)

```text
================================================================================
Act 3: kill the leader of the transactional sink partition mid-flow
================================================================================
  demo-sink-0 is led by broker 3 (container jbroker-broker3).
  Broker 3 is also the current Raft leader, so a metadata election runs too.
  Progress before the kill: pipeline 27/1000 moved, demo-consumers 985 consumed.
  Running: docker kill jbroker-broker3
  Broker 3 is down (SIGKILL, no graceful handoff). Waiting for the pipeline to keep
  committing on the two survivors (leader failover + acks=all on the remaining ISR)...
  Cluster view during the outage:
    broker 1  localhost:9092  alive
    broker 2  localhost:9093  alive
    broker 3  localhost:9094  DOWN
  Progress now: pipeline 218/1000 moved, demo-consumers 5368 consumed.
  Both workloads kept moving without broker 3.
  Running: docker start jbroker-broker3
  Broker 3 caught up from the new leader's log and rejoined demo-sink-0's ISR.

================================================================================
Act 4: the exactly-once audit
================================================================================
  Waiting for the pipeline to finish all 1000 records...
    pipeline: complete, 1000 records moved in 21 transactions
  Auditing: re-read demo-source and demo-sink (read_committed) end to end and compare.
    verify: source demo-source-0 records: 1000
    verify: sink demo-sink-0 records under read_committed: 1000
    verify: sink equals transformed source exactly once, in order: OK
    verify: group demo-pipeline committed offset on demo-source-0: 1000, equals source count: OK
    verify: PASS, exactly-once contract holds
```

Exact counts and broker ids vary run to run; the "also the current Raft
leader" line appears only when the victim happens to hold that role too.

## What to explore afterwards

The cluster is still running when the script ends.

| Page | URL | What it shows |
|---|---|---|
| Overview | <http://localhost:15672/> | Cluster topology with the killed-and-rejoined broker, controller badge, live events rail (the failover and rejoin are in it) |
| Topics | <http://localhost:15672/topics> | `demo-events`, `demo-source`, `demo-sink`; drill into a topic for per-partition leader, ISR, and high watermark |
| Groups | <http://localhost:15672/groups> | `demo-pipeline` (offsets committed transactionally, lag 0) and `demo-consumers`; drill in for per-partition lag |
| Raft | <http://localhost:15672/raft> | Term and commit index after the failover, per-broker roles |
| Metrics | <http://localhost:15672/metrics> | Produce/fetch throughput sparklines covering the demo |

With `DEMO_MONITORING=1`:

| Component | URL | Notes |
|---|---|---|
| Grafana | <http://localhost:3000> | Anonymous admin access; two auto-provisioned dashboards |
| Prometheus | <http://localhost:9091> | Scrapes the admin-app's merged `/actuator/prometheus` |

The demo mounts its own Prometheus scrape config
(`scripts/demo/prometheus-demo.yml`) that targets the compose admin-app on
host port 15672; the stock `scripts/monitoring/prometheus.yml` targets an
admin-app running directly on the host.

Client logs (producer, consumer, pipeline) are written to `/tmp/jbroker-demo`
(override with `DEMO_LOG_DIR`).

## Knobs

| Variable | Default | Meaning |
|---|---|---|
| `DEMO_MONITORING` | `0` | `1` adds Prometheus + Grafana via the monitoring profile |
| `DEMO_EVENTS_COUNT` / `DEMO_EVENTS_RATE` | `60000` / `200` | Act 1 workload size and records/second |
| `DEMO_PIPELINE_COUNT` / `DEMO_PIPELINE_RATE` | `1000` / `40` | Act 2 workload size and records/second |
| `DEMO_LOG_DIR` | `/tmp/jbroker-demo` | Demo client log directory |

## Teardown

```bash
docker compose down -v
```

removes the containers, the network, and the broker data volumes. Without
`-v` the data survives for the next `docker compose up`.

## Under the hood

The workloads are driven by `j-broker demo` (see
`broker-app/src/main/java/jbroker/app/DemoCli.java`), four small drivers over
the cluster-aware client — `feed` (paced idempotent producer), `drain`
(consumer group with a running total), `pipeline` (the transactional
consume-transform-produce loop), and `verify` (the exactly-once audit). All
of them bootstrap against all three brokers, so they keep working while any
single broker is down. `seed-for-readme-screenshots.sh` in this directory is
a separate, older helper for populating the UI before taking screenshots.
