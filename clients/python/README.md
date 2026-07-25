# jbroker-client (Python)

Minimal Python reference client for the j-broker gRPC wire protocol. It
exists to prove the protocol is language-neutral: the same protos, the same
v2 record-batch bytes, spoken from a second language. The Java client
(`jbroker.broker.client`) is the full-featured one — cluster routing,
failover, idempotent produce, TLS, compression. This client deliberately
stays small:

- `Producer` — synchronous produce, acks=1 or acks=-1, one batch per RPC.
  NOT_LEADER answers that carry `suggested_leader_host`/`_port` hints get
  one bounded retry against the hinted broker; everything else raises
  `BrokerError` with the numeric code and the hint map attached.
- `Consumer` — one group member: heartbeat, assignment, fetch, commit,
  clean leave. Single endpoint, no failover routing.
- `Admin` — `create_topic`.
- `jbroker.batch` — the Kafka-v2 record-batch codec (encode + decode,
  CRC-32C, zigzag varints). Codec 0 (uncompressed) only; compressed
  batches are rejected on decode with a clear error.

## Install

```sh
cd clients/python
python3 -m venv .venv && source .venv/bin/activate
pip install -e '.[dev]'
./generate.sh   # gRPC stubs from ../../proto/src/main/proto
```

`generate.sh` writes the stubs to `jbroker/generated/` (gitignored — the
repo ignores `**/generated/`). Re-run it whenever the protos change. It
needs `grpcio-tools`, which the `dev` extra installs.

## Produce and consume

```python
from jbroker import Admin, Consumer, Producer

with Admin("127.0.0.1", 9092) as admin:
    admin.create_topic("events", partitions=1, replication_factor=1)

with Producer("127.0.0.1", 9092) as producer:
    base, last = producer.produce("events", 0, [b"one", b"two", b"three"])
    print(f"appended offsets {base}..{last}")

with Consumer("127.0.0.1", 9092, group_id="workers", topics=["events"]) as consumer:
    records = []
    while len(records) < 3:  # poll() returns [] while the group forms
        records.extend(consumer.poll())
    for record in records:
        print(record.offset, record.value)
    consumer.commit()  # next member of "workers" resumes here
```

## Tests

```sh
pytest                              # unit tests always run
./gradlew :broker-app:installDist   # from the repo root, for the E2E tests
pytest tests/test_e2e.py -v
```

The E2E tests boot a real broker from the installDist binary (skipped with
a message when it is absent), then drive produce → group consume → commit →
re-poll, plus a cross-language round trip: records produced by the Java CLI
are fetched and decoded by this client, and records produced by this client
are read back through the Java console consumer.

Formatting and linting use ruff, configured in `pyproject.toml`:

```sh
ruff check . && ruff format .
```
