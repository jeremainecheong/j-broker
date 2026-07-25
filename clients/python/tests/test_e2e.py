"""End-to-end tests against a real broker booted from the installDist binary.

Skipped cleanly (via the ``broker`` fixture) when the binary is absent.
Covers the produce -> group-consume -> commit -> re-poll flow, plus the
cross-language batch proof: bytes encoded by the Java client are decoded by
this client and vice versa, through the broker's real log.
"""

from __future__ import annotations

import subprocess
import threading
import time

import grpc
import pytest

from jbroker import batch as batchcodec
from jbroker._stubs import broker_pb2, broker_pb2_grpc
from jbroker.admin import Admin
from jbroker.consumer import Consumer
from jbroker.errors import BrokerError
from jbroker.producer import Producer

_CONSUME_DEADLINE_S = 90.0
_CLI_DEADLINE_S = 60.0


def _poll_until(consumer: Consumer, count: int, deadline_s: float = _CONSUME_DEADLINE_S):
    """Poll until ``count`` records arrived; fail on deadline."""
    collected = []
    deadline = time.monotonic() + deadline_s
    while len(collected) < count:
        if time.monotonic() > deadline:
            pytest.fail(f"consumed only {len(collected)}/{count} records within {deadline_s}s")
        records = consumer.poll()
        if records:
            collected.extend(records)
        else:
            time.sleep(0.2)
    return collected


def test_produce_consume_commit_repoll(broker):
    topic = "py-e2e"
    group = "py-e2e-group"
    with Admin(broker.host, broker.port) as admin:
        admin.create_topic(topic, partitions=1, replication_factor=1)

    expected = [f"record-{i:03d}".encode() for i in range(50)]
    with Producer(broker.host, broker.port) as producer:
        next_offset = 0
        for i in range(0, 50, 10):
            # Alternate acks=1 / acks=-1 so both durability paths are hit.
            acks = 1 if (i // 10) % 2 == 0 else -1
            base, last = producer.produce(topic, 0, expected[i : i + 10], acks=acks)
            assert base == next_offset
            assert last == next_offset + 9
            next_offset = last + 1
    assert next_offset == 50

    # Consume everything in a group, then commit.
    with Consumer(broker.host, broker.port, group, [topic]) as consumer:
        records = _poll_until(consumer, 50)
        assert len(records) == 50
        assert [r.offset for r in records] == list(range(50))
        assert [r.value for r in records] == expected
        assert records[0].topic == topic
        assert records[0].partition == 0
        consumer.commit()
        assert consumer.committed(topic, 0) == 50

    # Five more records land after the first member left.
    tail = [f"tail-{i}".encode() for i in range(5)]
    with Producer(broker.host, broker.port) as producer:
        base, last = producer.produce(topic, 0, tail)
        assert (base, last) == (50, 54)

    # A fresh member in the same group resumes at the committed position:
    # exactly the 5 new records, never a replay of the first 50.
    with Consumer(broker.host, broker.port, group, [topic]) as consumer:
        records = _poll_until(consumer, 5)
        assert [r.offset for r in records] == [50, 51, 52, 53, 54]
        assert [r.value for r in records] == tail
        consumer.commit()
        assert consumer.committed(topic, 0) == 55


def test_create_topic_twice_reports_broker_error(broker):
    with Admin(broker.host, broker.port) as admin:
        admin.create_topic("py-dup", partitions=1, replication_factor=1)
        with pytest.raises(BrokerError) as excinfo:
            admin.create_topic("py-dup", partitions=1, replication_factor=1)
    assert excinfo.value.code_name == "TOPIC_ALREADY_EXISTS"


def _raw_fetch(broker, topic: str, offset: int = 0, max_bytes: int = 1 << 20):
    """Fetch raw batch bytes over the wire; decode with the Python codec."""
    with grpc.insecure_channel(broker.addr) as channel:
        stub = broker_pb2_grpc.ConsumerStub(channel)
        response = stub.Fetch(
            broker_pb2.FetchRequest(topic=topic, partition=0, offset=offset, max_bytes=max_bytes),
            timeout=5.0,
        )
    assert not (response.HasField("error") and response.error.code != 0), response.error
    return batchcodec.decode_all(response.records)


def test_cross_language_java_produce_python_fetch(broker):
    """Batches appended via the Java client are byte-exact for this codec.

    The CLI's ``produce`` subcommand drives the Java RecordBatch encoder;
    the fetch response carries the broker's stored bytes, which the Python
    decoder must parse (CRC included) without loss.
    """
    topic = "xlang-java-to-py"
    with Admin(broker.host, broker.port) as admin:
        admin.create_topic(topic, partitions=1, replication_factor=1)

    lines = [f"java-{i}" for i in range(10)]
    result = subprocess.run(
        [str(broker.binary), "produce", "--broker", broker.addr, "--topic", topic, "--partition", "0"],
        input="\n".join(lines) + "\n",
        capture_output=True,
        text=True,
        timeout=_CLI_DEADLINE_S,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.count("produced offset") == 10, result.stdout

    batches = _raw_fetch(broker, topic)
    values = [rec.value for parsed in batches for rec in parsed.records]
    assert values == [line.encode() for line in lines]
    offsets = [parsed.base_offset + rec.offset_delta for parsed in batches for rec in parsed.records]
    assert offsets == list(range(10))


def test_cross_language_python_produce_java_consume(broker):
    """Batches encoded by this codec parse on the Java side.

    The broker itself CRC-checks and decodes every produced batch (a broken
    encoding would be rejected as CORRUPT_BATCH); the CLI consumer then
    proves the stored records round-trip through the Java decode path.
    """
    topic = "xlang-py-to-java"
    with Admin(broker.host, broker.port) as admin:
        admin.create_topic(topic, partitions=1, replication_factor=1)

    values = [f"py-{i}".encode() for i in range(10)]
    with Producer(broker.host, broker.port) as producer:
        base, last = producer.produce(topic, 0, values)
    assert (base, last) == (0, 9)

    process = subprocess.Popen(
        [
            str(broker.binary),
            "console-consumer",
            "--broker",
            broker.addr,
            "--topic",
            topic,
            "--partition",
            "0",
            "--from-beginning",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    lines: list[str] = []

    def pump():
        for line in process.stdout:
            lines.append(line.rstrip("\n"))

    threading.Thread(target=pump, daemon=True).start()
    try:
        deadline = time.monotonic() + _CLI_DEADLINE_S
        while len(lines) < 10:
            if time.monotonic() > deadline:
                pytest.fail(f"console-consumer printed {len(lines)}/10 lines: {lines}")
            time.sleep(0.2)
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
    assert lines[:10] == [v.decode() for v in values]
