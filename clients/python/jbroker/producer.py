"""Synchronous single-endpoint producer.

Scope mirrors the Java ``BrokerClient`` produce surface: one batch per RPC,
acks=1 (leader ack) or acks=-1 (every ISR member), legacy producer sentinels
(no idempotence). One piece of retriable-code awareness is built in: a
NOT_LEADER answer that carries ``suggested_leader_host``/``_port`` hints is
retried once against the hinted broker, which then becomes the produce
target. A NOT_LEADER without hints (or a second failure) raises BrokerError
with the hint map attached so the caller can redial.
"""

from __future__ import annotations

import time

import grpc

from jbroker import batch as batchcodec
from jbroker import errors
from jbroker._stubs import broker_pb2, broker_pb2_grpc

_ACKS_ALL_DEADLINE_S = 10.0
_DEFAULT_DEADLINE_S = 7.0


class Producer:
    """Produce record batches to one broker over an insecure channel."""

    def __init__(self, host: str = "127.0.0.1", port: int = 9092):
        self._channel = grpc.insecure_channel(f"{host}:{port}")
        self._stub = broker_pb2_grpc.ProducerStub(self._channel)

    def produce(
        self,
        topic: str,
        partition: int,
        values: bytes | list[bytes],
        *,
        acks: int = 1,
    ) -> tuple[int, int]:
        """Produce one batch; return ``(base_offset, last_offset)``.

        ``values`` is a single record value or a list of values sent as one
        batch (offset deltas 0..n-1). ``acks=1`` returns after the leader
        append; ``acks=-1`` blocks until every ISR member has the batch.
        """
        if isinstance(values, bytes | bytearray):
            values = [bytes(values)]
        if not values:
            raise ValueError("values must be non-empty")
        now_ms = int(time.time() * 1000)
        records = [batchcodec.Record(offset_delta=i, value=v) for i, v in enumerate(values)]
        encoded = batchcodec.encode(
            records,
            first_timestamp=now_ms,
            max_timestamp=now_ms,
        )
        request = broker_pb2.ProduceRequest(
            topic=topic,
            partition=partition,
            batch=encoded,
            # Legacy sentinels: proto3 int64 defaults to 0, which would trip
            # the broker's idempotent-producer dedup.
            producer_id=-1,
            base_sequence=-1,
            acks=acks,
        )
        deadline = _ACKS_ALL_DEADLINE_S if acks == -1 else _DEFAULT_DEADLINE_S
        response = self._stub.Produce(request, timeout=deadline)
        if response.HasField("error") and response.error.code != errors.NONE:
            err = errors.BrokerError.from_proto(response.error)
            if err.code == errors.NOT_LEADER and err.suggested_leader() is not None:
                return self._retry_at_hinted_leader(request, deadline, err)
            raise err
        return response.base_offset, response.last_offset

    def _retry_at_hinted_leader(
        self,
        request,
        deadline: float,
        original: errors.BrokerError,
    ) -> tuple[int, int]:
        """One bounded retry against the leader named in the hint map.

        On success the hinted broker becomes this producer's channel; on any
        failure the original NOT_LEADER (or the retry's own error) raises.
        """
        host, port = original.suggested_leader()
        channel = grpc.insecure_channel(f"{host}:{port}")
        stub = broker_pb2_grpc.ProducerStub(channel)
        try:
            response = stub.Produce(request, timeout=deadline)
        except grpc.RpcError:
            channel.close()
            raise original from None
        if response.HasField("error") and response.error.code != errors.NONE:
            channel.close()
            raise errors.BrokerError.from_proto(response.error)
        self._channel.close()
        self._channel = channel
        self._stub = stub
        return response.base_offset, response.last_offset

    def close(self) -> None:
        self._channel.close()

    def __enter__(self) -> Producer:
        return self

    def __exit__(self, *exc) -> None:
        self.close()
