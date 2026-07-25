"""Minimal admin surface: topic creation.

The full admin surface (ACLs, membership, reassignment, ...) belongs to the
Java client and the admin app; this client only needs enough to set up a
topic to produce into. On NOT_LEADER the raised BrokerError carries the
``suggested_leader_*`` hint map so the caller can redial the controller.
"""

from __future__ import annotations

import grpc

from jbroker import errors
from jbroker._stubs import broker_pb2, broker_pb2_grpc

_RPC_DEADLINE_S = 10.0


class Admin:
    """Admin RPCs against one broker over an insecure channel."""

    def __init__(self, host: str = "127.0.0.1", port: int = 9092):
        self._channel = grpc.insecure_channel(f"{host}:{port}")
        self._stub = broker_pb2_grpc.AdminStub(self._channel)

    def create_topic(
        self,
        topic: str,
        partitions: int = 1,
        replication_factor: int = 1,
        config: dict[str, str] | None = None,
    ) -> None:
        request = broker_pb2.CreateTopicRequest(
            topic=topic,
            partitions=partitions,
            replication_factor=replication_factor,
            config=config or {},
        )
        response = self._stub.CreateTopic(request, timeout=_RPC_DEADLINE_S)
        errors.raise_if_error(response)

    def close(self) -> None:
        self._channel.close()

    def __enter__(self) -> Admin:
        return self

    def __exit__(self, *exc) -> None:
        self.close()
