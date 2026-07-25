"""Single-member group consumer, driven by repeated poll() calls.

The flow matches the Java client's poll loop: each poll sends one
ConsumerGroupHeartbeat (joining with an empty member_id on the first call),
applies any assignment the coordinator hands back (priming positions from
the group's committed offsets), then fetches each assigned partition from
its current position. commit() persists the positions via CommitOffsets;
close() leaves the group with member_epoch=-1.

Single endpoint: the bootstrap broker serves fetches, and coordinator-routed
RPCs go through the endpoint FindCoordinator names (cached; invalidated on
NOT_COORDINATOR). Multi-broker failover routing is out of scope — that is
the Java client's job.
"""

from __future__ import annotations

from dataclasses import dataclass

import grpc

from jbroker import batch as batchcodec
from jbroker import errors
from jbroker._stubs import broker_pb2, broker_pb2_grpc, common_pb2

_RPC_DEADLINE_S = 5.0


@dataclass(frozen=True)
class ConsumerRecord:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: tuple[tuple[bytes | None, bytes | None], ...] = ()


class Consumer:
    """One group member subscribed to a fixed set of topics."""

    def __init__(
        self,
        host: str,
        port: int,
        group_id: str,
        topics: list[str],
        *,
        instance_id: str = "",
        rebalance_timeout_ms: int = 30_000,
        fetch_max_bytes: int = 1 << 20,
    ):
        if not topics:
            raise ValueError("topics must be non-empty")
        self._bootstrap_addr = (host, port)
        self._group_id = group_id
        self._topics = list(topics)
        self._instance_id = instance_id
        self._rebalance_timeout_ms = rebalance_timeout_ms
        self._fetch_max_bytes = fetch_max_bytes

        self._channel = grpc.insecure_channel(f"{host}:{port}")
        self._stub = broker_pb2_grpc.ConsumerStub(self._channel)
        # Coordinator endpoint + stub, resolved lazily via FindCoordinator.
        self._coordinator_addr: tuple[str, int] | None = None
        self._coordinator_channel: grpc.Channel | None = None
        self._coordinator_stub: broker_pb2_grpc.ConsumerStub | None = None

        self._member_id = ""
        self._member_epoch = 0
        # [(topic, partition)] in assignment order; positions keyed the same.
        self._assignment: list[tuple[str, int]] = []
        self._positions: dict[tuple[str, int], int] = {}
        self._closed = False

    # ---- group membership ----

    @property
    def assignment(self) -> list[tuple[str, int]]:
        return list(self._assignment)

    def poll(self) -> list[ConsumerRecord]:
        """One heartbeat + fetch tick. Returns any new records.

        An empty list means "nothing this tick" — the group may still be
        rebalancing, or no records are available. Callers loop.
        """
        if self._closed:
            raise RuntimeError("consumer is closed")
        coordinator = self._ensure_coordinator()
        if coordinator is None:
            return []

        heartbeat = broker_pb2.ConsumerGroupHeartbeatRequest(
            group_id=self._group_id,
            member_id=self._member_id,
            member_epoch=self._member_epoch,
            instance_id=self._instance_id,
            rebalance_timeout_ms=self._rebalance_timeout_ms,
            subscribed_topics=self._topics,
            owned_partitions=_to_topic_partitions(self._assignment),
        )
        try:
            response = coordinator.ConsumerGroupHeartbeat(heartbeat, timeout=_RPC_DEADLINE_S)
        except grpc.RpcError:
            self._invalidate_coordinator()
            return []

        code = response.error
        if code in (errors.NOT_COORDINATOR, errors.COORDINATOR_NOT_AVAILABLE):
            self._invalidate_coordinator()
            return []
        if code in (errors.UNKNOWN_MEMBER_ID, errors.FENCED_MEMBER_EPOCH):
            # Fresh join on the next poll.
            self._member_id = ""
            self._member_epoch = 0
            self._assignment = []
            return []
        if code != errors.NONE:
            raise errors.BrokerError(code, "heartbeat failed")

        self._member_id = response.member_id
        self._member_epoch = response.member_epoch
        if response.HasField("assignment") and len(response.assignment.assigned_partitions) > 0:
            self._apply_assignment(_flatten(response.assignment))

        if not self._assignment:
            return []
        return self._fetch_assigned()

    def commit(self) -> None:
        """Commit current positions for every assigned partition."""
        if not self._assignment:
            return
        commits = [
            broker_pb2.OffsetCommit(
                tp=common_pb2.TopicPartition(topic=t, partition=p),
                offset=self._positions[(t, p)],
            )
            for t, p in self._assignment
            if (t, p) in self._positions
        ]
        if not commits:
            return
        coordinator = self._ensure_coordinator()
        if coordinator is None:
            raise RuntimeError("coordinator not available")
        response = coordinator.CommitOffsets(
            broker_pb2.CommitOffsetsRequest(
                group_id=self._group_id,
                member_id=self._member_id,
                generation_id_or_member_epoch=self._member_epoch,
                commits=commits,
            ),
            timeout=_RPC_DEADLINE_S,
        )
        for result in response.results:
            if result.error == errors.NOT_COORDINATOR:
                self._invalidate_coordinator()
                raise errors.BrokerError(result.error, "coordinator moved; retry commit")
            if result.error != errors.NONE:
                raise errors.BrokerError(
                    result.error,
                    f"commit for {result.tp.topic}-{result.tp.partition} failed",
                )

    def committed(self, topic: str, partition: int) -> int:
        """Last committed offset for (group, topic, partition); -1 if none."""
        coordinator = self._ensure_coordinator()
        if coordinator is None:
            raise RuntimeError("coordinator not available")
        response = coordinator.FetchOffsets(
            broker_pb2.FetchOffsetsRequest(
                group_id=self._group_id,
                tps=[common_pb2.TopicPartition(topic=topic, partition=partition)],
            ),
            timeout=_RPC_DEADLINE_S,
        )
        result = response.results[0]
        if result.error == errors.OFFSET_OUT_OF_RANGE:
            return -1
        if result.error != errors.NONE:
            raise errors.BrokerError(result.error, "FetchOffsets failed")
        return result.offset

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        # Best-effort leave; session-timeout eviction covers failures.
        if self._member_id and self._coordinator_stub is not None:
            try:
                self._coordinator_stub.ConsumerGroupHeartbeat(
                    broker_pb2.ConsumerGroupHeartbeatRequest(
                        group_id=self._group_id,
                        member_id=self._member_id,
                        member_epoch=-1,
                    ),
                    timeout=2.0,
                )
            except grpc.RpcError:
                pass
        if self._coordinator_channel is not None:
            self._coordinator_channel.close()
        self._channel.close()

    def __enter__(self) -> Consumer:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ---- internals ----

    def _ensure_coordinator(self):
        if self._coordinator_stub is not None:
            return self._coordinator_stub
        try:
            response = self._stub.FindCoordinator(
                broker_pb2.FindCoordinatorRequest(key=self._group_id),
                timeout=_RPC_DEADLINE_S,
            )
        except grpc.RpcError:
            return None
        if response.error != errors.NONE:
            return None
        addr = (response.coordinator.host, response.coordinator.port)
        self._coordinator_addr = addr
        if addr == self._bootstrap_addr:
            self._coordinator_channel = None
            self._coordinator_stub = self._stub
        else:
            self._coordinator_channel = grpc.insecure_channel(f"{addr[0]}:{addr[1]}")
            self._coordinator_stub = broker_pb2_grpc.ConsumerStub(self._coordinator_channel)
        return self._coordinator_stub

    def _invalidate_coordinator(self) -> None:
        if self._coordinator_channel is not None:
            self._coordinator_channel.close()
        self._coordinator_addr = None
        self._coordinator_channel = None
        self._coordinator_stub = None

    def _apply_assignment(self, new_assignment: list[tuple[str, int]]) -> None:
        old = set(self._assignment)
        new = set(new_assignment)
        for tp in old - new:
            self._positions.pop(tp, None)
        self._assignment = list(new_assignment)
        for tp in new_assignment:
            if tp in old:
                continue
            # Prime from the group's committed offset (or 0), but never
            # rewind a live local position — committed-ahead still wins.
            committed = self._committed_quiet(tp)
            floor = 0 if committed < 0 else committed
            self._positions[tp] = max(self._positions.get(tp, 0), floor)

    def _committed_quiet(self, tp: tuple[str, int]) -> int:
        try:
            return self.committed(tp[0], tp[1])
        except Exception:
            return -1

    def _fetch_assigned(self) -> list[ConsumerRecord]:
        out: list[ConsumerRecord] = []
        for topic, partition in self._assignment:
            position = self._positions.get((topic, partition), 0)
            try:
                response = self._stub.Fetch(
                    broker_pb2.FetchRequest(
                        topic=topic,
                        partition=partition,
                        offset=position,
                        max_bytes=self._fetch_max_bytes,
                    ),
                    timeout=_RPC_DEADLINE_S,
                )
            except grpc.RpcError:
                continue  # partition unreachable this tick — try next poll
            if response.HasField("error") and response.error.code != errors.NONE:
                continue  # skip this partition this tick
            for parsed in batchcodec.decode_all(response.records):
                for rec in parsed.records:
                    absolute = parsed.base_offset + rec.offset_delta
                    if absolute < position:
                        continue  # pre-fetch-offset records inside the batch
                    out.append(
                        ConsumerRecord(
                            topic=topic,
                            partition=partition,
                            offset=absolute,
                            key=rec.key,
                            value=rec.value,
                            headers=rec.headers,
                        )
                    )
                    self._positions[(topic, partition)] = absolute + 1
        return out


def _to_topic_partitions(assignment: list[tuple[str, int]]):
    by_topic: dict[str, list[int]] = {}
    for topic, partition in assignment:
        by_topic.setdefault(topic, []).append(partition)
    return [broker_pb2.TopicPartitions(topic=t, partitions=ps) for t, ps in by_topic.items()]


def _flatten(assignment) -> list[tuple[str, int]]:
    out: list[tuple[str, int]] = []
    for tps in assignment.assigned_partitions:
        for partition in tps.partitions:
            out.append((tps.topic, partition))
    return out
