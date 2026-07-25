"""Producer NOT_LEADER hint-following: one bounded retry, hints surfaced.

Uses two in-process gRPC servers: a "follower" that answers NOT_LEADER
with suggested_leader_* hints pointing at a "leader" that accepts the
produce. No broker binary needed.
"""

from concurrent import futures

import grpc
import pytest

from jbroker._stubs import broker_pb2, broker_pb2_grpc
from jbroker.errors import NOT_LEADER, BrokerError
from jbroker.producer import Producer


class _LeaderProducer(broker_pb2_grpc.ProducerServicer):
    def __init__(self):
        self.requests = []

    def Produce(self, request, context):
        self.requests.append(request)
        return broker_pb2.ProduceResponse(base_offset=7, last_offset=7)


class _FollowerProducer(broker_pb2_grpc.ProducerServicer):
    def __init__(self, hints):
        self.hints = hints
        self.requests = []

    def Produce(self, request, context):
        self.requests.append(request)
        return broker_pb2.ProduceResponse(
            error=broker_pb2.Error(code=NOT_LEADER, message="leader is elsewhere", hint=self.hints)
        )


def _serve(servicer):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    broker_pb2_grpc.add_ProducerServicer_to_server(servicer, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    return server, port


def test_not_leader_with_hints_redials_once_and_succeeds():
    leader = _LeaderProducer()
    leader_server, leader_port = _serve(leader)
    follower = _FollowerProducer(
        {
            "suggested_leader_id": "2",
            "suggested_leader_host": "127.0.0.1",
            "suggested_leader_port": str(leader_port),
        }
    )
    follower_server, follower_port = _serve(follower)
    try:
        with Producer("127.0.0.1", follower_port) as producer:
            base, last = producer.produce("t", 0, b"v")
            assert (base, last) == (7, 7)
            assert len(follower.requests) == 1
            assert len(leader.requests) == 1

            # The hinted leader became the produce target: no more
            # follower round-trips.
            producer.produce("t", 0, b"v2")
            assert len(follower.requests) == 1
            assert len(leader.requests) == 2
    finally:
        follower_server.stop(0)
        leader_server.stop(0)


def test_not_leader_without_hints_raises_with_code():
    follower = _FollowerProducer({})
    server, port = _serve(follower)
    try:
        with Producer("127.0.0.1", port) as producer:
            with pytest.raises(BrokerError) as excinfo:
                producer.produce("t", 0, b"v")
        assert excinfo.value.code == NOT_LEADER
        assert excinfo.value.retriable
        assert excinfo.value.suggested_leader() is None
        assert len(follower.requests) == 1  # no blind retry
    finally:
        server.stop(0)


def test_hinted_retry_failure_raises_the_retry_error():
    # Hint points at another NOT_LEADER answerer — the retry is bounded to
    # one hop, and the second error is what surfaces.
    second = _FollowerProducer({})
    second_server, second_port = _serve(second)
    first = _FollowerProducer(
        {
            "suggested_leader_host": "127.0.0.1",
            "suggested_leader_port": str(second_port),
        }
    )
    first_server, first_port = _serve(first)
    try:
        with Producer("127.0.0.1", first_port) as producer:
            with pytest.raises(BrokerError) as excinfo:
                producer.produce("t", 0, b"v")
        assert excinfo.value.code == NOT_LEADER
        assert len(first.requests) == 1
        assert len(second.requests) == 1
    finally:
        first_server.stop(0)
        second_server.stop(0)
