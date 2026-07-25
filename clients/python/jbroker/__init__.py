"""Minimal Python reference client for the j-broker gRPC wire protocol.

Proves the wire protocol is language-neutral: batch encode/decode plus
produce / group-consume / create-topic against a real broker. The Java
client (jbroker.broker.client) is the full-featured one — cluster routing,
failover, idempotence, TLS, compression.
"""

from jbroker.batch import (
    BatchDecodeError,
    CrcMismatchError,
    ParsedBatch,
    Record,
    UnsupportedCodecError,
    decode,
    decode_all,
    encode,
)
from jbroker.errors import BrokerError

__all__ = [
    "Admin",
    "BatchDecodeError",
    "BrokerError",
    "Consumer",
    "ConsumerRecord",
    "CrcMismatchError",
    "ParsedBatch",
    "Producer",
    "Record",
    "UnsupportedCodecError",
    "decode",
    "decode_all",
    "encode",
]


def __getattr__(name: str):
    # Lazy: the gRPC classes need the generated stubs, but the batch codec
    # (and its unit tests) must import without them.
    if name == "Producer":
        from jbroker.producer import Producer

        return Producer
    if name in ("Consumer", "ConsumerRecord"):
        from jbroker import consumer

        return getattr(consumer, name)
    if name == "Admin":
        from jbroker.admin import Admin

        return Admin
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
