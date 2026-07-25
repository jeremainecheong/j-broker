"""Single import point for the generated gRPC stubs.

The stubs are generated from ``proto/src/main/proto`` into
``jbroker/generated/`` (not checked in — the repo gitignores
``**/generated/``). Run ``clients/python/generate.sh`` once after checkout,
and again whenever the protos change.
"""

from __future__ import annotations

try:
    from jbroker.generated import broker_pb2, broker_pb2_grpc, common_pb2
except ImportError as e:  # pragma: no cover - setup error, not runtime logic
    raise ImportError(
        "jbroker.generated is missing — the gRPC stubs have not been generated. "
        "Run clients/python/generate.sh (requires grpcio-tools, installed by "
        "`pip install -e '.[dev]'`)."
    ) from e

__all__ = ["broker_pb2", "broker_pb2_grpc", "common_pb2"]
