"""Broker error envelope: numeric codes, names, and the hint map.

Two error shapes exist on the wire:

* ``Error`` messages (produce/fetch/admin) carry ``code`` (int32),
  ``message``, and a flat string ``hint`` map. On NOT_LEADER, controller-
  routed admin RPCs hint ``suggested_leader_id`` / ``suggested_leader_host``
  / ``suggested_leader_port`` so the caller can redial.
* ``ErrorCode`` enum fields (consumer-group RPCs) carry the numeric code
  only.

Code numbers mirror ``jbroker.broker.ErrorCodes`` and
``jbroker.common.ErrorCode``.
"""

from __future__ import annotations

# jbroker.broker.ErrorCodes (Error.code on produce/fetch/admin RPCs).
NONE = 0
UNKNOWN_TOPIC = 1
INVALID_PARTITION = 2
CORRUPT_BATCH = 3
TOPIC_ALREADY_EXISTS = 4
NOT_LEADER = 5
IO_ERROR = 6
FENCED_EPOCH = 7
OUT_OF_ORDER_SEQUENCE = 8
NOT_ENOUGH_REPLICAS = 9
INVALID_CONFIG = 10
MESSAGE_TOO_LARGE = 11
STORAGE_FULL = 12
UNAUTHORIZED = 13

# jbroker.common.ErrorCode (consumer-group RPCs; numerics shared).
COORDINATOR_NOT_AVAILABLE = 80
NOT_COORDINATOR = 81
UNKNOWN_MEMBER_ID = 82
FENCED_MEMBER_EPOCH = 83
OFFSET_OUT_OF_RANGE = 84
FETCH_SESSION_ID_NOT_FOUND = 85
QUOTA_VIOLATED = 86
REASSIGNMENT_IN_PROGRESS = 87
UNIMPLEMENTED = 99

CODE_NAMES = {
    NONE: "NONE",
    UNKNOWN_TOPIC: "UNKNOWN_TOPIC",
    INVALID_PARTITION: "INVALID_PARTITION",
    CORRUPT_BATCH: "CORRUPT_BATCH",
    TOPIC_ALREADY_EXISTS: "TOPIC_ALREADY_EXISTS",
    NOT_LEADER: "NOT_LEADER",
    IO_ERROR: "IO_ERROR",
    FENCED_EPOCH: "FENCED_EPOCH",
    OUT_OF_ORDER_SEQUENCE: "OUT_OF_ORDER_SEQUENCE",
    NOT_ENOUGH_REPLICAS: "NOT_ENOUGH_REPLICAS",
    INVALID_CONFIG: "INVALID_CONFIG",
    MESSAGE_TOO_LARGE: "MESSAGE_TOO_LARGE",
    STORAGE_FULL: "STORAGE_FULL",
    UNAUTHORIZED: "UNAUTHORIZED",
    COORDINATOR_NOT_AVAILABLE: "COORDINATOR_NOT_AVAILABLE",
    NOT_COORDINATOR: "NOT_COORDINATOR",
    UNKNOWN_MEMBER_ID: "UNKNOWN_MEMBER_ID",
    FENCED_MEMBER_EPOCH: "FENCED_MEMBER_EPOCH",
    OFFSET_OUT_OF_RANGE: "OFFSET_OUT_OF_RANGE",
    FETCH_SESSION_ID_NOT_FOUND: "FETCH_SESSION_ID_NOT_FOUND",
    QUOTA_VIOLATED: "QUOTA_VIOLATED",
    REASSIGNMENT_IN_PROGRESS: "REASSIGNMENT_IN_PROGRESS",
    UNIMPLEMENTED: "UNIMPLEMENTED",
}

# Codes where a retry (possibly after redialing per the hint map) can
# succeed without operator action.
RETRIABLE = frozenset(
    {
        NOT_LEADER,
        NOT_ENOUGH_REPLICAS,
        STORAGE_FULL,
        COORDINATOR_NOT_AVAILABLE,
        NOT_COORDINATOR,
        FETCH_SESSION_ID_NOT_FOUND,
        QUOTA_VIOLATED,
        REASSIGNMENT_IN_PROGRESS,
    }
)


class BrokerError(Exception):
    """A broker-reported error: numeric code, message, and hint map."""

    def __init__(self, code: int, message: str = "", hints: dict[str, str] | None = None):
        self.code = code
        self.code_name = CODE_NAMES.get(code, f"UNKNOWN({code})")
        self.hints = dict(hints or {})
        detail = f"{self.code_name} (code {code})"
        if message:
            detail += f": {message}"
        if self.hints:
            detail += f" hints={self.hints}"
        super().__init__(detail)

    @property
    def retriable(self) -> bool:
        return self.code in RETRIABLE

    def suggested_leader(self) -> tuple[str, int] | None:
        """(host, port) from the hint map, or None when not hinted."""
        host = self.hints.get("suggested_leader_host", "")
        port = self.hints.get("suggested_leader_port", "")
        if not host or not port.isdigit():
            return None
        return host, int(port)

    @classmethod
    def from_proto(cls, error) -> BrokerError:
        """Build from a broker.Error proto message."""
        return cls(error.code, error.message, dict(error.hint))


def raise_if_error(response) -> None:
    """Raise BrokerError when a response's Error envelope carries a code."""
    if response.HasField("error") and response.error.code != NONE:
        raise BrokerError.from_proto(response.error)
