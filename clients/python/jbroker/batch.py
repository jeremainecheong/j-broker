"""Kafka-v2 record batch codec, byte-compatible with jbroker.storage.RecordBatch.

Wire format (big-endian):

    [0..7]   baseOffset                      int64
    [8..11]  batchLength (bytes AFTER this)  int32
    [12..15] partitionLeaderEpoch            int32
    [16]     magic (= 2)                     int8
    [17..20] crc (CRC-32C of bytes [21..])   uint32
    [21..22] attributes (low 3 bits: codec)  int16
    [23..26] lastOffsetDelta                 int32
    [27..34] firstTimestamp                  int64
    [35..42] maxTimestamp                    int64
    [43..50] producerId                      int64
    [51..52] producerEpoch                   int16
    [53..56] baseSequence                    int32
    [57..60] numRecords                      int32
    [61..]   records

Record layout (zigzag varints, like the Java side):

    length varint, attributes int8, timestampDelta varlong, offsetDelta varint,
    keyLength varint (-1 = null) + key, valueLength varint (-1 = null) + value,
    numHeaders varint, then (headerKeyLen, headerKey, headerValueLen,
    headerValue) per header.

This client encodes codec 0 (uncompressed) only. Decoding a batch whose
attribute bits name any other codec raises UnsupportedCodecError after the
CRC check has proven the batch intact — a clear error, never garbage records.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field

MAGIC_V2 = 2
BATCH_OVERHEAD = 61  # bytes before records[]
CODEC_MASK = 0x07

# ---------------------------------------------------------------------------
# CRC-32C (Castagnoli), reflected polynomial 0x82F63B78 — matches
# java.util.zip.CRC32C. Pure Python keeps the client dependency-free; a
# reference client trades raw speed for clarity.
# ---------------------------------------------------------------------------


def _make_crc_table() -> list[int]:
    table = []
    for i in range(256):
        crc = i
        for _ in range(8):
            crc = (crc >> 1) ^ 0x82F63B78 if crc & 1 else crc >> 1
        table.append(crc)
    return table


_CRC_TABLE = _make_crc_table()


def crc32c(data: bytes) -> int:
    """CRC-32C of ``data`` as an unsigned 32-bit integer."""
    crc = 0xFFFFFFFF
    table = _CRC_TABLE
    for b in data:
        crc = table[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


# ---------------------------------------------------------------------------
# Zigzag varints (Kafka record framing).
# ---------------------------------------------------------------------------


def write_varint(value: int, out: bytearray) -> None:
    v = ((value << 1) ^ (value >> 31)) & 0xFFFFFFFF
    while v & ~0x7F:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    out.append(v)


def write_varlong(value: int, out: bytearray) -> None:
    v = ((value << 1) ^ (value >> 63)) & 0xFFFFFFFFFFFFFFFF
    while v & ~0x7F:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    out.append(v)


def read_varint(buf: bytes, pos: int) -> tuple[int, int]:
    """Return (value, next_pos). Raises BatchDecodeError on overrun."""
    raw = 0
    shift = 0
    while True:
        if pos >= len(buf):
            raise BatchDecodeError("varint runs past end of buffer")
        b = buf[pos]
        pos += 1
        raw |= (b & 0x7F) << shift
        if not b & 0x80:
            break
        shift += 7
        if shift > 28:
            raise BatchDecodeError("varint too long")
    return (raw >> 1) ^ -(raw & 1), pos


def read_varlong(buf: bytes, pos: int) -> tuple[int, int]:
    raw = 0
    shift = 0
    while True:
        if pos >= len(buf):
            raise BatchDecodeError("varlong runs past end of buffer")
        b = buf[pos]
        pos += 1
        raw |= (b & 0x7F) << shift
        if not b & 0x80:
            break
        shift += 7
        if shift > 63:
            raise BatchDecodeError("varlong too long")
    return (raw >> 1) ^ -(raw & 1), pos


# ---------------------------------------------------------------------------
# Records and batches.
# ---------------------------------------------------------------------------


class BatchDecodeError(ValueError):
    """Batch bytes are short, corrupt, or structurally invalid."""


class CrcMismatchError(BatchDecodeError):
    """Stored CRC does not match the computed CRC — the batch is corrupt."""


class UnsupportedCodecError(BatchDecodeError):
    """The batch is compressed with a codec this client does not implement."""


@dataclass(frozen=True)
class Record:
    """One record inside a batch. Headers are (key, value) byte pairs."""

    offset_delta: int
    timestamp_delta: int = 0
    key: bytes | None = None
    value: bytes | None = None
    headers: tuple[tuple[bytes | None, bytes | None], ...] = ()

    def __post_init__(self) -> None:
        if self.offset_delta < 0:
            raise ValueError(f"offset_delta must be non-negative: {self.offset_delta}")


@dataclass(frozen=True)
class ParsedBatch:
    """Decoded batch header plus its records."""

    base_offset: int
    batch_length: int
    partition_leader_epoch: int
    last_offset_delta: int
    first_timestamp: int
    max_timestamp: int
    producer_id: int
    producer_epoch: int
    base_sequence: int
    codec: int
    records: list[Record] = field(default_factory=list)

    @property
    def last_offset(self) -> int:
        return self.base_offset + self.last_offset_delta

    @property
    def total_bytes(self) -> int:
        return 12 + self.batch_length  # baseOffset(8) + batchLength(4) + rest


def _encode_record(rec: Record, out: bytearray) -> None:
    inner = bytearray()
    inner.append(0)  # record attributes
    write_varlong(rec.timestamp_delta, inner)
    write_varint(rec.offset_delta, inner)
    if rec.key is None:
        write_varint(-1, inner)
    else:
        write_varint(len(rec.key), inner)
        inner.extend(rec.key)
    if rec.value is None:
        write_varint(-1, inner)
    else:
        write_varint(len(rec.value), inner)
        inner.extend(rec.value)
    write_varint(len(rec.headers), inner)
    for hk, hv in rec.headers:
        if hk is None:
            write_varint(-1, inner)
        else:
            write_varint(len(hk), inner)
            inner.extend(hk)
        if hv is None:
            write_varint(-1, inner)
        else:
            write_varint(len(hv), inner)
            inner.extend(hv)
    write_varint(len(inner), out)
    out.extend(inner)


def encode(
    records: list[Record],
    *,
    base_offset: int = 0,
    partition_leader_epoch: int = 0,
    first_timestamp: int = 0,
    max_timestamp: int = 0,
    producer_id: int = -1,
    producer_epoch: int = -1,
    base_sequence: int = -1,
) -> bytes:
    """Encode an uncompressed (codec 0) v2 record batch.

    Defaults carry the legacy-producer sentinels (producer_id = -1,
    base_sequence = -1) so the broker skips idempotent-producer dedup.
    """
    if not records:
        raise ValueError("records must be non-empty")
    body = bytearray()
    for rec in records:
        _encode_record(rec, body)

    # CRC covers attributes..end-of-records ([21..]); build that span first.
    crc_span = bytearray(
        struct.pack(
            ">hiqqqhii",
            0,  # attributes: codec 0, no flags
            records[-1].offset_delta,
            first_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            len(records),
        )
    )
    crc_span.extend(body)
    crc = crc32c(bytes(crc_span))

    batch_length = 4 + 1 + 4 + len(crc_span)  # leaderEpoch + magic + crc + span
    head = struct.pack(
        ">qiibI",
        base_offset,
        batch_length,
        partition_leader_epoch,
        MAGIC_V2,
        crc,
    )
    return head + bytes(crc_span)


def decode(buf: bytes, pos: int = 0) -> tuple[ParsedBatch, int]:
    """Decode one batch starting at ``pos``; return (batch, next_pos).

    Raises BatchDecodeError (or a subclass) if the buffer is short, magic is
    wrong, the CRC fails, or the codec is unsupported.
    """
    if len(buf) - pos < BATCH_OVERHEAD:
        raise BatchDecodeError(f"short batch: {len(buf) - pos} < {BATCH_OVERHEAD}")
    base_offset, batch_length = struct.unpack_from(">qi", buf, pos)
    batch_end = pos + 12 + batch_length
    if batch_end > len(buf):
        raise BatchDecodeError(f"batch truncated: declared {batch_length} have {len(buf) - pos - 12}")
    partition_leader_epoch, magic, crc_read = struct.unpack_from(">ibI", buf, pos + 12)
    if magic != MAGIC_V2:
        raise BatchDecodeError(f"unsupported magic: {magic}")

    crc_start = pos + 21
    crc_computed = crc32c(buf[crc_start:batch_end])
    if crc_computed != crc_read:
        raise CrcMismatchError(f"CRC mismatch: computed={crc_computed} read={crc_read} at batch start={pos}")

    (
        attributes,
        last_offset_delta,
        first_timestamp,
        max_timestamp,
        producer_id,
        producer_epoch,
        base_sequence,
        num_records,
    ) = struct.unpack_from(">hiqqqhii", buf, crc_start)
    codec = attributes & CODEC_MASK
    if codec != 0:
        raise UnsupportedCodecError(
            f"batch is compressed with codec id {codec}; this client reads codec 0 "
            "(uncompressed) only — produce uncompressed or use the Java client"
        )

    p = crc_start + 40
    records: list[Record] = []
    for _ in range(num_records):
        inner, p = read_varint(buf, p)
        rec_end = p + inner
        if rec_end > batch_end:
            raise BatchDecodeError("record runs past end of batch")
        p += 1  # record attributes (unused)
        timestamp_delta, p = read_varlong(buf, p)
        offset_delta, p = read_varint(buf, p)
        key_len, p = read_varint(buf, p)
        key = None
        if key_len >= 0:
            key = buf[p : p + key_len]
            p += key_len
        value_len, p = read_varint(buf, p)
        value = None
        if value_len >= 0:
            value = buf[p : p + value_len]
            p += value_len
        header_count, p = read_varint(buf, p)
        headers: list[tuple[bytes | None, bytes | None]] = []
        for _ in range(max(header_count, 0)):
            hk_len, p = read_varint(buf, p)
            hk = None
            if hk_len >= 0:
                hk = buf[p : p + hk_len]
                p += hk_len
            hv_len, p = read_varint(buf, p)
            hv = None
            if hv_len >= 0:
                hv = buf[p : p + hv_len]
                p += hv_len
            headers.append((hk, hv))
        p = rec_end  # skip any unread trailer
        records.append(
            Record(
                offset_delta=offset_delta,
                timestamp_delta=timestamp_delta,
                key=key,
                value=value,
                headers=tuple(headers),
            )
        )
    return (
        ParsedBatch(
            base_offset=base_offset,
            batch_length=batch_length,
            partition_leader_epoch=partition_leader_epoch,
            last_offset_delta=last_offset_delta,
            first_timestamp=first_timestamp,
            max_timestamp=max_timestamp,
            producer_id=producer_id,
            producer_epoch=producer_epoch,
            base_sequence=base_sequence,
            codec=codec,
            records=records,
        ),
        batch_end,
    )


def decode_all(buf: bytes) -> list[ParsedBatch]:
    """Decode every complete batch in ``buf``, like the Java fetch path:

    stops silently at a truncated trailing batch (the broker may cut a
    response mid-batch at the max_bytes boundary).
    """
    out: list[ParsedBatch] = []
    pos = 0
    while len(buf) - pos >= BATCH_OVERHEAD:
        try:
            batch, pos = decode(buf, pos)
        except BatchDecodeError:
            break
        out.append(batch)
    return out
