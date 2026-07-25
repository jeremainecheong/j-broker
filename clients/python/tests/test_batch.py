"""Unit tests for the v2 record-batch codec (no broker required)."""

import struct

import pytest

from jbroker.batch import (
    BATCH_OVERHEAD,
    BatchDecodeError,
    CrcMismatchError,
    Record,
    UnsupportedCodecError,
    crc32c,
    decode,
    decode_all,
    encode,
)


def test_crc32c_known_vector():
    # RFC 3720 test vector for CRC-32C.
    assert crc32c(b"123456789") == 0xE3069283
    assert crc32c(b"") == 0


def test_round_trip_single_record():
    batch = encode([Record(offset_delta=0, value=b"hello")], first_timestamp=123, max_timestamp=123)
    parsed, consumed = decode(batch)
    assert consumed == len(batch)
    assert parsed.base_offset == 0
    assert parsed.last_offset_delta == 0
    assert parsed.first_timestamp == 123
    assert parsed.max_timestamp == 123
    assert parsed.producer_id == -1
    assert parsed.producer_epoch == -1
    assert parsed.base_sequence == -1
    assert parsed.codec == 0
    assert parsed.total_bytes == len(batch)
    [rec] = parsed.records
    assert rec.value == b"hello"
    assert rec.key is None
    assert rec.headers == ()


def test_round_trip_many_records_with_keys_and_headers():
    records = [
        Record(offset_delta=0, timestamp_delta=0, key=b"k0", value=b"v0"),
        Record(offset_delta=1, timestamp_delta=5, key=None, value=b"v1"),
        Record(offset_delta=2, timestamp_delta=9, key=b"k2", value=None),
        Record(
            offset_delta=3,
            timestamp_delta=-2,
            key=b"",
            value=b"x" * 300,
            headers=((b"h", b"1"), (b"empty", b""), (b"nullv", None)),
        ),
    ]
    batch = encode(
        records,
        base_offset=42,
        partition_leader_epoch=7,
        first_timestamp=1_000,
        max_timestamp=1_009,
        producer_id=99,
        producer_epoch=3,
        base_sequence=17,
    )
    parsed, _ = decode(batch)
    assert parsed.base_offset == 42
    assert parsed.partition_leader_epoch == 7
    assert parsed.last_offset_delta == 3
    assert parsed.last_offset == 45
    assert parsed.producer_id == 99
    assert parsed.producer_epoch == 3
    assert parsed.base_sequence == 17
    assert parsed.records == records


def test_decode_all_concatenated_batches():
    a = encode([Record(offset_delta=0, value=b"a")])
    b = encode([Record(offset_delta=0, value=b"b"), Record(offset_delta=1, value=b"c")], base_offset=1)
    batches = decode_all(a + b)
    assert [r.value for p in batches for r in p.records] == [b"a", b"b", b"c"]
    assert batches[1].base_offset == 1


def test_decode_all_stops_at_truncated_trailing_batch():
    a = encode([Record(offset_delta=0, value=b"a")])
    b = encode([Record(offset_delta=0, value=b"b" * 100)])
    batches = decode_all(a + b[:-5])
    assert len(batches) == 1
    assert batches[0].records[0].value == b"a"


def test_crc_corruption_raises():
    batch = bytearray(encode([Record(offset_delta=0, value=b"payload")]))
    batch[-1] ^= 0xFF  # flip a record byte — CRC must catch it
    with pytest.raises(CrcMismatchError):
        decode(bytes(batch))


def test_short_buffer_raises():
    with pytest.raises(BatchDecodeError):
        decode(b"\x00" * (BATCH_OVERHEAD - 1))


def test_wrong_magic_raises():
    batch = bytearray(encode([Record(offset_delta=0, value=b"v")]))
    batch[16] = 1  # magic byte
    with pytest.raises(BatchDecodeError, match="magic"):
        decode(bytes(batch))


def test_unsupported_codec_raises_after_crc_check():
    batch = bytearray(encode([Record(offset_delta=0, value=b"v")]))
    # Set codec bits to 2 (zstd) in attributes at [21..22], then re-CRC so
    # the codec check is what fires, proving the error is deliberate.
    struct.pack_into(">h", batch, 21, 2)
    struct.pack_into(">I", batch, 17, crc32c(bytes(batch[21:])))
    with pytest.raises(UnsupportedCodecError, match="codec id 2"):
        decode(bytes(batch))


def test_empty_records_rejected_on_encode():
    with pytest.raises(ValueError):
        encode([])


def test_negative_varint_lengths_and_deltas_round_trip():
    # timestamp_delta may be negative (Java writeVarlong is zigzag);
    # exercise the negative path end to end.
    records = [Record(offset_delta=0, timestamp_delta=-1_000_000, value=b"v")]
    parsed, _ = decode(encode(records))
    assert parsed.records[0].timestamp_delta == -1_000_000
