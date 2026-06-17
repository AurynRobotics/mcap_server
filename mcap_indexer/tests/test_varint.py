"""Tests for the LEB128 varint codec and the topic-counts blob."""

import pytest

from mcap_indexer.varint import (
    decode_counts_blob,
    decode_varint,
    encode_counts_blob,
    encode_varint,
)


def test_encode_known_value():
    # The real Dexory file 197 has message_count == 1283397.
    assert encode_varint(1283397).hex() == "c5aa4e"


def test_encode_zero():
    assert encode_varint(0) == b"\x00"


def test_single_value_roundtrip():
    for n in [0, 1, 127, 128, 16383, 16384, 1283397, 2**21, 2**21 + 1, 2**35]:
        blob = encode_varint(n)
        value, pos = decode_varint(blob)
        assert value == n, n
        assert pos == len(blob), n


def test_encode_negative_raises():
    with pytest.raises(ValueError):
        encode_varint(-1)


def test_counts_blob_roundtrip():
    for counts in [[], [0], [0, 1200, 6000, 24001, 11996], [2**21, 0, 5]]:
        assert decode_counts_blob(encode_counts_blob(counts)) == counts


def test_decode_counts_blob_truncated_raises():
    # continuation bit set with no following byte
    with pytest.raises(ValueError):
        decode_counts_blob(b"\x80")


def test_decode_varint_truncated_raises():
    with pytest.raises(ValueError):
        decode_varint(b"\x80", 0)
