"""Unsigned LEB128 varint codec and the packed topic-counts blob.

``files.topic_counts`` is one unsigned-LEB128 varint per topic-set member,
ordered by ``topic_id`` ASC (matching ``topic_set_members`` order).
"""


def encode_varint(n: int) -> bytes:
    """Encode a non-negative integer as an unsigned LEB128 varint."""
    if n < 0:
        raise ValueError(f"varint cannot encode negative value: {n}")
    out = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        if n:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def decode_varint(data: bytes, pos: int = 0) -> tuple[int, int]:
    """Decode one unsigned LEB128 varint from ``data`` starting at ``pos``.

    Returns ``(value, new_pos)``. Raises ``ValueError`` on truncated input.
    """
    result = 0
    shift = 0
    while True:
        if pos >= len(data):
            raise ValueError("truncated varint")
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return result, pos
        shift += 7


def encode_counts_blob(counts: list[int]) -> bytes:
    """Pack a list of per-topic message counts into a varint blob."""
    return b"".join(encode_varint(c) for c in counts)


def decode_counts_blob(blob: bytes) -> list[int]:
    """Unpack a varint blob back into the list of counts it encodes."""
    counts: list[int] = []
    pos = 0
    while pos < len(blob):
        value, pos = decode_varint(blob, pos)
        counts.append(value)
    return counts
