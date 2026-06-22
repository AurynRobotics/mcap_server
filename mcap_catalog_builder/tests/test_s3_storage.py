"""Tests for the S3 backend: range-GET reader + S3Source.

A FakeS3 client serves bytes from an in-memory dict and records every range
requested, so the tests assert the cheap-read property (R2) — that reading a
summary never downloads the message body — without touching AWS or boto3.
"""

import io

import pytest
from mcap.writer import CompressionType, Writer

from mcap_catalog_builder.mcap_summary import read_file_summary, summary_from_stream
from mcap_catalog_builder.s3_storage import (
    S3RangeReader,
    S3Source,
    _is_missing,
    _is_permanent,
)
from mcap_catalog_builder.tests.fixtures import write_minimal_mcap


class _FakeClientError(Exception):
    """Mimics botocore's ClientError shape for the missing-object path."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakeS3:
    """In-memory S3 stand-in recording the byte ranges it is asked for."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self._objects = objects
        self.ranges: list[tuple[int, int]] = []  # (start, end) inclusive
        self.fetched = 0

    def head_object(self, Bucket, Key):
        if Key not in self._objects:
            raise _FakeClientError("404")
        return {"ContentLength": len(self._objects[Key]), "ETag": f'"etag-{Key}"'}

    def get_object(self, Bucket, Key, Range):
        assert Range.startswith("bytes=")
        start_s, end_s = Range[len("bytes="):].split("-")
        start, end = int(start_s), int(end_s)
        self.ranges.append((start, end))
        chunk = self._objects[Key][start:end + 1]
        self.fetched += len(chunk)
        return {"Body": io.BytesIO(chunk)}

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        objects = self._objects

        class _Paginator:
            def paginate(self, Bucket, Prefix=""):
                contents = [
                    {"Key": k, "Size": len(v), "ETag": f'"etag-{k}"'}
                    for k, v in objects.items()
                    if k.startswith(Prefix)
                ]
                yield {"Contents": contents}

        return _Paginator()


# --- S3RangeReader ---------------------------------------------------------

def test_range_reader_reads_exact_slice():
    data = bytes(range(256))
    r = S3RangeReader(FakeS3({"k": data}), "b", "k", len(data))
    assert r.seek(10) == 10
    assert r.read(5) == data[10:15]
    assert r.tell() == 15


def test_range_reader_seek_end_and_read_past_eof():
    data = bytes(range(256))
    client = FakeS3({"k": data})
    r = S3RangeReader(client, "b", "k", len(data))
    assert r.seek(-4, io.SEEK_END) == 252
    assert r.read(10) == data[252:256]   # clamped to EOF, no out-of-range request
    assert r.read(10) == b""             # nothing left
    assert max(end for _, end in client.ranges) <= 255


def test_range_reader_never_downloads_whole_object():
    size = 100_000
    client = FakeS3({"k": b"\x00" * size})
    r = S3RangeReader(client, "b", "k", size)
    r.seek(0)
    r.read(8)
    r.seek(size - 10)
    r.read(50)
    assert client.fetched == 18                      # 8 + 10 (tail clamped), body untouched
    assert all(end - start + 1 <= 50 for start, end in client.ranges)


# --- S3Source --------------------------------------------------------------

def test_stat_returns_size_and_unquoted_etag():
    src = S3Source(FakeS3({"k.mcap": b"abc"}), "bucket")
    st = src.stat("k.mcap")
    assert st.size == 3
    assert st.etag == "etag-k.mcap"            # surrounding quotes stripped


def test_stat_missing_returns_none():
    assert S3Source(FakeS3({}), "bucket").stat("gone.mcap") is None


def test_permanent_classifier_includes_auth_and_missing():
    # Auth/bad-request + missing are permanent (not retried); 5xx/throttle retry.
    for code in ("403", "AccessDenied", "Forbidden", "400", "404", "NoSuchKey"):
        assert _is_permanent(_FakeClientError(code)), code
    for code in ("500", "503", "SlowDown"):
        assert not _is_permanent(_FakeClientError(code)), code
    assert _is_missing(_FakeClientError("404")) and not _is_missing(_FakeClientError("403"))


def test_stat_auth_error_raises_not_retried():
    # A 403 must propagate (not be mapped to None, not retried 6x).
    class _Counting(FakeS3):
        def __init__(self):
            super().__init__({})
            self.head_calls = 0

        def head_object(self, Bucket, Key):
            self.head_calls += 1
            raise _FakeClientError("403")

    c = _Counting()
    with pytest.raises(_FakeClientError):
        S3Source(c, "bucket").stat("x.mcap")
    assert c.head_calls == 1  # permanent => no retry


def test_list_all_filters_non_mcap_and_unquotes_etag():
    client = FakeS3({"a/x.mcap": b"1", "a/notes.txt": b"2", "b/y.mcap": b"33"})
    listings = sorted(S3Source(client, "bucket").list_all(), key=lambda x: x.key)
    assert [x.key for x in listings] == ["a/x.mcap", "b/y.mcap"]
    assert listings[0].stat.etag == "etag-a/x.mcap"
    assert listings[1].stat.size == 2


def test_s3_event_translation_helpers():
    src = S3Source(FakeS3({}), "bucket")
    assert src.event_key("customer=a/.../x.mcap") == "customer=a/.../x.mcap"  # key is the key
    assert src.intended_key("anything") is None       # the object key is authoritative
    assert src.wait_for_stable("anything") is True     # S3 PUT is atomic, no poll


def test_open_summary_reads_real_mcap_without_downloading_body(tmp_path):
    # A real MCAP with a multi-MB, incompressible body so "body skipped" is
    # unmistakable: the summary section is tiny next to the message chunks.
    dest = str(tmp_path / "big.mcap")
    with open(dest, "wb") as f:
        w = Writer(f, compression=CompressionType.NONE)
        w.start(profile="ros2", library="test")
        sid = w.register_schema(name="S", encoding="ros2msg", data=b"x")
        cid = w.register_channel(topic="/a", message_encoding="cdr", schema_id=sid)
        payload = bytes(range(256)) * 8  # 2 KiB, incompressible-ish
        for t in range(1, 2001):
            w.add_message(channel_id=cid, log_time=t, data=payload, publish_time=t)
        w.finish()
    with open(dest, "rb") as f:
        raw = f.read()
    total = len(raw)
    assert total > 3_000_000  # body dominates

    client = FakeS3({"big.mcap": raw})
    src = S3Source(client, "bucket")
    with src.open_summary("big.mcap", src.stat("big.mcap").size) as stream:
        got = summary_from_stream(stream)

    assert got == read_file_summary(dest)        # identical to the local read
    assert client.fetched < 200_000              # only footer + summary fetched
