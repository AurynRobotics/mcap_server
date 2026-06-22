"""Tests for the GCS backend: ranged-download reader + GCSSource.

A FakeGCS client serves bytes from an in-memory dict and records every range
requested, so the tests assert the cheap-read property (R2) without touching GCS
or google-cloud-storage. Mirrors test_s3_storage.py.
"""

import io

from mcap.writer import CompressionType, Writer

from mcap_catalog_builder.gcs_storage import GCSRangeReader, GCSSource, _is_permanent
from mcap_catalog_builder.mcap_summary import read_file_summary, summary_from_stream


class _GcsError(Exception):
    def __init__(self, code):
        super().__init__(str(code))
        self.code = code


def test_permanent_classifier_includes_auth_and_missing():
    for code in (400, 403, 404):
        assert _is_permanent(_GcsError(code)), code
    for code in (429, 500, 503):
        assert not _is_permanent(_GcsError(code)), code


class _FakeBlob:
    def __init__(self, name, data, generation, client):
        self.name = name
        self._data = data
        self.size = len(data)
        self.generation = generation
        self.updated = None
        self._client = client

    def download_as_bytes(self, start=0, end=None):
        if end is None:
            end = self.size - 1
        self._client.ranges.append((start, end))  # inclusive
        chunk = self._data[start:end + 1]
        self._client.fetched += len(chunk)
        return chunk


class _FakeBucket:
    def __init__(self, client):
        self._client = client

    def blob(self, key):
        return _FakeBlob(key, self._client.objects.get(key, b""), self._client.gen.get(key, 1), self._client)

    def get_blob(self, key):
        if key not in self._client.objects:
            return None
        return _FakeBlob(key, self._client.objects[key], self._client.gen.get(key, 1), self._client)


class FakeGCS:
    """In-memory GCS stand-in recording the byte ranges it is asked for."""

    def __init__(self, objects: dict[str, bytes], generations: dict[str, int] | None = None) -> None:
        self.objects = objects
        self.gen = generations or {k: 1 for k in objects}
        self.ranges: list[tuple[int, int]] = []
        self.fetched = 0

    def bucket(self, name):
        return _FakeBucket(self)

    def list_blobs(self, bucket, prefix=""):
        return [
            _FakeBlob(k, v, self.gen.get(k, 1), self)
            for k, v in self.objects.items()
            if k.startswith(prefix)
        ]


# --- GCSRangeReader --------------------------------------------------------

def test_range_reader_reads_exact_slice():
    data = bytes(range(256))
    r = GCSRangeReader(FakeGCS({"k": data}), "b", "k", len(data))
    assert r.seek(10) == 10
    assert r.read(5) == data[10:15]
    assert r.tell() == 15


def test_range_reader_seek_end_and_read_past_eof():
    data = bytes(range(256))
    client = FakeGCS({"k": data})
    r = GCSRangeReader(client, "b", "k", len(data))
    assert r.seek(-4, io.SEEK_END) == 252
    assert r.read(10) == data[252:256]   # clamped to EOF, no out-of-range request
    assert r.read(10) == b""
    assert max(end for _, end in client.ranges) <= 255


def test_range_reader_never_downloads_whole_object():
    size = 100_000
    client = FakeGCS({"k": b"\x00" * size})
    r = GCSRangeReader(client, "b", "k", size)
    r.seek(0)
    r.read(8)
    r.seek(size - 10)
    r.read(50)
    assert client.fetched == 18  # 8 + 10 (tail clamped), body untouched


# --- GCSSource -------------------------------------------------------------

def test_stat_uses_generation_as_etag():
    src = GCSSource(FakeGCS({"k.mcap": b"abc"}, {"k.mcap": 17}), "bucket")
    st = src.stat("k.mcap")
    assert st.size == 3
    assert st.etag == "17"  # generation, decimal string (matches gcsreader.go)


def test_stat_generation_change_changes_etag():
    a = GCSSource(FakeGCS({"k.mcap": b"abc"}, {"k.mcap": 1}), "bucket").stat("k.mcap")
    b = GCSSource(FakeGCS({"k.mcap": b"abcd"}, {"k.mcap": 2}), "bucket").stat("k.mcap")
    assert a.etag != b.etag  # an overwrite (new generation) invalidates the cache key


def test_stat_missing_returns_none():
    assert GCSSource(FakeGCS({}), "bucket").stat("gone.mcap") is None


def test_list_all_filters_non_mcap():
    client = FakeGCS({"a/x.mcap": b"1", "a/notes.txt": b"2", "b/y.mcap": b"33"}, {"a/x.mcap": 5, "b/y.mcap": 9})
    listings = sorted(GCSSource(client, "bucket").list_all(), key=lambda x: x.key)
    assert [x.key for x in listings] == ["a/x.mcap", "b/y.mcap"]
    assert listings[0].stat.etag == "5"
    assert listings[1].stat.size == 2


def test_gcs_event_translation_helpers():
    src = GCSSource(FakeGCS({}), "bucket")
    assert src.event_key("customer=a/.../x.mcap") == "customer=a/.../x.mcap"
    assert src.intended_key("anything") is None
    assert src.wait_for_stable("anything") is True


def test_open_summary_reads_real_mcap_without_downloading_body(tmp_path):
    dest = str(tmp_path / "big.mcap")
    with open(dest, "wb") as f:
        w = Writer(f, compression=CompressionType.NONE)
        w.start(profile="ros2", library="test")
        sid = w.register_schema(name="S", encoding="ros2msg", data=b"x")
        cid = w.register_channel(topic="/a", message_encoding="cdr", schema_id=sid)
        payload = bytes(range(256)) * 8
        for t in range(1, 2001):
            w.add_message(channel_id=cid, log_time=t, data=payload, publish_time=t)
        w.finish()
    with open(dest, "rb") as f:
        raw = f.read()
    assert len(raw) > 3_000_000  # body dominates

    client = FakeGCS({"big.mcap": raw})
    src = GCSSource(client, "bucket")
    with src.open_summary("big.mcap", src.stat("big.mcap").size) as stream:
        got = summary_from_stream(stream)

    assert got == read_file_summary(dest)  # identical to the local read
    assert client.fetched < 200_000        # only footer + summary fetched
