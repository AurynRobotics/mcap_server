"""Test fixtures: real-data access, synthetic MCAP writers, and Hive trees.

These helpers let the suite exercise the happy path even though the real
``../DATA/dexory`` samples are flat and carry no ``s3_key`` (so they must be
copied into a Hive tree, or a synthetic MCAP with an injected ``s3_key`` used).
"""

import io
import os
import shutil

import pytest
from mcap.writer import Writer

DEXORY_DIR = "/home/davide/ws_plotjuggler/DATA/dexory"


class _S3ClientError(Exception):
    """Mimics botocore's ClientError shape for the missing-object path."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class InMemoryS3Client:
    """A minimal in-memory S3 client (head / ranged get / list) for tests.

    Serves bytes from a ``{key: data}`` dict and tallies the bytes fetched, so
    tests can assert the cheap-read property without touching AWS or boto3.
    """

    def __init__(self, objects: dict[str, bytes]) -> None:
        self._objects = dict(objects)
        self.fetched = 0

    def head_object(self, Bucket, Key):
        if Key not in self._objects:
            raise _S3ClientError("404")
        return {"ContentLength": len(self._objects[Key]), "ETag": f'"etag-{Key}"'}

    def get_object(self, Bucket, Key, Range):
        start_s, end_s = Range[len("bytes="):].split("-")
        chunk = self._objects[Key][int(start_s):int(end_s) + 1]
        self.fetched += len(chunk)
        return {"Body": io.BytesIO(chunk)}

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        objects = self._objects

        class _Paginator:
            def paginate(self, Bucket, Prefix=""):
                yield {
                    "Contents": [
                        {"Key": k, "Size": len(v), "ETag": f'"etag-{k}"'}
                        for k, v in objects.items()
                        if k.startswith(Prefix)
                    ]
                }

        return _Paginator()


def dexory_file(name: str) -> str:
    """Absolute path to a real Dexory sample; ``pytest.skip`` if the data is absent."""
    path = os.path.join(DEXORY_DIR, name)
    if not os.path.exists(path):
        pytest.skip(f"Dexory data absent: {path}")
    return path


def make_hive_fixture(src_mcap: str, root: str, dims: dict[str, str]) -> str:
    """Copy ``src_mcap`` into a Hive tree under ``root`` and return the dest path.

    ``shutil.copy2`` preserves mtime so the (size, mtime) fingerprint is deterministic.
    """
    dest = os.path.join(
        root,
        f"customer={dims['customer']}",
        f"customer_site={dims['site']}",
        f"robot={dims['robot']}",
        f"source={dims['source']}",
        f"date={dims['date']}",
        dims["filename"],
    )
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    shutil.copy2(src_mcap, dest)
    return dest


def write_minimal_mcap(
    dest: str,
    *,
    s3_key: str | None = None,
    channels: list[tuple[str, str, str, int]] | None = None,
) -> None:
    """Write a small valid MCAP for tests.

    ``channels`` is a list of ``(topic, schema_name, schema_encoding, n_messages)``.
    ``n_messages == 0`` yields a declared-but-empty channel (absent from
    ``channel_message_counts``). If ``s3_key`` is given, an ``s3_key`` metadata
    record carrying ``{"key": s3_key}`` is written.
    """
    if channels is None:
        channels = [("/test", "std_msgs/msg/String", "ros2msg", 1)]
    os.makedirs(os.path.dirname(dest) or ".", exist_ok=True)
    with open(dest, "wb") as f:
        writer = Writer(f)
        writer.start(profile="ros2", library="test-fixture")
        schema_ids: dict[tuple[str, str], int] = {}
        log_time = 1
        for topic, schema_name, schema_encoding, n_messages in channels:
            schema_key = (schema_name, schema_encoding)
            if schema_key not in schema_ids:
                schema_ids[schema_key] = writer.register_schema(
                    name=schema_name, encoding=schema_encoding, data=b"x"
                )
            channel_id = writer.register_channel(
                topic=topic, message_encoding="cdr", schema_id=schema_ids[schema_key]
            )
            for _ in range(n_messages):
                writer.add_message(
                    channel_id=channel_id,
                    log_time=log_time,
                    data=b"d",
                    publish_time=log_time,
                    sequence=0,
                )
                log_time += 1
        if s3_key is not None:
            writer.add_metadata(name="s3_key", data={"key": s3_key})
        writer.finish()


def write_flat_no_metadata(dest: str) -> None:
    """A minimal MCAP with no ``s3_key`` metadata (mimics the flat Dexory samples)."""
    write_minimal_mcap(dest, s3_key=None, channels=[("/a", "S", "ros2msg", 1)])


def write_unsummarized_mcap(dest: str, s3_key: str | None = None) -> None:
    """An MCAP with NO Statistics/summary (``use_statistics=False``) — the codec
    rejects it, so the builder must quarantine it (catalog-migration §4.6)."""
    os.makedirs(os.path.dirname(dest) or ".", exist_ok=True)
    with open(dest, "wb") as f:
        writer = Writer(f, use_statistics=False, use_summary_offsets=False)
        writer.start(profile="ros2", library="test-fixture")
        sid = writer.register_schema(name="S", encoding="ros2msg", data=b"x")
        cid = writer.register_channel(topic="/a", message_encoding="cdr", schema_id=sid)
        writer.add_message(channel_id=cid, log_time=1, data=b"d", publish_time=1, sequence=0)
        if s3_key is not None:
            writer.add_metadata(name="s3_key", data={"key": s3_key})
        writer.finish()
