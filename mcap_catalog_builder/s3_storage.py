"""S3-backed Source: range-GET footer reads, ETag fingerprints, paginated LIST.

This module never imports ``boto3``/``botocore``. The S3 client is injected
(``S3Source(boto3.client("s3"), bucket)``), so the catalog builder library and
its tests run with no AWS dependency; only an actual deployment needs boto3.

The cheap-read guarantee (R2) is structural: the MCAP reader's ``get_summary()``
only seeks to the footer and reads the summary section, and ``S3RangeReader``
turns each of those seeks/reads into a small HTTP Range GET — so the message
body is never downloaded, however large the recording.
"""

import io
from typing import Iterator

from .storage import Listing, Stat

# Error codes a HEAD/GET returns when an object is absent (duck-typed off the
# botocore ClientError shape, so botocore need not be importable here).
_MISSING_CODES = {"404", "NoSuchKey", "NotFound"}


def _is_missing(exc: Exception) -> bool:
    resp = getattr(exc, "response", None)
    if not isinstance(resp, dict):
        return False
    return resp.get("Error", {}).get("Code") in _MISSING_CODES


def _last_modified_ns(last_modified) -> int:
    """Convert a boto3 ``LastModified`` datetime to ns, or 0 if absent."""
    if last_modified is None:
        return 0
    return int(last_modified.timestamp() * 1_000_000_000)


class S3RangeReader(io.RawIOBase):
    """A seekable, read-only view of an S3 object backed by HTTP Range GETs.

    Only the bytes a caller actually seeks to and reads are fetched. ``size``
    (the object's ContentLength, known from the listing/HEAD) lets ``SEEK_END``
    work without a body read.
    """

    def __init__(self, client, bucket: str, key: str, size: int) -> None:
        self._c = client
        self._bucket = bucket
        self._key = key
        self._size = size
        self._pos = 0

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        base = {io.SEEK_SET: 0, io.SEEK_CUR: self._pos, io.SEEK_END: self._size}[whence]
        self._pos = base + offset
        return self._pos

    def readinto(self, b) -> int:
        if self._pos >= self._size:
            return 0  # at/past EOF: never issue an out-of-range request
        end = min(self._pos + len(b), self._size) - 1  # HTTP Range end is inclusive
        body = self._c.get_object(
            Bucket=self._bucket, Key=self._key, Range=f"bytes={self._pos}-{end}",
        )["Body"].read()
        n = len(body)
        b[:n] = body
        self._pos += n
        return n


class S3Source:
    """The object-store backend: keys are S3 object keys (the Hive key itself)."""

    def __init__(self, client, bucket: str, prefix: str = "") -> None:
        self._c = client
        self._bucket = bucket
        self._prefix = prefix

    def stat(self, key: str) -> Stat | None:
        try:
            h = self._c.head_object(Bucket=self._bucket, Key=key)
        except Exception as e:  # noqa: BLE001 - re-raised unless it's a missing-object
            if _is_missing(e):
                return None
            raise
        return Stat(
            size=h["ContentLength"],
            etag=h["ETag"].strip('"'),
            mtime_ns=_last_modified_ns(h.get("LastModified")),
        )

    def event_key(self, payload: str) -> str:
        return payload  # the SQS event already carries the object key

    def intended_key(self, key: str) -> str | None:
        return None  # the object key is authoritative; no in-file override

    def wait_for_stable(self, payload: str) -> bool:
        return True  # an S3 PUT / multipart-complete is atomic — nothing to poll

    def open_summary(self, key: str, size: int):
        # BufferedReader coalesces the MCAP reader's small sequential reads of
        # the summary section into a few larger range GETs.
        return io.BufferedReader(
            S3RangeReader(self._c, self._bucket, key, size), buffer_size=1 << 16
        )

    def list_all(self) -> Iterator[Listing]:
        for page in self._c.get_paginator("list_objects_v2").paginate(
            Bucket=self._bucket, Prefix=self._prefix
        ):
            for o in page.get("Contents", []):
                if o["Key"].endswith(".mcap"):
                    # ETag + Size come from the LIST itself — R4's "fingerprint
                    # from the listing", zero GETs.
                    yield Listing(
                        key=o["Key"],
                        stat=Stat(size=o["Size"], etag=o["ETag"].strip('"')),
                    )
