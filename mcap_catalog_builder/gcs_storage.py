"""GCS-backed Source: range-download footer reads, Generation fingerprints,
paginated LIST.

This module never imports ``google.cloud.storage`` / ``google.api_core``. The
client is injected (``GCSSource(storage.Client(), bucket)``), exactly like the S3
module injects boto3, so the catalog builder library + its tests need no
google-cloud-storage dependency; only a real deployment does.

Change-detection identity matches the Go reader's ``gcsreader.go``: the
**Generation** (a monotonic int64, as a decimal string) is the etag, and
**Updated** is the modification time. ``STORAGE_EMULATOR_HOST`` is auto-handled by
the SDK on the injected client (the fake-gcs leg), so this module is unaware of it.
"""

import calendar
import io
from typing import Iterator

from .retry import retry_with
from .storage import Listing, Stat

# Permanent (non-retryable) HTTP codes: bad-request / auth / missing. The Go GCS
# classifier (gcsreader.go) treats 400/403/404 as permanent, 429/5xx as transient;
# retrying a 403/400 just wastes the backoff budget. Duck-typed off
# google.api_core.exceptions so that package need not be importable here.
_PERMANENT_CODES = {400, 403, 404}


def _is_permanent(exc: Exception) -> bool:
    return getattr(exc, "code", None) in _PERMANENT_CODES or type(exc).__name__ in {
        "NotFound", "Forbidden", "BadRequest",
    }


def _updated_ns(updated) -> int:
    """Convert a GCS blob ``updated`` datetime to ns, or 0 if absent. Integer
    arithmetic (no float*1e9) so it is exact to microsecond resolution and matches
    Go's Updated.UnixNano()."""
    if updated is None:
        return 0
    return calendar.timegm(updated.utctimetuple()) * 1_000_000_000 + updated.microsecond * 1000


def _stat_from_blob(blob) -> Stat:
    # generation = the monotonic change token (matches gcsreader.go's identity).
    return Stat(size=blob.size, etag=str(blob.generation), mtime_ns=_updated_ns(blob.updated))


class GCSRangeReader(io.RawIOBase):
    """A seekable, read-only view of a GCS object backed by ranged downloads —
    only the bytes a caller seeks to + reads are fetched (the footer + summary)."""

    def __init__(self, client, bucket: str, key: str, size: int) -> None:
        self._blob = client.bucket(bucket).blob(key)
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
        end = min(self._pos + len(b), self._size) - 1  # GCS range end is inclusive
        data = retry_with(
            lambda: self._blob.download_as_bytes(start=self._pos, end=end),
            is_permanent=_is_permanent,
        )
        n = len(data)
        b[:n] = data
        self._pos += n
        return n


class GCSSource:
    """The GCS backend: keys are object names (the Hive key itself)."""

    def __init__(self, client, bucket: str, prefix: str = "") -> None:
        self._c = client
        self._bucket = bucket
        self._prefix = prefix

    def stat(self, key: str) -> Stat | None:
        # get_blob returns None for a missing object (it does not raise), so a
        # genuine 404 falls through to None; transient errors retry.
        blob = retry_with(lambda: self._c.bucket(self._bucket).get_blob(key), is_permanent=_is_permanent)
        if blob is None:
            return None
        return _stat_from_blob(blob)

    def event_key(self, payload: str) -> str:
        return payload  # the event already carries the object key

    def intended_key(self, key: str) -> str | None:
        return None  # the object key is authoritative; no in-file override

    def wait_for_stable(self, payload: str) -> bool:
        return True  # a GCS upload is atomic — nothing to poll

    def open_summary(self, key: str, size: int):
        return io.BufferedReader(
            GCSRangeReader(self._c, self._bucket, key, size), buffer_size=1 << 16
        )

    def list_all(self) -> Iterator[Listing]:
        # list_blobs is lazy + paginates as it is iterated; materialize under retry
        # so a transient mid-list error re-lists rather than aborting the reconcile.
        for blob in retry_with(
            lambda: list(self._c.list_blobs(self._bucket, prefix=self._prefix)),
            is_permanent=_is_permanent,
        ):
            if blob.name.endswith(".mcap"):
                yield Listing(key=blob.name, stat=_stat_from_blob(blob))
