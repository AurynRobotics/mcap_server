"""Storage abstraction: where recordings live and how to cheaply inspect them.

The catalog builder only ever needs three things from a backend:

1. a **fingerprint** for a key, derived from the listing/HEAD with **no body
   read** (R4 cheap change detection);
2. a **seekable byte stream** whose ``get_summary()`` touches only the footer +
   summary section, never the message body (R2 footer-only read);
3. a way to **list every recording** for the reconcile sweep (R5).

``LocalSource`` is today's local-filesystem behavior behind that seam;
``s3_storage.S3Source`` is the object-store implementation. Defining the seam
here keeps the two interchangeable and ``boto3`` an optional, runtime-only dep.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Iterator, Protocol


@dataclass(frozen=True)
class Stat:
    """A recording's cheap fingerprint, from the listing — no body read."""

    size: int
    etag: str  # the change token (R4): the real S3 ETag, or a synthetic local one


@dataclass(frozen=True)
class Listing:
    """One recording surfaced by a full listing: its key plus fingerprint."""

    key: str
    stat: Stat


class Source(Protocol):
    """A storage backend the catalog builder can read without knowing where bytes live."""

    def stat(self, key: str) -> Stat | None:
        """Fingerprint for ``key`` from the listing/HEAD (no body read), or ``None`` if gone."""

    def open_summary(self, key: str, size: int) -> BinaryIO:
        """A seekable, read-only stream for ``key``; only footer+summary get fetched."""

    def list_all(self) -> Iterator[Listing]:
        """Every catalogable ``.mcap`` (key + fingerprint) for the reconcile sweep."""


def _is_catalogable_name(name: str) -> bool:
    return (
        name.endswith(".mcap")
        and not name.startswith(".")
        and not name.endswith(".mcap.tmp")
        and not name.endswith(".part")
    )


def local_etag(size: int, mtime_ns: int) -> str:
    """Synthetic local change token — mirrors ``builder.synth_etag``."""
    return f"local:{size}:{mtime_ns}"


class LocalSource:
    """The local-filesystem backend: keys are POSIX paths relative to ``root``."""

    def __init__(self, root: str) -> None:
        self._root = root

    def _abs(self, key: str) -> str:
        return os.path.join(self._root, key)

    def stat(self, key: str) -> Stat | None:
        try:
            st = os.stat(self._abs(key))
        except OSError:
            return None
        return Stat(size=st.st_size, etag=local_etag(st.st_size, st.st_mtime_ns))

    def open_summary(self, key: str, size: int) -> BinaryIO:
        return open(self._abs(key), "rb")

    def list_all(self) -> Iterator[Listing]:
        root = Path(self._root)
        for p in root.rglob("*.mcap"):
            rel = p.relative_to(root)
            if any(part.startswith(".") for part in rel.parts):
                continue
            if not _is_catalogable_name(p.name):
                continue
            st = self.stat(str(rel))
            if st is not None:
                yield Listing(key=str(rel), stat=st)
