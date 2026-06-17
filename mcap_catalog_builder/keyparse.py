"""Hive object-key parsing, rebuilding, and relative-path key derivation.

The object key is a Hive-partitioned path::

    customer=<c>/customer_site=<site>/robot=<r>/source=<s>/date=<d>/<filename>.mcap

Note the path literal is ``customer_site=`` but the parsed field is ``site``.
``rebuild_hive_key`` is the exact inverse of ``parse_hive_key`` so the catalog builder
can verify ``rebuild_hive_key(dims) == key.lstrip('/')`` before trusting a parse.
"""

import os
import re

HIVE_RE: re.Pattern = re.compile(
    r"^customer=(?P<customer>[^/]+)/"
    r"customer_site=(?P<site>[^/]+)/"
    r"robot=(?P<robot>[^/]+)/"
    r"source=(?P<source>[^/]+)/"
    r"date=(?P<date>[^/]+)/"
    r"(?P<filename>[^/]+\.mcap)$"
)


def parse_hive_key(key: str) -> dict[str, str] | None:
    """Parse a Hive key into ``{customer, site, robot, source, date, filename}``.

    Returns ``None`` if the key does not match the exact template (e.g. a flat
    name, a missing partition, or a non-``.mcap`` filename).
    """
    m = HIVE_RE.match(key.lstrip("/"))
    return m.groupdict() if m is not None else None


def rebuild_hive_key(dims: dict[str, str]) -> str:
    """Rebuild the object key from parsed dimensions (exact inverse of parse)."""
    return (
        f"customer={dims['customer']}/"
        f"customer_site={dims['site']}/"
        f"robot={dims['robot']}/"
        f"source={dims['source']}/"
        f"date={dims['date']}/"
        f"{dims['filename']}"
    )


def relpath_key(abs_path: str, watched_root: str) -> str:
    """The file's path relative to ``watched_root``, POSIX-normalized, no leading slash."""
    return os.path.relpath(abs_path, watched_root).replace(os.sep, "/")
