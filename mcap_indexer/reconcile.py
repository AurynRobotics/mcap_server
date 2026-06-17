"""Full reconcile scan: index every on-disk file, then hard-delete vanished rows.

This is the authoritative path for removals (live ``on_deleted`` events are
best-effort). It runs on the single writer thread like everything else.
"""

import logging
import os
from pathlib import Path

import sqlite3

from .db import Caches
from .indexer import index_file, resolve_dimensions

logger = logging.getLogger(__name__)


def _is_indexable_name(name: str) -> bool:
    return (
        name.endswith(".mcap")
        and not name.startswith(".")
        and not name.endswith(".mcap.tmp")
        and not name.endswith(".part")
    )


def scan_disk(watched_root: str) -> list[str]:
    """Return sorted absolute paths of indexable ``.mcap`` files under ``watched_root``.

    Skips dotfiles, any path with a hidden directory component, and ``*.mcap.tmp`` /
    ``*.part`` temp files.
    """
    out: list[str] = []
    root = Path(watched_root)
    for p in root.rglob("*.mcap"):
        rel_parts = p.relative_to(root).parts
        if any(part.startswith(".") for part in rel_parts):
            continue
        if _is_indexable_name(p.name):
            out.append(str(p))
    return sorted(out)


def full_reconcile(
    conn: sqlite3.Connection, caches: Caches, watched_root: str
) -> dict[str, int]:
    """Index all on-disk files, then delete catalog rows with no on-disk file.

    Returns a tally ``{"indexed", "skipped", "failed", "deleted"}``.
    """
    tally = {"indexed": 0, "skipped": 0, "failed": 0, "deleted": 0}
    paths = scan_disk(watched_root)
    for path in paths:
        tally[index_file(conn, caches, path, watched_root).status] += 1

    # Deletion sweep: composite keys present on disk (parseable + cached ids).
    present: set[tuple] = set()
    for path in paths:
        res = resolve_dimensions(path, watched_root)
        if res is None:
            continue
        dims = res[0]
        cid = caches.customer.get(dims["customer"])
        sid = caches.site.get((cid, dims["site"])) if cid is not None else None
        rid = caches.robot.get((sid, dims["robot"])) if sid is not None else None
        srcid = caches.source.get(dims["source"])
        if None in (cid, sid, rid, srcid):
            continue
        present.add((cid, sid, rid, srcid, dims["date"], dims["filename"]))

    for r in conn.execute(
        "SELECT id, customer_id, site_id, robot_id, source_id, date, filename FROM files"
    ).fetchall():
        comp = (
            r["customer_id"], r["site_id"], r["robot_id"], r["source_id"],
            r["date"], r["filename"],
        )
        if comp not in present:
            conn.execute("DELETE FROM files WHERE id=?", (r["id"],))
            tally["deleted"] += 1
    conn.commit()

    logger.info(
        "reconcile %s: indexed=%d skipped=%d failed=%d deleted=%d",
        watched_root, tally["indexed"], tally["skipped"], tally["failed"], tally["deleted"],
    )
    return tally
