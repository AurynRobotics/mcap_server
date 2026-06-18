"""Full reconcile scan: catalog every object in the source, then hard-delete
vanished rows.

This is the authoritative path for removals (live ``on_deleted`` / SQS-delete
events are best-effort). It is **backend-agnostic**: it iterates a storage
``Source.list_all()``, so it works over the local filesystem or S3. It runs on
the single writer thread like everything else.
"""

import logging
from pathlib import Path

import sqlite3

from .db import Caches
from .builder import catalog_object, resolve_key_dims
from .storage import LocalSource

logger = logging.getLogger(__name__)


def _is_catalogable_name(name: str) -> bool:
    return (
        name.endswith(".mcap")
        and not name.startswith(".")
        and not name.endswith(".mcap.tmp")
        and not name.endswith(".part")
    )


def scan_disk(watched_root: str) -> list[str]:
    """Return sorted absolute paths of catalogable ``.mcap`` files under ``watched_root``.

    Skips dotfiles, any path with a hidden directory component, and ``*.mcap.tmp`` /
    ``*.part`` temp files.
    """
    out: list[str] = []
    root = Path(watched_root)
    for p in root.rglob("*.mcap"):
        rel_parts = p.relative_to(root).parts
        if any(part.startswith(".") for part in rel_parts):
            continue
        if _is_catalogable_name(p.name):
            out.append(str(p))
    return sorted(out)


def full_reconcile(conn: sqlite3.Connection, caches: Caches, source) -> dict[str, int]:
    """Catalog all objects in ``source``, then delete catalog rows with no object.

    ``source`` is a storage ``Source``; a ``str`` is accepted as shorthand for a
    local watch root. Returns a tally ``{"cataloged", "skipped", "failed", "deleted"}``.
    """
    if isinstance(source, str):
        source = LocalSource(source)

    tally = {"cataloged": 0, "skipped": 0, "failed": 0, "deleted": 0}
    listings = list(source.list_all())
    for lst in listings:
        tally[catalog_object(conn, caches, lst.key, source).status] += 1

    # Deletion sweep: composite keys present in the source (parseable + cached ids).
    present: set[tuple] = set()
    for lst in listings:
        res = resolve_key_dims(lst.key, source)
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
        "reconcile: cataloged=%d skipped=%d failed=%d deleted=%d",
        tally["cataloged"], tally["skipped"], tally["failed"], tally["deleted"],
    )
    return tally
