"""The catalog builder core: dimension resolution + the §8 per-file transaction.

Correctness guards (this daemon is the catalog's only writer):
- dimensions are trusted only if ``rebuild_hive_key(dims) == key`` (round-trip);
- the ``topic_counts`` blob is built from the sorted topic-set members with a
  ``.get(channel_id, 0)`` default, then an in-transaction check
  ``sum(counts) == message_count`` rolls a bad row into ``catalog_failures``;
- on rollback the in-memory caches are reloaded from the committed DB state, so
  ids inserted inside the rolled-back transaction can never poison the caches.
"""

import hashlib
import json
import logging
import os
import sqlite3
from dataclasses import dataclass

from .db import (
    Caches,
    load_caches,
    now_ns,
    record_failure,
    resolve_customer,
    resolve_robot,
    resolve_schema,
    resolve_site,
    resolve_source,
    resolve_topic,
    resolve_topic_set,
)
from .keyparse import parse_hive_key, rebuild_hive_key, relpath_key
from .mcap_summary import derive_tags, extract_s3_key, read_file_summary
from .varint import encode_counts_blob

logger = logging.getLogger(__name__)

_COMPOSITE = (
    "customer_id=? AND site_id=? AND robot_id=? AND source_id=? AND date=? AND filename=?"
)


@dataclass(frozen=True)
class CatalogResult:
    status: str  # "cataloged" | "skipped" | "failed"
    detail: str = ""


def compute_set_fingerprint(members: list[tuple[int, int]]) -> str:
    """Stable hash of the sorted ``(topic_id, schema_id)`` members."""
    payload = json.dumps(sorted(members), separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def get_fingerprint(path: str) -> tuple[int, int]:
    """Local change-detection fingerprint: ``(size_bytes, mtime_ns)``."""
    st = os.stat(path)
    return st.st_size, st.st_mtime_ns


def synth_etag(size_bytes: int, mtime_ns: int) -> str:
    """A synthetic, never-compared etag for local files."""
    return f"local:{size_bytes}:{mtime_ns}"


def resolve_dimensions(path: str, watched_root: str) -> tuple[dict[str, str], str] | None:
    """Dimensions from the ``s3_key`` metadata, else the relative Hive path.

    Returns ``(dims, key)`` only if the key parses AND round-trips exactly;
    otherwise ``None`` (the caller records an ``catalog_failures`` row).
    """
    key = extract_s3_key(path)
    if key is None:
        key = relpath_key(path, watched_root)
    dims = parse_hive_key(key)
    if dims is None:
        return None
    if rebuild_hive_key(dims) != key.lstrip("/"):
        return None
    return dims, key


def is_error_tag(key: str, value: str) -> bool:
    """Whether a ``(key, value)`` tag marks the recording as errored."""
    return key in {"error", "has_error"} and value.lower() in {"1", "true", "yes"}


def _composite_row(conn, ids, dims):
    return conn.execute(
        f"SELECT id, size_bytes, last_modified_ns FROM files WHERE {_COMPOSITE}",
        (*ids, dims["date"], dims["filename"]),
    ).fetchone()


def catalog_file(
    conn: sqlite3.Connection, caches: Caches, path: str, watched_root: str
) -> CatalogResult:
    """Catalog one MCAP file into the catalog (insert or update)."""
    try:
        size, mtime_ns = get_fingerprint(path)
    except OSError as e:
        # The file vanished (TOCTOU between scan and catalog) — not a real failure;
        # the reconcile deletion sweep removes any stale row. Don't crash or record.
        logger.debug("file vanished before cataloging: %s (%s)", path, e)
        return CatalogResult("failed", "file vanished")

    res = resolve_dimensions(path, watched_root)
    if res is None:
        record_failure(conn, relpath_key(path, watched_root), "unparseable key")
        conn.commit()
        return CatalogResult("failed", "unparseable key")
    dims, key = res

    # Resolve dimension ids; commit so the (append-only) lookup rows persist.
    customer_id = resolve_customer(conn, caches, dims["customer"])
    site_id = resolve_site(conn, caches, customer_id, dims["site"])
    robot_id = resolve_robot(conn, caches, site_id, dims["robot"])
    source_id = resolve_source(conn, caches, dims["source"])
    conn.commit()
    ids = (customer_id, site_id, robot_id, source_id)

    # Fingerprint-skip (read-only): no file read when (size, mtime) are unchanged.
    existing = _composite_row(conn, ids, dims)
    if existing is not None and existing["size_bytes"] == size and existing["last_modified_ns"] == mtime_ns:
        return CatalogResult("skipped")

    # Read the summary OUTSIDE the transaction (slow / can throw).
    try:
        summary = read_file_summary(path)
    except Exception as e:  # noqa: BLE001
        record_failure(conn, key, f"{type(e).__name__}: {e}")
        conn.commit()
        return CatalogResult("failed", str(e))

    try:
        with conn:  # commit on success, rollback on exception
            by_topic: dict[int, tuple[int, int]] = {}  # topic_id -> (schema_id, count)
            for ch in summary.channels:
                topic_id = resolve_topic(conn, caches, ch.topic)
                schema_id = resolve_schema(conn, caches, ch.schema_name, ch.schema_encoding)
                if topic_id in by_topic:  # defensive: no duplicate topics in real data
                    prev_schema, prev_count = by_topic[topic_id]
                    by_topic[topic_id] = (prev_schema, prev_count + ch.message_count)
                    logger.warning("duplicate topic %s in %s", ch.topic, path)
                else:
                    by_topic[topic_id] = (schema_id, ch.message_count)

            members = sorted((tid, sid) for tid, (sid, _) in by_topic.items())
            set_id = resolve_topic_set(conn, caches, compute_set_fingerprint(members), members)

            counts = [by_topic[tid][1] for tid, _ in members]
            if sum(counts) != summary.message_count:
                raise ValueError(
                    f"count mismatch: sum(counts)={sum(counts)} != "
                    f"message_count={summary.message_count}"
                )
            blob = encode_counts_blob(counts)

            conn.execute(
                "INSERT INTO files("
                "filename, etag, size_bytes, last_modified_ns, cataloged_at_ns, "
                "customer_id, site_id, robot_id, source_id, date, "
                "start_time_ns, end_time_ns, topic_set_id, topic_counts, has_error) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(customer_id, site_id, robot_id, source_id, date, filename) "
                "DO UPDATE SET etag=excluded.etag, size_bytes=excluded.size_bytes, "
                "last_modified_ns=excluded.last_modified_ns, cataloged_at_ns=excluded.cataloged_at_ns, "
                "start_time_ns=excluded.start_time_ns, end_time_ns=excluded.end_time_ns, "
                "topic_set_id=excluded.topic_set_id, topic_counts=excluded.topic_counts, "
                "has_error=excluded.has_error",
                (
                    dims["filename"], synth_etag(size, mtime_ns), size, mtime_ns, now_ns(),
                    customer_id, site_id, robot_id, source_id, dims["date"],
                    summary.start_time_ns, summary.end_time_ns, set_id, blob, 0,
                ),
            )
            file_id = conn.execute(
                f"SELECT id FROM files WHERE {_COMPOSITE}",
                (*ids, dims["date"], dims["filename"]),
            ).fetchone()["id"]

            conn.execute("DELETE FROM tags WHERE file_id=?", (file_id,))
            tags = derive_tags(summary)
            if tags:
                conn.executemany(
                    "INSERT INTO tags(file_id, key, value) VALUES (?, ?, ?)",
                    [(file_id, k, v) for k, v in tags],
                )
            has_error = 1 if any(is_error_tag(k, v) for k, v in tags) else 0
            conn.execute("UPDATE files SET has_error=? WHERE id=?", (has_error, file_id))

            conn.execute("DELETE FROM catalog_failures WHERE s3_key=?", (key,))
    except Exception as e:  # noqa: BLE001
        # The rolled-back txn may have inserted topics/schemas/sets that are now
        # gone from the DB — reload caches so they can never reference a missing row.
        caches.__dict__.update(load_caches(conn).__dict__)
        record_failure(conn, key, f"{type(e).__name__}: {e}")
        conn.commit()
        return CatalogResult("failed", str(e))

    return CatalogResult("cataloged")


def delete_by_path(
    conn: sqlite3.Connection, caches: Caches, path: str, watched_root: str
) -> bool:
    """Hard-delete a removed file's row (best-effort; the reconcile sweep is authoritative).

    The file is gone, so dimensions come from the path only and ids are resolved
    cache-only (a missing lookup means the row cannot exist).
    """
    key = relpath_key(path, watched_root)
    dims = parse_hive_key(key)
    if dims is None:
        return False
    customer_id = caches.customer.get(dims["customer"])
    site_id = caches.site.get((customer_id, dims["site"])) if customer_id is not None else None
    robot_id = caches.robot.get((site_id, dims["robot"])) if site_id is not None else None
    source_id = caches.source.get(dims["source"])
    if None in (customer_id, site_id, robot_id, source_id):
        return False
    cur = conn.execute(
        f"DELETE FROM files WHERE {_COMPOSITE}",
        (customer_id, site_id, robot_id, source_id, dims["date"], dims["filename"]),
    )
    conn.execute("DELETE FROM catalog_failures WHERE s3_key=?", (key,))
    conn.commit()
    return cur.rowcount > 0
