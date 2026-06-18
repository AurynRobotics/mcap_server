"""The catalog builder core: dimension resolution + the §8 per-file transaction.

The core (``catalog_object`` / ``delete_by_key``) is **backend-agnostic**: it
talks to a storage ``Source`` (local FS or S3), never to ``os``/``open`` directly.
``catalog_file`` / ``delete_by_path`` are thin local-filesystem wrappers kept for
the watcher path and the existing tests.

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
from .mcap_summary import derive_tags, summary_from_stream
from .storage import LocalSource, local_etag
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
    """A synthetic etag for local files (canonical form lives in ``storage``)."""
    return local_etag(size_bytes, mtime_ns)


def resolve_key_dims(key: str, source) -> tuple[dict[str, str], str] | None:
    """Resolve a key to ``(dims, effective_key)`` via the source.

    An in-file override (``source.intended_key`` — an ``s3_key`` metadata record
    for local files; always ``None`` for S3) wins over the passed key. The result
    is trusted only if it parses AND round-trips exactly; otherwise ``None``.
    """
    eff = source.intended_key(key)
    if eff is None:
        eff = key
    dims = parse_hive_key(eff)
    if dims is None:
        return None
    if rebuild_hive_key(dims) != eff.lstrip("/"):
        return None
    return dims, eff


def resolve_dimensions(path: str, watched_root: str) -> tuple[dict[str, str], str] | None:
    """Backward-compatible local resolver: dimensions from ``s3_key`` else the path."""
    source = LocalSource(watched_root)
    return resolve_key_dims(relpath_key(path, watched_root), source)


def is_error_tag(key: str, value: str) -> bool:
    """Whether a ``(key, value)`` tag marks the recording as errored."""
    return key in {"error", "has_error"} and value.lower() in {"1", "true", "yes"}


def _composite_row(conn, ids, dims):
    return conn.execute(
        f"SELECT id, etag FROM files WHERE {_COMPOSITE}",
        (*ids, dims["date"], dims["filename"]),
    ).fetchone()


def catalog_object(
    conn: sqlite3.Connection, caches: Caches, key: str, source
) -> CatalogResult:
    """Catalog one MCAP object (insert or update) reading bytes via ``source``."""
    st = source.stat(key)
    if st is None:
        # Vanished between listing and catalog (TOCTOU) — not a real failure; the
        # reconcile deletion sweep removes any stale row. Don't crash or record.
        logger.debug("object vanished before cataloging: %s", key)
        return CatalogResult("failed", "vanished")

    res = resolve_key_dims(key, source)
    if res is None:
        record_failure(conn, key, "unparseable key")
        conn.commit()
        return CatalogResult("failed", "unparseable key")
    dims, eff_key = res

    # Resolve dimension ids; commit so the (append-only) lookup rows persist.
    customer_id = resolve_customer(conn, caches, dims["customer"])
    site_id = resolve_site(conn, caches, customer_id, dims["site"])
    robot_id = resolve_robot(conn, caches, site_id, dims["robot"])
    source_id = resolve_source(conn, caches, dims["source"])
    conn.commit()
    ids = (customer_id, site_id, robot_id, source_id)

    # Fingerprint-skip (read-only): no body read when the etag is unchanged (R4).
    existing = _composite_row(conn, ids, dims)
    if existing is not None and existing["etag"] == st.etag:
        return CatalogResult("skipped")

    # Read the summary OUTSIDE the transaction (slow / can throw). Only the
    # footer + summary are fetched — never the message body (R2).
    try:
        with source.open_summary(key, st.size) as stream:
            summary = summary_from_stream(stream)
    except Exception as e:  # noqa: BLE001
        record_failure(conn, eff_key, f"{type(e).__name__}: {e}")
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
                    logger.warning("duplicate topic %s in %s", ch.topic, key)
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
                    dims["filename"], st.etag, st.size, st.mtime_ns, now_ns(),
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

            conn.execute("DELETE FROM catalog_failures WHERE s3_key=?", (eff_key,))
    except Exception as e:  # noqa: BLE001
        # The rolled-back txn may have inserted topics/schemas/sets that are now
        # gone from the DB — reload caches so they can never reference a missing row.
        caches.__dict__.update(load_caches(conn).__dict__)
        record_failure(conn, eff_key, f"{type(e).__name__}: {e}")
        conn.commit()
        return CatalogResult("failed", str(e))

    return CatalogResult("cataloged")


def catalog_file(
    conn: sqlite3.Connection, caches: Caches, path: str, watched_root: str
) -> CatalogResult:
    """Local-filesystem entry point: catalog the file at ``path`` under ``watched_root``."""
    source = LocalSource(watched_root)
    return catalog_object(conn, caches, source.event_key(path), source)


def delete_by_key(conn: sqlite3.Connection, caches: Caches, key: str) -> bool:
    """Hard-delete a removed object's row (best-effort; the reconcile sweep is authoritative).

    Dimensions come from the key only and ids are resolved cache-only (a missing
    lookup means the row cannot exist).
    """
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


def delete_by_path(
    conn: sqlite3.Connection, caches: Caches, path: str, watched_root: str
) -> bool:
    """Local-filesystem entry point: delete the row for the (now-gone) ``path``."""
    return delete_by_key(conn, caches, relpath_key(path, watched_root))
