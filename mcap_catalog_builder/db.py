"""SQLite connection, schema init, in-memory id caches, and id resolvers.

The daemon is the single writer; all DB access is serialized through one worker
thread draining a queue, so ``check_same_thread=False`` is safe here (the
connection is created on the main thread but used from the worker).

Resolvers use ``INSERT OR IGNORE`` then ``SELECT id`` (never ``lastrowid`` after
``OR IGNORE``), and do NOT commit — the caller owns the transaction.
"""

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"

# SCHEMA_VERSION pins the cross-language catalog contract (see CATALOG_CONTRACT.md):
# the single integer the Python writer and the Go reader must agree on. The builder
# writes it into the schema_version table; the Go reader and open_db below both fail
# fast on mismatch. BUMP THIS on any change to a table/column the Go reader reads
# (the contract doc enumerates them) — never on a writer-internal change.
#
# v1 -> v2 (M2): tags -> tags_embedded + tags_override + tags_effective view (the
#               override-survives-reindex model); files.chunk_count column.
# v2 -> v3 (M6): build_metadata table (catalog-freshness / swap-detection §6.5/§6.2a).
SCHEMA_VERSION = 3

# BUILDER_VERSION stamps build_metadata.builder_version so an operator can see which
# builder wrote the catalog. Bump on a builder release (independent of SCHEMA_VERSION).
BUILDER_VERSION = "0.1.0"


class SchemaVersionError(RuntimeError):
    """An existing catalog DB was written under an incompatible schema version.

    Recovery is to rebuild the catalog (delete the DB so it is re-created at the
    current version) or run a builder whose SCHEMA_VERSION matches the DB.
    """


def open_db(path: str) -> sqlite3.Connection:
    """Open (creating/upgrading) the catalog DB with WAL + FK + busy_timeout.

    After applying the schema, pins/validates the cross-language ``schema_version``
    row: a fresh DB is stamped with ``SCHEMA_VERSION``; an existing DB whose version
    differs raises ``SchemaVersionError`` (fail fast — never silently write the wrong
    shape under a stale reader, or read a stale shape under a new writer).
    """
    # isolation_level="" pins legacy deferred-transaction control, so `with conn:`
    # reliably commits on success / rolls back on exception across Python versions.
    conn = sqlite3.connect(path, check_same_thread=False, isolation_level="")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    # synchronous=NORMAL under WAL: durable across an app crash, only risking the
    # last transaction on a power loss / OS crash. The catalog is a rebuildable
    # cache of the bucket (a lost write re-extracts on the next reconcile), so the
    # throughput win over FULL is worth it. The Go reader is read-only, so its DSN
    # does not set synchronous (it never writes). (catalog-migration §4.3)
    conn.execute("PRAGMA synchronous = NORMAL")
    # Two guards BEFORE any DDL, so a wrong DB is NEVER mutated:
    #  (1) a DB with tables but no schema_version (legacy Go / foreign catalog) is
    #      refused — applying schema.sql would create+stamp it and defeat the
    #      cross-language interlock;
    #  (2) an EXISTING auryn DB whose version differs is refused before executescript
    #      — otherwise the v2 schema would be committed into a v1 DB, leaving it
    #      polluted with v2 tables yet stamped v1 (the version gate must precede DDL).
    _guard_not_foreign_db(conn, path)
    _validate_existing_version(conn, path)
    conn.executescript(_SCHEMA_PATH.read_text())
    conn.commit()
    _stamp_schema_version(conn)
    return conn


def _guard_not_foreign_db(conn: sqlite3.Connection, path: str) -> None:
    """Refuse to write a DB that has tables but is not an auryn catalog.

    A fresh DB (no user tables) and an auryn DB (already has ``schema_version``)
    both pass; a legacy/foreign DB fails fast — see the call site for why this MUST
    run before any schema DDL.
    """
    tables = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }
    if tables and "schema_version" not in tables:
        raise SchemaVersionError(
            f"{path!r} has tables but no schema_version — not an auryn-built catalog "
            f"(a legacy Go-written or foreign DB?). Refusing to write it. Use a fresh "
            f"path or delete the DB."
        )


def _validate_existing_version(conn: sqlite3.Connection, path: str) -> None:
    """Pre-DDL: refuse an EXISTING auryn DB whose version differs, before any schema
    is applied — so a stale-version catalog is never mutated. A fresh DB (no
    schema_version table) or an unstamped one passes through to be stamped."""
    has_table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_version'"
    ).fetchone()
    if has_table is None:
        return  # fresh DB (the foreign case was already refused above)
    row = conn.execute("SELECT version FROM schema_version WHERE id = 1").fetchone()
    if row is not None and row["version"] != SCHEMA_VERSION:
        raise SchemaVersionError(
            f"catalog schema_version mismatch: {path!r} was written with version "
            f"{row['version']}, but this builder writes version {SCHEMA_VERSION}. No "
            f"schema was applied. Rebuild the catalog (delete the DB) or use a "
            f"matching builder."
        )


def _stamp_schema_version(conn: sqlite3.Connection) -> None:
    """Post-DDL: stamp a fresh DB with ``SCHEMA_VERSION`` (race-safe), then re-validate.

    ``INSERT OR IGNORE`` wins-or-noops if two openers race a fresh DB; the
    authoritative row is then re-read and compared (a mismatch was already caught
    pre-DDL by _validate_existing_version, but this is defense in depth).
    """
    conn.execute(
        "INSERT OR IGNORE INTO schema_version(id, version) VALUES (1, ?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    row = conn.execute("SELECT version FROM schema_version WHERE id = 1").fetchone()
    if row is None:  # unreachable after INSERT OR IGNORE, but never silently pass
        raise SchemaVersionError("schema_version row missing after stamp (corrupt DB?)")
    if row["version"] != SCHEMA_VERSION:
        raise SchemaVersionError(
            f"catalog schema_version mismatch: DB was written with version "
            f"{row['version']}, but this builder writes version {SCHEMA_VERSION}. "
            f"Rebuild the catalog (delete the DB) or use a matching builder."
        )


def now_ns() -> int:
    return time.time_ns()


@dataclass
class Caches:
    customer: dict[str, int] = field(default_factory=dict)
    site: dict[tuple[int, str], int] = field(default_factory=dict)    # (customer_id, name) -> id
    robot: dict[tuple[int, str], int] = field(default_factory=dict)   # (site_id, name) -> id
    source: dict[str, int] = field(default_factory=dict)
    topic: dict[str, int] = field(default_factory=dict)
    schema: dict[tuple[str, str], int] = field(default_factory=dict)  # (name, encoding) -> id
    topic_set: dict[str, int] = field(default_factory=dict)           # fingerprint -> id


def load_caches(conn: sqlite3.Connection) -> Caches:
    """Populate all in-memory caches from the current DB contents."""
    c = Caches()
    for r in conn.execute("SELECT id, name FROM customers"):
        c.customer[r["name"]] = r["id"]
    for r in conn.execute("SELECT id, customer_id, name FROM sites"):
        c.site[(r["customer_id"], r["name"])] = r["id"]
    for r in conn.execute("SELECT id, site_id, name FROM robots"):
        c.robot[(r["site_id"], r["name"])] = r["id"]
    for r in conn.execute("SELECT id, name FROM sources"):
        c.source[r["name"]] = r["id"]
    for r in conn.execute("SELECT id, name FROM topic_names"):
        c.topic[r["name"]] = r["id"]
    for r in conn.execute("SELECT id, name, encoding FROM schemas"):
        c.schema[(r["name"], r["encoding"])] = r["id"]
    for r in conn.execute("SELECT id, fingerprint FROM topic_sets"):
        c.topic_set[r["fingerprint"]] = r["id"]
    return c


def _resolve_named(conn, cache, table, name) -> int:
    if name in cache:
        return cache[name]
    conn.execute(f"INSERT OR IGNORE INTO {table}(name) VALUES (?)", (name,))
    cache[name] = conn.execute(f"SELECT id FROM {table} WHERE name = ?", (name,)).fetchone()["id"]
    return cache[name]


def resolve_customer(conn, caches: Caches, name: str) -> int:
    return _resolve_named(conn, caches.customer, "customers", name)


def resolve_source(conn, caches: Caches, name: str) -> int:
    return _resolve_named(conn, caches.source, "sources", name)


def resolve_topic(conn, caches: Caches, name: str) -> int:
    return _resolve_named(conn, caches.topic, "topic_names", name)


def resolve_site(conn, caches: Caches, customer_id: int, name: str) -> int:
    key = (customer_id, name)
    if key in caches.site:
        return caches.site[key]
    conn.execute(
        "INSERT OR IGNORE INTO sites(customer_id, name) VALUES (?, ?)", (customer_id, name)
    )
    caches.site[key] = conn.execute(
        "SELECT id FROM sites WHERE customer_id=? AND name=?", (customer_id, name)
    ).fetchone()["id"]
    return caches.site[key]


def resolve_robot(conn, caches: Caches, site_id: int, name: str) -> int:
    key = (site_id, name)
    if key in caches.robot:
        return caches.robot[key]
    conn.execute("INSERT OR IGNORE INTO robots(site_id, name) VALUES (?, ?)", (site_id, name))
    caches.robot[key] = conn.execute(
        "SELECT id FROM robots WHERE site_id=? AND name=?", (site_id, name)
    ).fetchone()["id"]
    return caches.robot[key]


def resolve_schema(conn, caches: Caches, name: str, encoding: str) -> int:
    key = (name, encoding)
    if key in caches.schema:
        return caches.schema[key]
    conn.execute("INSERT OR IGNORE INTO schemas(name, encoding) VALUES (?, ?)", (name, encoding))
    caches.schema[key] = conn.execute(
        "SELECT id FROM schemas WHERE name=? AND encoding=?", (name, encoding)
    ).fetchone()["id"]
    return caches.schema[key]


def resolve_topic_set(
    conn, caches: Caches, fingerprint: str, members: list[tuple[int, int]]
) -> int:
    """Resolve a topic set by fingerprint; insert its members only on first insert.

    ``members`` must already be sorted by ``topic_id`` ASC (the counts blob is
    aligned to this order).
    """
    if fingerprint in caches.topic_set:
        return caches.topic_set[fingerprint]
    row = conn.execute(
        "SELECT id FROM topic_sets WHERE fingerprint = ?", (fingerprint,)
    ).fetchone()
    if row is not None:
        caches.topic_set[fingerprint] = row["id"]
        return row["id"]
    if list(members) != sorted(members):
        raise ValueError("topic_set members must be sorted by topic_id ASC")
    conn.execute("INSERT INTO topic_sets(fingerprint) VALUES (?)", (fingerprint,))
    set_id = conn.execute(
        "SELECT id FROM topic_sets WHERE fingerprint = ?", (fingerprint,)
    ).fetchone()["id"]
    conn.executemany(
        "INSERT INTO topic_set_members(set_id, topic_id, schema_id) VALUES (?, ?, ?)",
        [(set_id, tid, sid) for tid, sid in members],
    )
    caches.topic_set[fingerprint] = set_id
    return set_id


def update_tags(
    conn: sqlite3.Connection,
    file_id: int,
    set_kv: dict[str, str] | None = None,
    unset_keys: list[str] | None = None,
) -> None:
    """Apply user tag edits to ``tags_override`` (the ONLY writer of that table).

    Mirrors the Go override model (SetOverride / MaskEmbedded / UnsetOverride):

    - **set** ``key=value`` → upsert a non-NULL override row (override wins over any
      embedded value for that key).
    - **unset** ``key`` → if the file has an *embedded* tag with that key, write a
      NULL-valued override row to MASK it (so a re-catalog that re-derives the
      embedded tag stays hidden); otherwise just DELETE any override row.

    The builder never touches ``tags_override``, so these edits survive re-catalog.
    Commits on success (single-writer model). ``set`` is applied before ``unset``.
    """
    set_kv = set_kv or {}
    unset_keys = unset_keys or []
    now = now_ns()
    with conn:  # one transaction: commit on success, rollback on exception
        for key, value in set_kv.items():
            conn.execute(
                "INSERT INTO tags_override(file_id, key, value, updated_at) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT(file_id, key) DO UPDATE SET "
                "value=excluded.value, updated_at=excluded.updated_at",
                (file_id, key, value, now),
            )
        for key in unset_keys:
            has_embedded = conn.execute(
                "SELECT 1 FROM tags_embedded WHERE file_id=? AND key=?", (file_id, key)
            ).fetchone()
            if has_embedded is not None:
                # Mask the embedded tag with a NULL override (survives re-catalog).
                conn.execute(
                    "INSERT INTO tags_override(file_id, key, value, updated_at) "
                    "VALUES (?, ?, NULL, ?) "
                    "ON CONFLICT(file_id, key) DO UPDATE SET value=NULL, updated_at=excluded.updated_at",
                    (file_id, key, now),
                )
            else:
                conn.execute(
                    "DELETE FROM tags_override WHERE file_id=? AND key=?", (file_id, key)
                )


def record_build(
    conn: sqlite3.Connection,
    *,
    files_scanned: int,
    files_failed: int,
    outcome: str = "ok",
    version: str = BUILDER_VERSION,
) -> None:
    """Stamp build_metadata at the end of a reconcile (catalog freshness, §6.5).

    Upserts the single row, bumping ``build_id`` monotonically so a read-only
    reader can detect a new build (§6.2a). Commits.
    """
    conn.execute(
        "INSERT INTO build_metadata(id, build_id, last_build_ns, files_scanned, "
        "files_failed, build_outcome, builder_version) VALUES (1, 1, ?, ?, ?, ?, ?) "
        "ON CONFLICT(id) DO UPDATE SET "
        "build_id = build_metadata.build_id + 1, last_build_ns = excluded.last_build_ns, "
        "files_scanned = excluded.files_scanned, files_failed = excluded.files_failed, "
        "build_outcome = excluded.build_outcome, builder_version = excluded.builder_version",
        (now_ns(), files_scanned, files_failed, outcome, version),
    )
    conn.commit()


def record_failure(conn, key: str, error_text: str) -> None:
    """Upsert an ``catalog_failures`` row (keyed by the raw object key)."""
    conn.execute(
        "INSERT INTO catalog_failures(s3_key, failed_at_ns, error_text) VALUES (?, ?, ?) "
        "ON CONFLICT(s3_key) DO UPDATE SET "
        "failed_at_ns=excluded.failed_at_ns, error_text=excluded.error_text",
        (key, now_ns(), error_text),
    )
