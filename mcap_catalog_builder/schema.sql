-- mcap_catalog_builder catalog schema (browse & filter).
--
-- Verbatim copy of §3 of 2026-06-15-catalog-sqlite-schema.md, made idempotent
-- with CREATE TABLE/INDEX IF NOT EXISTS so executescript() is re-runnable.
--
-- PRAGMAs (journal_mode=WAL, foreign_keys=ON, busy_timeout) are authoritative in
-- db.py and set per-connection — they are intentionally NOT embedded here.
--
-- NB: `files` references the lookup / `topic_sets` tables defined further down.
-- SQLite allows forward FK references (the target need only exist at row-write
-- time), so the create order below is fine as written.

-- schema_version: single-row table pinning the cross-language catalog contract
-- version (see CATALOG_CONTRACT.md). The Python builder writes it; the Go reader
-- and Python open_db both FAIL FAST on mismatch, so a DB written by an
-- incompatible builder/reader can never be silently mis-served. The version
-- integer is NOT seeded here — it lives in db.py (SCHEMA_VERSION), the single
-- source of truth for the writer. CHECK(id=1) makes the table structurally
-- single-row. Bump SCHEMA_VERSION on ANY change to a table/column the Go reader
-- reads.
CREATE TABLE IF NOT EXISTS schema_version (
    id      INTEGER PRIMARY KEY CHECK (id = 1),
    version INTEGER NOT NULL
);

-- build_metadata: a single row the builder stamps at the END of each reconcile so
-- the read-only Go server can report catalog FRESHNESS (last build time, scanned/
-- failed counts, outcome, builder version) — replacing the in-process indexer-run
-- signals orphaned by moving the writer out of process (§6.5). `build_id` is a
-- monotonic counter that bumps on every completed build (swap-detection, §6.2a).
CREATE TABLE IF NOT EXISTS build_metadata (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    build_id        INTEGER NOT NULL,           -- monotonic; +1 per completed build
    last_build_ns   INTEGER NOT NULL,           -- when the last build completed
    files_scanned   INTEGER NOT NULL,           -- cataloged + skipped (objects seen)
    files_failed    INTEGER NOT NULL,           -- objects quarantined this build
    build_outcome   TEXT    NOT NULL,           -- 'ok' | 'partial'
    builder_version TEXT    NOT NULL
);

-- files: one row per MCAP recording. Single-valued facts live here as columns.
CREATE TABLE IF NOT EXISTS files (
    id                INTEGER PRIMARY KEY,        -- internal id; also the keyset-pagination cursor

    filename          TEXT    NOT NULL,           -- key's leaf; the only non-dimension part (essential)

    -- Change-detection fingerprint ("checksum").
    etag              TEXT    NOT NULL,            -- S3 ETag / GCS generation; locally synthesized
    size_bytes        INTEGER NOT NULL,
    last_modified_ns  INTEGER NOT NULL,
    cataloged_at_ns     INTEGER NOT NULL,

    -- Path-derived dimensions, as FK ids into the lookup tables below.
    customer_id       INTEGER NOT NULL REFERENCES customers(id),
    site_id           INTEGER NOT NULL REFERENCES sites(id),
    robot_id          INTEGER NOT NULL REFERENCES robots(id),
    source_id         INTEGER NOT NULL REFERENCES sources(id),
    date              TEXT    NOT NULL,           -- the 'date=' partition, e.g. '2026-05-19'

    -- Recording-derived facts (from the MCAP summary/footer).
    start_time_ns     INTEGER NOT NULL,
    end_time_ns       INTEGER NOT NULL,
    chunk_count       INTEGER NOT NULL DEFAULT 0, -- MCAP Statistics.chunk_count; Go reader's flat 'chunk_count'

    -- Topic layout (deduped in topic_sets) + per-file per-topic counts (blob).
    topic_set_id      INTEGER NOT NULL REFERENCES topic_sets(id),
    topic_counts      BLOB    NOT NULL,           -- one varint per set member, ordered by topic_id ASC

    -- Domain flag: materialized predicate; the details live in `tags_embedded`.
    has_error         INTEGER NOT NULL DEFAULT 0, -- 0/1

    -- Idempotency key: the parsed components uniquely identify a file.
    UNIQUE (customer_id, site_id, robot_id, source_id, date, filename)
);

CREATE INDEX IF NOT EXISTS idx_files_time  ON files(start_time_ns, end_time_ns);
-- idx_files_error: global "list ALL files that failed validation", ordered by id.
CREATE INDEX IF NOT EXISTS idx_files_error ON files(id) WHERE has_error = 1;
-- idx_files_cust_err: customer/site-SCOPED error queries (keyset-native, no sort).
-- Complements idx_files_error, which would otherwise force a scan across all
-- customers' errors for a scoped query (measured: 8.8 ms -> 0.066 ms at 1M files).
CREATE INDEX IF NOT EXISTS idx_files_cust_err ON files(customer_id, site_id, id) WHERE has_error = 1;
CREATE INDEX IF NOT EXISTS idx_files_set   ON files(topic_set_id, id);

-- Dimension lookups (hierarchical: site→customer, robot→site; sources flat).
CREATE TABLE IF NOT EXISTS customers (
    id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS sites (
    id          INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    name        TEXT    NOT NULL,
    UNIQUE (customer_id, name)
);
CREATE TABLE IF NOT EXISTS robots (
    id      INTEGER PRIMARY KEY,
    site_id INTEGER NOT NULL REFERENCES sites(id),
    name    TEXT    NOT NULL,
    UNIQUE (site_id, name)
);
CREATE TABLE IF NOT EXISTS sources (
    id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

-- Dictionaries (stable integer identity for topics & schemas).
CREATE TABLE IF NOT EXISTS topic_names (
    id   INTEGER PRIMARY KEY,
    name TEXT    NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS schemas (
    id       INTEGER PRIMARY KEY,
    name     TEXT NOT NULL,
    encoding TEXT NOT NULL,
    UNIQUE (name, encoding)
);

-- topic_sets + topic_set_members: the SET of channels a file contains, DEDUPED.
CREATE TABLE IF NOT EXISTS topic_sets (
    id          INTEGER PRIMARY KEY,
    fingerprint TEXT    NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS topic_set_members (
    set_id    INTEGER NOT NULL REFERENCES topic_sets(id) ON DELETE CASCADE,
    topic_id  INTEGER NOT NULL REFERENCES topic_names(id),
    schema_id INTEGER NOT NULL REFERENCES schemas(id),
    PRIMARY KEY (set_id, topic_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_tsm_topic ON topic_set_members(topic_id);

-- Two-layer tags (mirrors the Go override model the migration ports in):
--   tags_embedded  = codec/footer-derived; REWRITTEN on every re-catalog.
--   tags_override  = user edits; NEVER touched by the builder (override-survives-reindex).
--   tags_effective = override-wins merge VIEW (a NULL override masks the embedded tag).
-- The builder writes only tags_embedded; update_tags() (db.py) is the sole writer of
-- tags_override.
CREATE TABLE IF NOT EXISTS tags_embedded (
    file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    key     TEXT    NOT NULL,
    value   TEXT    NOT NULL,
    PRIMARY KEY (file_id, key)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_tags_embedded_kv ON tags_embedded(key, value);

CREATE TABLE IF NOT EXISTS tags_override (
    file_id    INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    key        TEXT    NOT NULL,
    value      TEXT,                              -- NULL => mask (hide) the embedded tag
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (file_id, key)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_tags_override_kv ON tags_override(key, value);

-- tags_effective: a non-NULL override wins; an override with NULL value hides the
-- embedded tag; an embedded tag with no override shows through. is_override marks
-- the source layer (1 = override, 0 = embedded).
CREATE VIEW IF NOT EXISTS tags_effective AS
SELECT file_id, key, value, 1 AS is_override
FROM tags_override
WHERE value IS NOT NULL
UNION ALL
SELECT e.file_id, e.key, e.value, 0 AS is_override
FROM tags_embedded e
LEFT JOIN tags_override o ON (o.file_id = e.file_id AND o.key = e.key)
WHERE o.file_id IS NULL;

-- catalog_failures: files we COULD NOT catalog (keeps the raw key).
CREATE TABLE IF NOT EXISTS catalog_failures (
    s3_key       TEXT    NOT NULL PRIMARY KEY,
    failed_at_ns INTEGER NOT NULL,
    error_text   TEXT    NOT NULL
);

-- ─────────────────────────────────────────────────────────────────────────────
-- PLANNED — NOT YET POPULATED: derived per-signal metrics (REQUIREMENTS.md R11-R13).
--
-- A separate, content-aware extraction pass — distinct from the metadata catalog builder,
-- which never reads payloads (R2) — reads the ~10% of files carrying queryable
-- numeric data and caches per-signal aggregates here, so a threshold query
-- ("signal > X") is answered from the catalog for ANY X without re-reading files.
-- These tables are forward-declared so the query server can build against them;
-- the current catalog builder writes to NEITHER of them.

-- file_metrics: cached per-(file, signal, field) aggregates.
CREATE TABLE IF NOT EXISTS file_metrics (
    file_id  INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    topic_id INTEGER NOT NULL REFERENCES topic_names(id),
    field    TEXT    NOT NULL,          -- numeric field path, e.g. 'linear.x'
    stat     TEXT    NOT NULL,          -- 'min' | 'max' | 'mean' | 'p99' | ...
    value    REAL    NOT NULL,
    etag     TEXT    NOT NULL,          -- file fingerprint these were computed for (R12)
    PRIMARY KEY (file_id, topic_id, field, stat)
) WITHOUT ROWID;
-- Drives "signal > X" as an cataloged range scan. Paginate this query class by
-- `value` (the cursor that matches this index), NOT by files.id — a files.id
-- cursor would force a sort over the whole match set (measured: 55x slower).
CREATE INDEX IF NOT EXISTS idx_metrics_q ON file_metrics(topic_id, field, stat, value);

-- file_metric_status: per-file extraction bookkeeping — skip the ~90% with no
-- numeric data, recompute the rest only when the fingerprint changes (R13).
CREATE TABLE IF NOT EXISTS file_metric_status (
    file_id           INTEGER NOT NULL PRIMARY KEY REFERENCES files(id) ON DELETE CASCADE,
    has_numeric       INTEGER NOT NULL,  -- 0/1: does this file carry queryable numeric data
    computed_for_etag TEXT               -- fingerprint metrics were last computed for; NULL = pending
) WITHOUT ROWID;
