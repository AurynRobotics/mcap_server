-- mcap_indexer catalog schema (browse & filter).
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

-- files: one row per MCAP recording. Single-valued facts live here as columns.
CREATE TABLE IF NOT EXISTS files (
    id                INTEGER PRIMARY KEY,        -- internal id; also the keyset-pagination cursor

    filename          TEXT    NOT NULL,           -- key's leaf; the only non-dimension part (essential)

    -- Change-detection fingerprint ("checksum").
    etag              TEXT    NOT NULL,            -- S3 ETag / GCS generation; locally synthesized
    size_bytes        INTEGER NOT NULL,
    last_modified_ns  INTEGER NOT NULL,
    indexed_at_ns     INTEGER NOT NULL,

    -- Path-derived dimensions, as FK ids into the lookup tables below.
    customer_id       INTEGER NOT NULL REFERENCES customers(id),
    site_id           INTEGER NOT NULL REFERENCES sites(id),
    robot_id          INTEGER NOT NULL REFERENCES robots(id),
    source_id         INTEGER NOT NULL REFERENCES sources(id),
    date              TEXT    NOT NULL,           -- the 'date=' partition, e.g. '2026-05-19'

    -- Recording-derived facts (from the MCAP summary/footer).
    start_time_ns     INTEGER NOT NULL,
    end_time_ns       INTEGER NOT NULL,

    -- Topic layout (deduped in topic_sets) + per-file per-topic counts (blob).
    topic_set_id      INTEGER NOT NULL REFERENCES topic_sets(id),
    topic_counts      BLOB    NOT NULL,           -- one varint per set member, ordered by topic_id ASC

    -- Domain flag: materialized predicate; the details live in `tags`.
    has_error         INTEGER NOT NULL DEFAULT 0, -- 0/1

    -- Idempotency key: the parsed components uniquely identify a file.
    UNIQUE (customer_id, site_id, robot_id, source_id, date, filename)
);

CREATE INDEX IF NOT EXISTS idx_files_time  ON files(start_time_ns, end_time_ns);
CREATE INDEX IF NOT EXISTS idx_files_error ON files(id) WHERE has_error = 1;
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

-- tags: open-ended key/value (1:N EAV).
CREATE TABLE IF NOT EXISTS tags (
    file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    key     TEXT    NOT NULL,
    value   TEXT    NOT NULL,
    PRIMARY KEY (file_id, key)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_tags_kv ON tags(key, value);

-- indexer_failures: files we COULD NOT index (keeps the raw key).
CREATE TABLE IF NOT EXISTS indexer_failures (
    s3_key       TEXT    NOT NULL PRIMARY KEY,
    failed_at_ns INTEGER NOT NULL,
    error_text   TEXT    NOT NULL
);
