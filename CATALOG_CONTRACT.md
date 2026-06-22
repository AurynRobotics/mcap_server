# CATALOG_CONTRACT.md — the cross-language SQLite contract

**Status:** LIVE change-control document (catalog-migration plan §1.0). Landed M1
(2026-06-22). This file exists **identically in both repos** — the consumer
(`pj-mcap-server`, the Go reader/streamer) and the producer (`mcap_catalog`, the
Python builder). Keep them in lockstep; a change to one is a change to both.

> The catalog SQLite file is now an **interface between two languages with two
> release cadences** (Python writer, Go reader). This document is the change
> control for that interface. **Any change to a table/column the Go reader reads,
> or to an encoding below, REQUIRES bumping `SCHEMA_VERSION` (see §1) in the same
> change, in both repos.**

---

## 0. The two sides

| Side | Repo / path | Role |
|---|---|---|
| **Writer (sole)** | `mcap_catalog/mcap_catalog_builder` (Python) | The **only** process that writes the catalog. Owns `schema.sql`. |
| **Reader** | `server/internal/catalog` (Go) | Opens the DB **read-only** (`OpenReadOnly`), never writes. Serves the wire RPCs + feeds the streamer. |

The Go streaming subsystem (`internal/session`, `internal/ws` session path,
`internal/format`) is **out of this contract** — it reads object bytes from
storage, not the catalog. The catalog only tells it *which* object key + time
bounds to fetch.

> **Staged cutover (M2 state, 2026-06-22).** The full auryn-schema **reader** is
> built and proven (M2: `auryn_read.go`, reached via the `s.readOnly` branch in
> `FilterFiles`/`GetFile`/`ListTopicsForFile`/`HasHierarchicalKey`), exercised by
> the hermetic `TestAurynReader_Hermetic` + the `crosslang` e2e + a real-bucket
> check. But the live server **still opens the catalog read-write** via
> `catalog.Open` + the Go indexer (`cmd/pj-cloud-server/main.go` is unchanged).
> **The cutover is M6** (flip `main.go` to `OpenReadOnly`, delete the Go indexer,
> migrate the smoke/matrix harness to the Python builder, resolve D2) — kept green
> by landing it WITH the harness migration. Until then, do not point a Go writer
> and the Python builder at the same DB file.

---

## 1. `schema_version` — the interlock

A single-row table pins the contract version. **Both sides fail fast on mismatch.**

```sql
CREATE TABLE IF NOT EXISTS schema_version (
    id      INTEGER PRIMARY KEY CHECK (id = 1),
    version INTEGER NOT NULL
);
```

- **Current value: `2`.**
- History: **v1** (M1) = the cross-language interlock; **v2** (M2) = the
  `tags_embedded`/`tags_override`/`tags_effective` override layer + `files.chunk_count`.
- Writer source of truth: `mcap_catalog_builder/db.py` → `SCHEMA_VERSION = 2`.
  `open_db` stamps a fresh DB and raises `SchemaVersionError` if an existing DB's
  version differs (and refuses a non-auryn DB before any DDL — see `open_db`).
- Reader source of truth: `server/internal/catalog/readonly.go` → `const
  SchemaVersion = 2`. `OpenReadOnly` raises `*SchemaVersionError` when the DB is
  missing the row/table or carries a different version.
- The two constants **MUST be equal**. They are the human-checkable pin; the
  table is the machine-checkable enforcement.

### When to bump (and when NOT to)

**BUMP** (both constants, same PR, in both repos) when you change anything in
§2–§6 below: a column the reader reads is added/removed/renamed/retyped, the
`topic_counts` encoding changes, a `WITHOUT ROWID` ordering assumption changes,
the dimension/`tags` semantics change, or the `s3_key` rebuild rule changes.

**DO NOT bump** for writer-internal changes the reader never observes: a new
index, a `catalog_failures` tweak, builder logging, the (empty, forward-declared)
`file_metrics` tables, or a new column the reader does not (yet) read.

A bump means: old DBs are refused on open. The recovery is to rebuild the
catalog with a matching builder (the catalog is a pure cache of the bucket, so a
rebuild is always safe — see §7).

---

## 2. `files` — the row the reader serves

```sql
CREATE TABLE files (
    id                INTEGER PRIMARY KEY,        -- wire file_id; STABLE across rebuilds (§7)
    filename          TEXT    NOT NULL,           -- key leaf, e.g. "rosbox_0.mcap"
    etag              TEXT    NOT NULL,            -- change-detect fingerprint (writer-internal)
    size_bytes        INTEGER NOT NULL,           -- READER: FileSummary.size_bytes
    last_modified_ns  INTEGER NOT NULL,           -- writer-internal
    cataloged_at_ns   INTEGER NOT NULL,           -- writer-internal
    customer_id       INTEGER NOT NULL,           -- READER: FK -> customers.id (dimension + s3_key rebuild)
    site_id           INTEGER NOT NULL,           -- READER: FK -> sites.id
    robot_id          INTEGER NOT NULL,           -- READER: FK -> robots.id
    source_id         INTEGER NOT NULL,           -- READER: FK -> sources.id
    date              TEXT    NOT NULL,            -- READER: 'date=' partition, e.g. '2026-05-19'; s3_key rebuild
    start_time_ns     INTEGER NOT NULL,           -- READER: FileSummary.recorded.start / flat start_ns
    end_time_ns       INTEGER NOT NULL,           -- READER: FileSummary.recorded.end   / flat end_ns
    chunk_count       INTEGER NOT NULL DEFAULT 0, -- READER: flat chunk_count (MCAP Statistics.chunk_count)
    topic_set_id      INTEGER NOT NULL,           -- READER: -> topic_set_members (topics + topic_count)
    topic_counts      BLOB    NOT NULL,           -- READER: per-topic message counts (§4)
    has_error         INTEGER NOT NULL DEFAULT 0, -- READER: error filter predicate (0/1)
    UNIQUE (customer_id, site_id, robot_id, source_id, date, filename)
);
```

Columns the reader **reads**: `id, filename, size_bytes, customer_id, site_id,
robot_id, source_id, date, start_time_ns, end_time_ns, chunk_count,
topic_set_id, topic_counts`, plus `etag` (read as `FileRecord.S3ETag` for the
session's range-GET If-Match). `has_error` is materialized by the writer but **not
yet read** by the Go reader (reserved for a future error-filter predicate — adding
that read does not need a version bump). The rest (`last_modified_ns,
cataloged_at_ns`) are writer-internal change-detection bookkeeping.

### Derived wire fields → source

| Wire / flat-metadata field | Derivation from the schema |
|---|---|
| `FileSummary.size_bytes` / flat `size_bytes` | `files.size_bytes` |
| `FileSummary.recorded` / flat `start_ns`,`end_ns` | `files.start_time_ns`, `files.end_time_ns` |
| flat `duration_ns` | `end_time_ns - start_time_ns` |
| `FileSummary.topic_count` / flat `topic_count` | `COUNT(*) FROM topic_set_members WHERE set_id = files.topic_set_id` |
| `FileSummary.message_count` / flat `message_count` | `SUM(decode_counts(files.topic_counts))` |
| `FileSummary.s3_key` / flat `s3_key` | **rebuild from dimensions** — see §5 (D1). Go: `rebuildHiveKey` (`auryn_read.go`). |
| flat `chunk_count` | `files.chunk_count` (MCAP `Statistics.chunk_count`; landed M2). |
| `FileSummary.tags` (+ `Tag.is_override`) | `tags_effective` view — override-wins (§6, landed M2). Go: `EffectiveTags`. |
| `GetFileResponse.topics[]` | `topic_set_members` ⋈ `topic_names` ⋈ `schemas` + `topic_counts` (§3,§4). |

---

## 3. Dimensions & dictionaries

Strict hierarchy `customer → site → robot`; `source` is flat. Topic/schema names
are interned.

```sql
customers (id PK, name UNIQUE)
sites     (id PK, customer_id -> customers.id, name, UNIQUE(customer_id,name))
robots    (id PK, site_id -> sites.id,         name, UNIQUE(site_id,name))
sources   (id PK, name UNIQUE)
topic_names (id PK, name UNIQUE)
schemas     (id PK, name, encoding, UNIQUE(name,encoding))
```

Reader reads all of these (the `GetVocabulary` tree comes from
`customers/sites/robots`; `sources` is the flat facet; `topic_names`/`schemas`
resolve `GetFileResponse.topics[]`). Dimension ids are **session-scoped handles**
on the wire (same contract as `file_id`): a client filters using ids from the
same `GetVocabularyResponse` snapshot, so id renumbering across a rebuild is a
non-issue for a live client.

---

## 4. `topic_sets` / `topic_set_members` / `topic_counts` — the dedup + counts

The **set** of channels a file has is stored once; each file points at a set and
stores only its per-topic counts.

```sql
topic_sets (id PK, fingerprint TEXT UNIQUE)      -- sha256 of sorted (topic_id,schema_id) members
topic_set_members (
    set_id    -> topic_sets.id ON DELETE CASCADE,
    topic_id  -> topic_names.id,
    schema_id -> schemas.id,
    PRIMARY KEY (set_id, topic_id)
) WITHOUT ROWID;                                  -- rows ordered by (set_id, topic_id ASC)
```

### `files.topic_counts` encoding (LOAD-BEARING — bump on any change)

- **One unsigned LEB128 varint per topic-set member.**
- **Ordered by `topic_id` ASC** — the *exact same order* as
  `topic_set_members` for that `set_id` under its `WITHOUT ROWID` primary key
  `(set_id, topic_id)`. So decoding zips positionally:
  `i`-th varint ↔ `i`-th member row ordered by `topic_id ASC`.
- A zero-message channel is **present** in the set with a count of `0` (the
  writer reads counts with `.get(channel_id, 0)`).
- `SUM(counts) == files`' MCAP `Statistics.message_count` — the writer asserts
  this in-transaction (a mismatch quarantines the file), so the reader may trust
  it.

Reference codecs that MUST agree:
- Writer: `mcap_catalog_builder/varint.py` (`encode_counts_blob` / `decode_counts_blob`).
- Reader: Go `decode_counts_blob` port (M2 task 2.3 — `GetFile`).

To reconstruct `GetFileResponse.topics[]`: select the member rows for
`files.topic_set_id` **ordered by `topic_id ASC`**, join `topic_names`/`schemas`
for `(name, schema_name, schema_encoding)`, and zip with the decoded
`topic_counts` for each topic's `message_count`.

---

## 5. `s3_key` rebuild (D1)

There is **no stored object-key column**. The reader rebuilds the key from
dimensions, the exact inverse of the writer's `parse_hive_key`:

```
customer=<c>/customer_site=<site>/robot=<r>/source=<s>/date=<d>/<filename>
```

- Writer reference: `mcap_catalog_builder/keyparse.py::rebuild_hive_key`.
- Reader: a Go `rebuildHiveKey` (M2 task 2.4), round-trip-checked against the
  writer's literal. The streamer uses this for range-GETs (`session.FileKeys`).
- The writer **only catalogs keys that round-trip** (`rebuild == key`), so every
  cataloged file's key is reconstructable. If non-Hive production keys ever
  appear, switch to a stored `s3_key` column (D1) **and bump the version**.

---

## 6. `tags_embedded` / `tags_override` / `tags_effective` (landed M2)

The two-layer override model (decision 3), mirroring the Go schema:

```sql
tags_embedded (file_id -> files.id ON DELETE CASCADE, key, value NOT NULL, PRIMARY KEY(file_id,key)) WITHOUT ROWID;
tags_override (file_id -> files.id ON DELETE CASCADE, key, value /*NULL=mask*/, updated_at, PRIMARY KEY(file_id,key)) WITHOUT ROWID;
CREATE VIEW tags_effective AS
  SELECT file_id,key,value,1 AS is_override FROM tags_override WHERE value IS NOT NULL
  UNION ALL
  SELECT e.file_id,e.key,e.value,0 FROM tags_embedded e
  LEFT JOIN tags_override o ON (o.file_id=e.file_id AND o.key=e.key) WHERE o.file_id IS NULL;
```

- **`tags_embedded`** = codec/footer-derived; the builder **REWRITES** it on every
  re-catalog. **`tags_override`** = user edits; the builder **NEVER** touches it
  (override-survives-reindex — the Go smoke "step h" invariant). `db.update_tags()`
  is the **sole** writer of `tags_override`: set ⇒ non-NULL override (wins over
  embedded); unset of an embedded key ⇒ NULL mask (hides it across re-catalog);
  unset of a pure override ⇒ delete.
- **`tags_effective`** = the override-wins merge the reader reads (Go `EffectiveTags`
  + `FilterFiles`/`DistinctMetadataKeys`). `is_override` marks the source layer →
  wire `Tag.is_override`. (Disjoint UNION ALL: the two SELECTs never share a
  `(file_id,key)`, so no dedup is needed.)
- **No embedded-tag extraction is implemented yet:** `derive_tags()` is a stub
  returning `[]`, so `tags_embedded` is empty for the current corpus — but user
  *overrides* work regardless. The intended embedded source, when wired (M5 task
  4.4), is the **MCAP footer `pj.user_tags`** only (no S3/GCS object-metadata/`Head`
  path).
- **D2 (the tag-edit WRITE path) is unresolved.** The wire `UpdateTags` is
  client→server, but the Go server is read-only. `update_tags()` is the Python
  entry point; how a client edit *reaches* it (Go→Python IPC vs a narrow Go-writes
  exception) is decided at the M6 cutover. Until then the Go `UpdateTags` handler
  still writes the legacy Go-schema `tags_override` (live path), which the auryn
  reader does not serve.

---

## 7. `files.id` stability (the rowid contract)

The wire `file_id` **is** `files.id` (a bare `INTEGER PRIMARY KEY` = the SQLite
rowid). Two stability regimes — **do not conflate them**:

- **In-place re-catalog (an incremental builder run): `id` IS preserved.**
  `catalog_object` **upserts by the composite key** `(customer_id, site_id,
  robot_id, source_id, date, filename)` via `ON CONFLICT … DO UPDATE`, so an
  unchanged file keeps its `id` across re-catalogs (it is never
  deleted-then-reinserted). This is the regime the wire `file_id` contract relies
  on — in-flight sessions across a reindex and `source_file_id` in batch bodies.
- **Full DB replace (delete the DB file + rebuild from scratch): ids are
  RENUMBERED.** SQLite reassigns rowids, so `files.id` is NOT stable across a full
  rebuild. The catalog *content* is still correct (it is a pure cache of the
  bucket), but any in-flight `file_id` becomes invalid. A full rebuild MUST
  therefore be paired with a reader restart / client re-`list` — M6 task 6.2a
  (atomic-publish + reader-reopen) formalizes this. Clients normally resolve by
  *name* from a fresh `list`, so a browse→open after a rebuild is unaffected.

If cross-*rebuild* id stability is ever required, switch to deterministic /
content-derived ids (and bump the version).

---

## 8. Status & remaining gaps

**Landed (v2):** all reads in §2–§6 — dimensions, topic-set + `topic_counts`
decode, `s3_key` rebuild, `chunk_count`, the `tags_effective` override layer. The
Go reader (`auryn_read.go`) serves them via the `s.readOnly` branch; the frozen 8
`DerivedMetadataKeys` are all derivable.

**Remaining (forward, not breaking unless noted):**

- **Cutover (M6).** The live server still opens the catalog read-WRITE
  (`catalog.Open` + the Go indexer); the auryn reader is reached only via
  `OpenReadOnly` (the crosslang/hermetic tests today). M6 flips `main.go`, removes
  the Go indexer, migrates the smoke/matrix harness to the Python builder, and
  resolves **D2** (the tag-edit write path, §6).
- **Embedded-tag extraction (M5 task 4.4).** `derive_tags()` is a stub → wire the
  `pj.user_tags` footer source + real `has_error` health.
- **Raw MCAP summary bytes** (durable chunk-index warm) — migration plan §3.4,
  deferred; additive, reader-optional.

### The frozen 8 `DerivedMetadataKeys` (client-ingest contract — `caps.go`)

`s3_key, size_bytes, message_count, topic_count, chunk_count, duration_ns,
start_ns, end_ns`. All 8 MUST remain derivable from this schema — and are, as of
v2. Audit this list before any sign-off.

---

## 9. Open/connection invariants

- Writer DSN (`db.py`): `journal_mode=WAL`, `foreign_keys=ON`,
  `busy_timeout=5000`. `PRAGMA synchronous` is **not set today** — it is at the
  SQLite default. M5 task 4.3 will set it consciously (and document the agreed
  value) in **both** the writer and the reader DSN.
- Reader DSN (`readonly.go`): `mode=ro`, `busy_timeout=5000`, `foreign_keys=ON`.
  The reader **never** applies a schema (the writer's `schema.sql` is
  authoritative; applying Go's own `CREATE TABLE` would silently diverge the
  `files` definition). Note `mode=ro` (not `immutable=1`) still attaches the
  WAL/`-shm` sidecars, so the DB's **directory must be writable** even though the
  reader never writes the DB itself — a strictly read-only mount would fail to
  open.
- **Atomic publish / reopen (M6 task 6.2a):** when the builder rebuilds the DB
  underneath a live reader it must build to a temp file then atomically rename;
  the reader detects the swap and reopens. Until that lands, treat a rebuild as
  requiring a reader restart.
