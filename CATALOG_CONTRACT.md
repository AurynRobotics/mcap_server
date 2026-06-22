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

> **Staged cutover (M1 state, 2026-06-22).** `OpenReadOnly` and this contract
> landed in M1, but the live server still opens the catalog **read-write** via
> `catalog.Open` (`cmd/pj-cloud-server/main.go`) — the Go indexer remains the
> writer until **M2**, which is the read-only reader rewrite. So today both a Go
> writer and the Python builder *can* target a DB; **do not point them at the same
> file yet.** The reader path proven in M1 is the `crosslang` test, not production
> wiring.

---

## 1. `schema_version` — the interlock

A single-row table pins the contract version. **Both sides fail fast on mismatch.**

```sql
CREATE TABLE IF NOT EXISTS schema_version (
    id      INTEGER PRIMARY KEY CHECK (id = 1),
    version INTEGER NOT NULL
);
```

- **Current value: `1`.**
- Writer source of truth: `mcap_catalog_builder/db.py` → `SCHEMA_VERSION = 1`.
  `open_db` stamps a fresh DB and raises `SchemaVersionError` if an existing DB's
  version differs.
- Reader source of truth: `server/internal/catalog/readonly.go` → `const
  SchemaVersion = 1`. `OpenReadOnly` raises `*SchemaVersionError` when the DB is
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
    topic_set_id      INTEGER NOT NULL,           -- READER: -> topic_set_members (topics + topic_count)
    topic_counts      BLOB    NOT NULL,           -- READER: per-topic message counts (§4)
    has_error         INTEGER NOT NULL DEFAULT 0, -- READER: error filter predicate (0/1)
    UNIQUE (customer_id, site_id, robot_id, source_id, date, filename)
);
```

Columns the reader **reads**: `id, filename, size_bytes, customer_id, site_id,
robot_id, source_id, date, start_time_ns, end_time_ns, topic_set_id,
topic_counts, has_error`. The rest (`etag, last_modified_ns, cataloged_at_ns`)
are writer-internal change-detection bookkeeping the reader ignores.

### Derived wire fields → source

| Wire / flat-metadata field | Derivation from the schema |
|---|---|
| `FileSummary.size_bytes` / flat `size_bytes` | `files.size_bytes` |
| `FileSummary.recorded` / flat `start_ns`,`end_ns` | `files.start_time_ns`, `files.end_time_ns` |
| flat `duration_ns` | `end_time_ns - start_time_ns` |
| `FileSummary.topic_count` / flat `topic_count` | `COUNT(*) FROM topic_set_members WHERE set_id = files.topic_set_id` |
| `FileSummary.message_count` / flat `message_count` | `SUM(decode_counts(files.topic_counts))` |
| `FileSummary.s3_key` / flat `s3_key` | **rebuild from dimensions** — see §5 (D1). |
| flat `chunk_count` | `files.chunk_count` — **NOT YET A COLUMN. M2 task 2.2 adds it** (see §8). |
| `FileSummary.tags` (+ `Tag.is_override`) | `tags` today; `tags_effective` after the M2 override port (§6). |
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

## 6. `tags` / `tags_effective` semantics

**M1 (today):** open-ended EAV, writer-derived.

```sql
tags (file_id -> files.id ON DELETE CASCADE, key, value, PRIMARY KEY(file_id,key)) WITHOUT ROWID;
```

**No tag extraction is implemented yet.** `derive_tags()` is a stub that returns
`[]` (there is no `pj.user_tags` footer reader today), so the `tags` table is
empty for the current corpus. The INTENDED source, when wired (M5 task 4.4 — real
validation-health tags), is the **MCAP footer `pj.user_tags`** only — there is no
S3/GCS object-custom-metadata / `Head` path planned. On the wire each tag is a
`Tag{key, value, is_override=false}`.

**M2 (planned, decision 3 — the override port):** add `tags_override(file_id,
key, value NULLABLE, updated_at)` + a `tags_effective` override-wins view (NULL
masks the embedded tag), and rename `tags` → `tags_embedded`. The builder writes
embedded tags and **never touches `tags_override`** on re-catalog (the
override-survives-reindex invariant). The reader then reads `tags_effective` and
sets `Tag.is_override` accordingly. **That change bumps the version.**

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

## 8. Known gaps the reader/writer must close (forward, not breaking)

These are **planned additions**; each that the reader reads will bump the version
when it lands:

- **`files.chunk_count`** (MCAP `Statistics.chunk_count`). The frozen 8
  `DerivedMetadataKeys` (`caps.go`) include `chunk_count`, but the auryn schema
  does not store it yet. **M2 task 2.2 / M5 task 4.4** add the column (writer) +
  read it (reader). Until then the reader must treat it as absent.
- **`tags_override` + `tags_effective`** — §6, M2.
- **Raw MCAP summary bytes** (durable chunk-index warm) — migration plan §3.4,
  deferred; additive, reader-optional.

### The frozen 8 `DerivedMetadataKeys` (client-ingest contract — `caps.go`)

`s3_key, size_bytes, message_count, topic_count, chunk_count, duration_ns,
start_ns, end_ns`. All 8 MUST remain derivable from this schema. Audit this list
before any sign-off; `chunk_count` is the one currently pending a column (above).

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
