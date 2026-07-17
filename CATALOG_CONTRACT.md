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

> **Cutover COMPLETE (M6 §2.6, 2026-07-06).** The Go catalog **writer** is
> deleted: `catalog.Open`, the write API (`UpsertFile`, `ReplaceTopicsForFile`,
> `ReplaceEmbeddedTagsForFile`, `SetOverride`/`UnsetOverride`/`MaskEmbedded`/
> `HasEmbeddedTag`), the write-job goroutine, and the embedded legacy
> `schema.sql` are gone, along with `internal/indexer` (the in-process bucket
> scanner). `catalog.OpenReadOnly` is now the **only** constructor
> (`server/internal/catalog`), and `cmd/pj-cloud-server/main.go` always opens
> the catalog read-only — `catalog.external_builder` / `-external-builder` /
> `PJ_CLOUD_EXTERNAL_BUILDER` are DEPRECATED NO-OPS kept only so an existing
> launch script or config.yaml does not fail to start. The Python
> `mcap_catalog_builder` is the sole writer of every served catalog DB; a Go
> process must never open one for writing again.

---

## 1. `schema_version` — the interlock

A single-row table pins the contract version. **Both sides fail fast on mismatch.**

```sql
CREATE TABLE IF NOT EXISTS schema_version (
    id      INTEGER PRIMARY KEY CHECK (id = 1),
    version INTEGER NOT NULL
);
```

- **Current value: `3`.**
- History: **v1** (M1) = the cross-language interlock; **v2** (M2) = the
  `tags_embedded`/`tags_override`/`tags_effective` override layer + `files.chunk_count`;
  **v3** (M6) = the `build_metadata` table (catalog freshness; `build_id` is a
  monotonic confirmation counter, **not** the swap-detection trigger — see §9).
- Writer source of truth: `mcap_catalog_builder/db.py` → `SCHEMA_VERSION = 3`.
  `open_db` stamps a fresh DB and raises `SchemaVersionError` if an existing DB's
  version differs (and refuses a non-auryn DB before any DDL — see `open_db`).
- Reader source of truth: `server/internal/catalog/readonly.go` → `const
  SchemaVersion = 3`. `OpenReadOnly` raises `*SchemaVersionError` when the DB is
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
    id                INTEGER PRIMARY KEY,        -- wire file_id; stable ONLY across in-place re-catalogs — a full rebuild RENUMBERS it (§7)
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
- **D2 (the tag-edit WRITE path): LANDED, both sides (2026-07-06).** `update_tags()`
  is the sole `tags_override` writer. The **tag-edit IPC endpoint** (§10) lets the
  Go `UpdateTags` RPC handler reach it over a local UNIX socket, so a second DB
  writer never needs to exist. The Go side is live: when `catalog.tag_ipc_socket`
  (`-tag-ipc-socket` / `PJ_CLOUD_TAG_IPC_SOCKET`) is configured, `handleUpdateTags`
  forwards the edit over that socket and returns the builder's confirmed
  `tags_effective` response; when it is NOT configured, `UpdateTags` is rejected
  with a clear `ERROR_INVALID_REQUEST` (`readOnlyTagEditMessage`,
  `server/internal/ws/server.go`) — there is no other write path left (the legacy
  Go-schema direct-write behavior this paragraph used to describe is gone along
  with the rest of the Go catalog writer, §2.6).

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
  bucket), but any in-flight `file_id` becomes invalid. A full rebuild therefore
  requires a client re-`list` (any `file_id` a client is still holding from
  before the rebuild is stale) — **not** a reader (server) restart: M6 task 6.2a
  (atomic-publish + reader-reopen, §9) means the Go reader detects the swap and
  reopens the new generation on its own, transparently, on its next freshness
  tick. Clients normally resolve by *name* from a fresh `list`, so a browse→open
  after a rebuild is unaffected.

If cross-*rebuild* id stability is ever required, switch to deterministic /
content-derived ids (and bump the version).

---

## 8. Status & remaining gaps

**Landed (v2):** all reads in §2–§6 — dimensions, topic-set + `topic_counts`
decode, `s3_key` rebuild, `chunk_count`, the `tags_effective` override layer. The
Go reader (`auryn_read.go`) serves them via the `s.readOnly` branch; the frozen 8
`DerivedMetadataKeys` are all derivable.

**`build_metadata` (v3, M6 §6.5):** a single row (`id=1`) the builder stamps at the
end of each reconcile — `build_id` (monotonic; +1 per completed build),
`last_build_ns`, `files_scanned`, `files_failed`, `build_outcome`,
`builder_version`. The Go reader reads it via `catalog.GetBuildInfo` →
`pj_cloud_catalog_*` Prometheus gauges + the dashboard freshness panel (it replaces
the in-process indexer-run signals orphaned by moving the writer out of process).
Present=false before the first build (the table exists but has no `id=1` row yet)
— there is no other DB shape a Go reader can open (`OpenReadOnly` requires
`schema_version = 3`, which always ships this table). **`build_id` is a
freshness/confirmation value only — it is NOT the swap-detection mechanism.** A
rebuild replaces the served DB *file* (a new inode); the reader's swap trigger is
(dev, inode) identity polling on the served path, not `build_id` (§9's "Publish &
reopen protocol"). `build_id` merely lets an operator/dashboard confirm *which*
build is being served and that successive builds are moving forward.

**Cutover (M6 §2.6): COMPLETE (2026-07-06).** See the box at the top of this
document (§0) — the Go catalog writer + in-process indexer are deleted,
`main.go` always opens the catalog read-only, and the smoke harness
(`scripts/smoke.sh`) runs the Python builder + Go read-only server end-to-end.
**D2 (the tag-edit write path, §6/§10) is also complete:** the Go `UpdateTags`
handler forwards over the tag-edit IPC socket when `catalog.tag_ipc_socket` is
configured, and rejects with a clear error when it is not (§6). `scripts/matrix.sh`
(the deeper, real-corpus gate) is NOT yet migrated to this shape — it still
assumes the retired in-process indexer and fails fast with a clear message
instead of running; its migration is tracked separately (see its header comment).

**Remaining (forward, not breaking unless noted):**

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
  `busy_timeout=5000`, `synchronous=NORMAL` (landed M5 task 4.3 — durable across
  an app crash, only risking the last transaction on true power/OS loss; the
  catalog is a rebuildable cache of the bucket, so that tradeoff favors
  throughput). The reader DSN does not set `synchronous` (it never writes).
- Reader DSN (`readonly.go`): `mode=ro`, `busy_timeout=5000`, `foreign_keys=ON`.
  The reader **never** applies a schema (the writer's `schema.sql` is
  authoritative; applying Go's own `CREATE TABLE` would silently diverge the
  `files` definition). Note `mode=ro` (not `immutable=1`) still attaches the
  WAL/`-shm` sidecars, so the DB's **directory must be writable** even though the
  reader never writes the DB itself — a strictly read-only mount would fail to
  open.
- **Atomic publish / reopen (M6 task 6.2a): COMPLETE, both sides.** The WRITER
  side (temp-file + checkpoint-gate + atomic rename) is described in "Publish &
  reopen protocol" below. The READER side is `catalog.Store.ReopenIfSwapped`
  (`server/internal/catalog/reopen.go`), polled on a 30s ticker by `main.go`: it
  stats the served path's `(dev, inode)` identity, and when it differs from the
  last-observed one, reopens + re-verifies (schema version, identity-race-safe)
  before swapping the `Store`'s handle over — so a rebuild no longer requires a
  server restart. A rebuild-in-progress or a verification failure leaves the OLD
  handle serving (fail-closed); the next tick retries.

### Publish & reopen protocol (M6 task 6.2a)

The catalog builder (`mcap_catalog_builder/publish.py`) is the **only** writer, so
whether a write is safe to do in place depends on whether the served DB already
exists in a compatible shape:

1. **In-place WAL mutation (the norm for an existing, schema-compatible DB).**
   `catalog_object`'s composite-key upsert and `full_reconcile`'s deletion sweep
   run directly against the served path, each within its own transaction. This is
   **transactionally safe per file** (each individual write commits or rolls back
   cleanly) but **NOT reconcile-atomic across the whole DB** — a concurrent reader
   can observe a mid-reconcile state (some files updated, some not yet, mid-sweep).
   That is an accepted, long-standing property: `files.id` is stable across it
   (§7), readers already tolerate eventually-consistent catalog content, and no
   reader ever sees a torn *row* (SQLite's own transaction isolation guarantees
   that much).
2. **Create / rebuild publishes via temp file + checkpoint-gate + atomic rename.**
   When the served DB does not exist yet (first build) or must be rebuilt from
   scratch (`--rebuild`), the builder never creates or mutates the served path
   directly — a reader must never be able to open a half-built first catalog, or
   observe a rebuild's deletion-then-reinsertion mid-flight. Instead:
   `build_and_publish` builds the whole catalog into `<served_path>.building` (a
   fresh `open_db` there), checkpoints it to zero WAL frames and closes it, then
   — if a served DB already exists — checkpoint-gates it (`PRAGMA
   wal_checkpoint(TRUNCATE)`, retried for a bounded ~5s window; **aborts the whole
   publish, leaving the served DB completely untouched,** if a concurrent reader
   holds it busy past that window). A served DB that is genuinely corrupt / not a
   database at all (S4) is a DIFFERENT case from busy: the gate lets a raw
   `sqlite3.Error` (as opposed to the `PublishBusyError` above) surface, and
   `build_and_publish` treats it as garbage rather than aborting — there is no
   verified reader that could be usefully attached to a corrupt DB anyway, so it
   logs a warning, clears the old served path's `-wal`/`-shm` sidecars BEFORE the
   rename (the same reasoning as the fresh-create sidecar-clearing below), and
   lets the publish proceed. Then `os.replace`s the temp file onto the
   served path (atomic, same filesystem) and unlinks any stale `-wal`/`-shm`
   sidecars left at the served path's old generation (in the fresh-create case —
   no prior served DB — any orphaned sidecar is instead cleared *before* the
   rename, so a reader can never see the new main file next to a stale WAL). A
   rebuild's `build_id` is seeded to the old served DB's `build_id` **when that
   old build_id is readable** (`_read_old_build_id` is best-effort: a corrupt,
   unreadable, or absent old DB is not fatal — the seeding step just starts fresh
   at `build_id=1`, so an operator rebuilding *because* the DB is corrupt can
   still succeed). So the published `build_id` ends up strictly greater than the
   DB it replaces only when the old value was readable; **readers must not rely
   on `build_id` monotonicity across rebuilds** — the swap trigger is file
   identity (see point 3), never `build_id`. Separately, **`tags_override` is
   carried forward** from the OLD served DB into the temp DB (right after
   `build_fn` completes, before the temp checkpoint), by **composite file
   identity** (customer/site/robot/source/date/filename NAMES — the only
   identity stable across a rebuild, since `files.id` itself is renumbered, see
   point 3): user tag edits are not derivable from the bucket, so without this a
   `--rebuild` would silently lose every one of them. A file whose composite
   identity is no longer present in the new build has its override **dropped**
   (counted + logged), never resurrected under a different identity. This
   carry-forward is best-effort exactly like the `build_id` seeding above — an
   old DB that cannot be read is logged and skipped, never fatal to the publish.
3. **The reader's swap trigger is file identity, not `build_id`.** A rebuild
   replaces the served DB file — a new inode — while an in-place reconcile never
   does. The reader is expected to detect this by polling `(dev, inode)` (or
   equivalent) on the served path and reopening when it changes. `build_id` is
   *not* the swap token; it is a monotonically-increasing freshness/confirmation
   counter an operator/dashboard can use to confirm forward progress across
   builds (fixed in this revision — earlier drafts of this document and of
   `schema.sql`'s `build_metadata` comment described `build_id` itself as the
   swap-detection mechanism; that was never implemented that way and is not the
   locked design).
4. **WAL requires a local, same-host filesystem.** SQLite's WAL mode (and this
   protocol's `os.replace` + directory-`fsync` atomicity) assumes POSIX rename and
   `mmap`/lock semantics that hold on local disks but are **not** guaranteed on
   network filesystems. The shared DB volume between the Python writer and the Go
   reader must never be NFS/EFS-class network storage (a local disk, a local SSD,
   or a same-host bind mount only).

---

## 10. Tag-edit IPC (D2(a) — catalog-migration §1.1, LANDED both sides)

The Go server is **read-only**; the wire `UpdateTags` RPC (client→server) must
still reach this builder, the catalog's **sole writer**, somehow. Decision D2
picked **(a): the Go server forwards over a local IPC endpoint; this builder
applies the edit through its existing single-writer queue.** This section
documents the **Python side**: `mcap_catalog_builder/tag_ipc.py` (the server +
the queue item + its worker handler) and the `--tag-socket` flag in
`__main__.py` (CLI + wiring). **The Go forwarder is also landed** (§6): when
`catalog.tag_ipc_socket` is configured, `handleUpdateTags` forwards over this
socket; when it is not configured, `UpdateTags` is rejected with a clear
operator-facing error instead of falling back to any Go-side write.

### Shape

- **Transport:** a UNIX domain socket at `--tag-socket <path>` (off by
  default). Started **only** in daemon mode, **only after** the initial
  reconcile/publish has completed and the served DB is open in place — a tag
  edit must never race the startup build. `--once` never serves it.
- **Protocol:** HTTP/1.0 framing (`http.server.BaseHTTPRequestHandler`) over
  that socket (`socketserver.UnixStreamServer` + `ThreadingMixIn`), one route:
  `POST /update_tags`.
- **Request body (JSON):** `{"key": "<object key>", "set_tags": {"k": "v",
  ...}, "unset_keys": ["k", ...]}`. `set_tags` must map **string** keys to
  **string** values (a `null`/`None` value is rejected with `400` — masking an
  embedded tag is only ever done via `unset_keys`, never by "setting" a tag to
  null); `unset_keys` must be a list of **strings**. Unknown top-level fields
  are ignored.
- **Response (JSON), by outcome:**
  - `200 {"tags": [{"key", "value", "is_override"}, ...]}` — the file's full
    `tags_effective` after the edit (same shape as the wire `Tag`;
    `is_override` a JSON bool).
  - `400 {"error": ...}` — malformed JSON body, a `key` that does not parse as
    a Hive key / does not round-trip (`keyparse.parse_hive_key` +
    `rebuild_hive_key` — the same trust rule the builder itself applies, §5),
    or a `set_tags`/`unset_keys` field that fails the type validation above
    (missing/malformed/negative `Content-Length` is also a `400`).
  - `404 {"error": ...}` — the key parses, but no file with that composite
    identity exists in the catalog. This is a **lookup-only** miss
    (`db.lookup_file_id`) — a wholly unknown customer/site/robot/source in the
    key never fabricates a dimension row, unlike the builder's own
    `resolve_customer`/`resolve_site`/... (which insert on miss).
  - `413 {"error": ...}` — the request body exceeds `MAX_BODY_BYTES` (see
    "Bounded surface" below); rejected from `Content-Length` alone, before any
    body bytes are read.
  - `503 {"error": "busy"}` — the edit could not be confirmed applied within
    its deadline (see below), e.g. the single writer is mid a slow full
    reconcile, **or** the server is already at its concurrent-pending-edits
    cap (see "Bounded surface"). Not a guarantee the edit will never apply —
    only that this request could not confirm it in time; the caller should
    retry.

### Bounded surface (B1)

The endpoint is reachable by anything with local access to the socket (see
"Socket permissions & trust boundary" below), so it is deliberately bounded
against a local denial-of-service, independent of the WS-level auth the Go
server enforces upstream:

- **Body size:** a `Content-Length` that is absent, non-integer, negative, or
  above `MAX_BODY_BYTES` (1 MiB) is rejected (`400`/`413`) **before** any body
  bytes are read.
- **Per-connection timeout:** every handler socket carries a
  `HANDLER_TIMEOUT_SECONDS` (10s) read/write timeout, bounding how long a
  slow/stalled client can hold a handler thread.
- **Bounded pending edits:** at most `MAX_PENDING_EDITS` (32) tag edits may be
  enqueued-but-unreplied at once, enforced by a `threading.BoundedSemaphore`
  acquired **non-blocking** in the handler **before** the item ever reaches
  the work queue — acquisition failure is an immediate `503 {"error": "busy"}`
  that never enqueues a `TagEditItem` and never leaves a handler thread parked
  on `event.wait()`. This bounds both the queue depth and the number of
  *useful* `ThreadingMixIn` handler threads regardless of how many
  connections a local client opens.

### Key-based addressing (not `file_id`)

The request addresses the file by its **object key**, not the wire `file_id` /
`files.id`. Rationale: `files.id` is renumbered across a full rebuild (§7), and
a rebuild is exactly when a client tag edit is most likely to be in flight (the
served DB just got replaced); a key survives that because it is rebuilt from
stable dimension **names**, never a rowid. The handler
(`tag_ipc.handle_tag_edit`) parses the key back into dimensions and resolves
the CURRENT `file_id` via `db.lookup_file_id` — a lookup-only join that, unlike
`resolve_customer`/`resolve_site`/..., never inserts a dimension row for an
unknown file — immediately before writing, so it is always correct for
whichever DB generation is being served at write time.

### Deadline semantics

Each request computes `deadline = time.monotonic() + deadline_seconds`
(default 5s) before enqueuing a `TagEditItem`. The **worker thread**, when it
dequeues the item, checks `time.monotonic() > item.deadline` FIRST and — if
expired — skips the write entirely (`status = "expired"`, logged), never
applying it late. This matters because the queue can back up behind a slow
full reconcile: a caller that has already given up must never later see its
edit silently land. The IPC thread's own `event.wait()` timeout is set
ABOVE the request deadline (`deadline_seconds + 2s`, S1) so a genuine worker
reply (`ok`/`not_found`/`expired`) is preferred over the IPC thread timing out
on its own local wait — a `503` caused by *this* local timeout means the
worker had not even STARTED processing the item by its deadline, so
`handle_tag_edit`'s own deadline check will make it skip the write when it
eventually dequeues it.

Residual pathological case: if the worker dequeues the item a hair before its
deadline and then spends longer than the 2s margin inside the one small
`UPDATE` transaction (an unrealistically slow single-row write), the IPC
thread can still time out and reply `503` even though the edit actually lands
moments later. Concurrency caveat: a caller that retries after such a `503`,
and whose retry lands as a SECOND, later write, will simply overwrite the
first under the existing `tags_override` last-writer-wins semantic (the same
semantic that already governs two genuinely concurrent edits) — not a new
hazard, just the existing one surfacing through a different trigger.

### Socket permissions & trust boundary

The socket is `chmod`'d `0o660` (owner+group read/write) after bind. A stale
socket file left by a crashed prior daemon is unlinked before bind — but only
after confirming (`stat.S_ISSOCK`) that the path really IS a UNIX socket
special file (S3): a regular file or directory already occupying that path is
a **conflict**, not staleness, and fails startup with a clear error instead of
being silently clobbered. The bound socket's inode is captured after bind, and
a clean shutdown (`server_close()`) unlinks the socket file only if the path
still refers to that same inode — if another daemon replaced it in the
meantime (e.g. a slow shutdown racing a new instance's startup), the unlink is
skipped and logged rather than deleting the other daemon's live socket.
**The socket itself enforces no authentication** — a deployment is expected to
mount it into a shared, non-world-readable directory reachable only by the Go
server process and this builder process (the same host/pod; never a network
mount, same constraint as point 4 above). **The Go server's WS bearer-auth
layer is the actual auth boundary** for a client's `UpdateTags` RPC; anything
with local access to the socket can edit tags directly, bypassing that check —
an accepted, documented trust boundary (local IPC, not a network-facing
endpoint), not an oversight.

## 11. Single-writer enforcement (the process lock)

"The builder is the SOLE writer of the served DB" was, until this section, a
deploy convention. It is now a **kernel-enforced invariant**: the builder takes
an exclusive `flock` on the sidecar file **`<db_path>.writer.lock`** at startup
— before any DB write and before the tag-edit socket bind — in **both** daemon
and `--once` modes, and holds it for the process lifetime.

- **Conflict = fail fast.** A second builder pointed at the same `--db` exits
  with **code 3** and a message naming the lock path and the holder's PID. It
  must never queue behind the holder (two writers taking turns is still two
  writers), and it touches neither the DB nor the socket before failing. This
  closes the pre-lock hazard where a second daemon would treat the first one's
  LIVE tag socket as stale, unlink it, and silently steal tag edits while both
  reconcilers interleaved writes.
- **No stale-lock recovery problem.** The kernel drops a `flock` on ANY process
  death — crash, SIGKILL, clean exit — so a leftover lock can never block a
  restart (unlike a pidfile). The PID written inside the file is a diagnostic
  only, never consulted for liveness.
- **The lock file is never unlinked.** Unlink-on-release reintroduces a
  two-holders race (lock an orphaned inode vs. lock a re-created path). A
  leftover `.writer.lock` next to the DB is expected and harmless.
- **Scope = one DB path.** Builders serving *different* `--db` paths do not
  conflict (e.g. a harness catalog alongside an interactive one).
- **Local filesystem only** — the same constraint the catalog volume already
  has (§9: SQLite WAL forbids NFS/EFS); `flock` is reliable exactly there. The
  atomic publish (`--rebuild`: temp build + rename onto the served path) does
  not touch the sidecar, so the lock spans a publish unchanged.
- **Reader side: nothing changes.** The Go server takes no lock — it opens the
  DB read-only and follows §9. The socket stale-cleanup in §10 is now provably
  safe: a leftover socket can only belong to a dead builder, because a live one
  would have held this lock.
