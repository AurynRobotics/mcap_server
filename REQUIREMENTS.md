# mcap_server — Catalog & Indexer: Requirements

Plain and intentionally lightweight for now. The executable schema in
[`mcap_indexer/schema.sql`](mcap_indexer/schema.sql) is the source of truth for
table structure; this file is the *why* and the *what*. Requirements are numbered
(`R1`…) so they can be cited.

## The pipeline (vision)

1. MCAP recordings are **uploaded to the server** (today: a watched folder;
   later: an S3/GCS bucket).
2. The **indexer** detects each new / changed / removed file and keeps a
   **SQLite catalog** in sync. It is the *single writer*.
3. A **query / data server** (likely Go — not decided yet) reads that same
   catalog and serves clients.
4. A **client** (PlotJuggler plugin / CLI) asks which tags & metadata exist,
   builds a query — *"give me the files in this time range matching these
   tags"* — and gets back the matching files.
5. The client selects a subset of those files and **streams their content
   directly** — only the chosen topics, not whole files. *(Streaming is a
   separate subsystem, not in this repo yet — see "Not in scope".)*

The catalog is the hinge: browsing and filtering MUST be a fast database query,
**never** a scan of the recordings themselves.

## What the catalog stores

Metadata *about* recordings — never the recorded messages:

- **identity & location** — filename plus the path labels
  `customer / site / robot / source / date` (the full object key is rebuildable
  from these, so it isn't stored twice);
- a **change fingerprint** — free from the storage listing, so detecting a change
  needs no file read;
- the **time span** (start / end);
- the **set of signals** it contains (topic name + schema) and the **per-signal
  message counts**;
- **tags** (open-ended `key=value`) and a **health flag**.

## Use cases (the queries the catalog must answer)

- Enumerate available filter options (which customers / sites / robots / tags
  exist) **without scanning the file list**.
- Filter recordings by: **time-window overlap**; **"contains signal X"**; **tag**
  (`key`, or `key=value`); **path dimensions**. Combine with AND.
- **Page** through results at constant cost, however deep.
- **Inspect one recording** (its signals, counts, time span, tags) with no file
  read.
- List recordings that **failed validation**, and — separately — files that
  **could not be indexed** at all.

## Requirements

**Indexer**
- **R1** — Single writer to the catalog; readers (the query server) run
  concurrently (SQLite WAL).
- **R2** — To index a file, read **only its MCAP summary/footer** (a few KB) plus
  the path — never the whole file.
- **R3** — Derive labels from the file's Hive-partitioned key; trust the parse
  only if it **round-trips** back to the original key, else log a failure (keep
  the raw key, skip the file) — never guess a wrong row.
- **R4** — Detect change with a cheap **fingerprint from the listing** (S3 ETag /
  GCS generation; locally `size + mtime`). Unchanged → skip with no read; a
  restart over an indexed lake re-reads **zero** files.
- **R5** — Keep the catalog in sync: **insert** new, **re-index** changed,
  **hard-delete** vanished; **reconcile** on startup. Each file's update is one
  transaction.

**Catalog**
- **R6** — Store metadata only — **never** message payloads.
- **R7** — **Deduplicate** the set of channels across files (most files share a
  layout): store each layout once, point files at it, keep only the per-file
  counts per file.
- **R8** — Tags are **open-ended yet index-seekable** (arbitrary keys *and*
  filterable at scale) — not a JSON blob.
- **R9** — Browsing/filtering hits the catalog **only** (zero object-store reads)
  and uses **keyset pagination** (a cursor on `id`), not `OFFSET`.

**Scale**
- **R10** — A filtered page costs the same at 100 files or 8,000,000 (pre-built
  index + paginated seek → single-digit ms). Catalog size tracks file *count*,
  not bytes (~0.7 GB per 1M files, thanks to R7). The cost that scales with the
  lake is the **cold index build**, not queries — the main lever there is
  indexing files in parallel (not done yet).

## Not in scope yet

- The **streaming / download** path (reading chunks, filtering to the selected
  topics/time, compressing, resume).
- The **S3/GCS backend** — the indexer is local-filesystem today; the schema and
  fingerprint requirements are already storage-agnostic.
- **Human-edited tags** that survive a re-index.
- **Parallel** indexing.

## See also

- [`mcap_indexer/`](mcap_indexer/) — the running indexer + tests.
- [`mcap_indexer/schema.sql`](mcap_indexer/schema.sql) — the executable catalog
  schema (source of truth for tables).
- [`mcap_indexer/README.md`](mcap_indexer/README.md) — the daemon's CLI and
  behavior.
