# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

The **catalog schema + catalog builder** for a system that makes a lake of MCAP recordings
browsable/filterable as a fast SQLite query (find the few files you want among
millions, then stream only the signals you pick). The full vision:

```
upload ──► catalog builder ──► SQLite catalog ──► query server ──► client
(.mcap)    (writer)    (metadata only)    (reader, later)  filter + stream subset
```

Only the **catalog builder** (a Python daemon, the catalog's single writer) and the
**catalog schema** live here. The query/data server and the streaming path are
future work (likely Go), not in this repo.

- `REQUIREMENTS.md` — the *why*/*what*, with numbered requirements **R1–R10**.
  Code and commits cite these (e.g. "R7 dedup"); preserve that convention.
- `mcap_catalog_builder/schema.sql` — the **source of truth** for table structure.
- `mcap_catalog_builder/README.md` — daemon CLI + behavior detail.

## Commands

No `requirements.txt`/`pyproject.toml`. Deps are expected pre-installed:
`mcap`, `watchdog`, `pytest`. Code targets Python 3.10+ syntax (`X | None`,
`list[str]`); 3.14 is what's installed here.

```bash
# Run the daemon (watches a folder recursively, keeps the catalog in sync)
python3 -m mcap_catalog_builder <watch_root> [--db PATH]      # default DB: /tmp/pj-cloud-catalog.db

# Full test suite (63 tests, ~1s) — run from the repo root
python3 -m pytest mcap_catalog_builder/tests/ -v

# Single test / file
python3 -m pytest mcap_catalog_builder/tests/test_builder.py -v
python3 -m pytest mcap_catalog_builder/tests/test_builder.py::test_name -v

# Syntax/compile check (used in CI-style verification)
python3 -m compileall mcap_catalog_builder
```

For ad-hoc inspection of `.mcap` files, use the official CLI at
`~/Apps/mcap-linux-amd64` (e.g. `info`, `list channels`) rather than parsing bytes.

## Architecture

**Single-writer, producer/consumer** (`__main__.py`). The `watchdog` observer,
the per-path debounce `Timer`s, and the periodic rescan thread are **producers** —
they *only* `queue.put(WatchEvent)` and never touch the DB. One `worker_loop` on
the main thread is the **sole consumer and the only DB writer**. SQLite runs in
**WAL** mode so external readers query concurrently. The worker wraps every event
in try/except so it can never die.

Module layering (each does one job):

- `watcher.py` — inotify handler (enqueue-only) + `wait_for_stable` (size-poll
  guard against cataloging a file mid-copy, run by the *worker*, not the handler).
- `builder.py` — `catalog_file`: resolves dimensions, fingerprint-skips unchanged
  files, then runs the per-file `with conn:` transaction (insert/update). Also
  `delete_by_path` (best-effort live removal).
- `reconcile.py` — `full_reconcile`: scan disk → catalog all → deletion sweep of
  rows with no on-disk file. **Authoritative** for removals; runs on startup and
  on each `--rescan-interval`. Live `on_deleted` events are just an optimization.
- `db.py` — connection (WAL/FK/busy_timeout set per-connection, **not** in
  schema.sql), in-memory id `Caches`, and `resolve_*` helpers.
- `mcap_summary.py` — reads per-file facts from the MCAP **summary/footer only**.
- `keyparse.py` — Hive object-key parse / rebuild / relpath.
- `varint.py` — LEB128 codec for the packed `topic_counts` blob.

### Catalog schema (the data-model decisions that matter)

- **Dimensions** `customer/site/robot/source/date` are normalized into lookup
  tables (hierarchical: site→customer, robot→site); `files` holds FK ids.
- **R7 dedup:** the *set* of channels a file has is stored once in `topic_sets`
  (keyed by a fingerprint hash of sorted `(topic_id, schema_id)` members); each
  file points at a set and stores only its **per-topic counts** as a varint blob.
- **R8 tags:** open-ended `(key, value)` EAV in `tags`, index-seekable — never a
  JSON blob.
- **R9 pagination:** keyset cursor on `files.id`, never `OFFSET`.
- **`catalog_failures`** — files that couldn't be cataloged (keeps the raw key).

## Invariants you must not break

These are deliberate correctness guards — the catalog builder is the catalog's only
writer, so a wrong row is unrecoverable. Each makes a bad write *impossible*, not
merely unlikely:

- **Round-trip the key (R3).** Dimensions are trusted only if
  `rebuild_hive_key(dims) == key.lstrip("/")`. A near-miss key → `catalog_failures`,
  never a guessed row. `keyparse` parse/rebuild must stay exact inverses.
- **Count check.** Inside the transaction, `sum(counts) != message_count` raises →
  the file rolls into `catalog_failures`. Don't relax this.
- **Reload caches on rollback.** On any transaction failure, `catalog_file` does
  `caches.__dict__.update(load_caches(conn).__dict__)` so ids inserted in the
  rolled-back txn can't poison the in-memory caches. Preserve this.
- **Zero-message channels.** Read counts with `counts.get(ch.id, 0)` — a
  zero-message channel is in `summary.channels` but **absent** from
  `channel_message_counts`. The `.get(..., 0)` default is mandatory.
- **Summary only.** Per-file stats come from the MCAP summary/Statistics, **never**
  the embedded `rosbag2` metadata (which describes the whole multi-day bag).
- **R2 / R4 cheap path.** Cataloging reads only the footer (a few KB), and an unchanged
  `(size, mtime)` fingerprint skips with **no file read**. A restart over an
  cataloged lake must re-read zero files.
- **Read the summary OUTSIDE the transaction** (it's slow and can throw).
- **`topic_counts` blob ordering:** one varint per topic-set member, sorted by
  `topic_id` ASC, aligned with `topic_set_members`. Encode/decode must agree.
- **Resolvers** use `INSERT OR IGNORE` then `SELECT id` (never `lastrowid` after
  `OR IGNORE`) and **do not commit** — the caller owns the transaction.

## Gotchas

- **Repo path:** this checkout is `auryn-mcap-server/`, but the project is named
  `mcap_server` in the doc titles (`README.md`, `REQUIREMENTS.md`) — the repo dir
  carries an "auryn" prefix the project name does not. The name is intentional;
  only filesystem paths point at `auryn-mcap-server/`.
- **Real Dexory samples are flat** (`/home/davide/ws_plotjuggler/DATA/dexory`,
  referenced as `../DATA/dexory`) and carry **no `s3_key`**, so they route
  straight to `catalog_failures`. Tests therefore either copy a sample into a Hive
  tree (`make_hive_fixture`) or synthesize an MCAP with an injected `s3_key`
  (`write_minimal_mcap`). The real-data e2e test **auto-skips** when the data is
  absent — a green suite does not mean the real-data path ran.
- **Dimension resolution order** (`resolve_dimensions`): an `s3_key` metadata
  record wins; else the file's Hive-structured path relative to `watch_root`;
  else failure.
- **Removal leaves orphans** by design — vanished `files` rows are hard-deleted
  (tags cascade), but lookup/dictionary/`topic_sets` rows are left as harmless
  orphans (no GC).
- **`schema.sql` is idempotent** (`CREATE … IF NOT EXISTS`) and re-runnable via
  `executescript`; PRAGMAs live in `db.open_db`, not the schema file.
- **`file_metrics` / `file_metric_status` are forward-declared and empty.** They
  back the *planned* async metric-extraction pass (REQUIREMENTS R11–R13, for
  numeric-threshold queries like *velocity > X*); the current catalog builder writes to
  neither. `open_db` creates them, but don't expect rows until that pass exists.
