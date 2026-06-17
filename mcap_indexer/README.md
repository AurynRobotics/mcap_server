# mcap_indexer

A local-filesystem daemon that watches a folder of `.mcap` recordings and keeps the
SQLite **catalog** (`schema.sql`) in sync: insert/update when a file is added or
modified, hard-delete when a file is removed. It is the **single writer** to the DB.

## Usage

```bash
python3 -m mcap_indexer <watch_root> [options]
```

| Option | Default | Meaning |
|---|---|---|
| `watch_root` (positional) | — | folder of `.mcap` files to watch (recursive) |
| `--db` | `/tmp/pj-cloud-catalog.db` | catalog SQLite file |
| `--rescan-interval` | `300.0` | seconds between safety re-scans |
| `--debounce` | `2.0` | seconds to debounce file events |
| `--stability-checks` | `3` | size-stability polls before indexing |
| `--stability-interval` | `0.5` | seconds between stability polls |
| `--log-level` | `INFO` | `DEBUG`/`INFO`/`WARNING`/`ERROR` |

On startup it runs a full **reconcile** (index missing files, hard-delete vanished
rows), then watches via `watchdog` (inotify) plus a periodic safety re-scan.

## How dimensions are resolved

Each file's `customer/site/robot/source/date` come from, in order:

1. an **`s3_key` MCAP metadata** record (`{"key": "customer=…/customer_site=…/…/<f>.mcap"}`);
2. else the file's **path relative to `watch_root`**, if it is Hive-structured;
3. else → `indexer_failures` (the raw key is kept; the file is skipped).

The parse is trusted only if `rebuild_hive_key(dims) == key` (round-trip), so a
near-miss key is never guessed into a wrong row.

> **Caveat:** the real sample files in `../DATA/dexory` are **flat** and carry **no
> `s3_key`**, so they route to `indexer_failures` as-is. The tests therefore copy
> them into a Hive tree (`make_hive_fixture`) or synthesize MCAPs with an injected
> `s3_key`. Per-file stats come only from the MCAP **summary** — never the embedded
> `rosbag2` metadata, which describes the whole multi-day bag.

## Change detection & removal

- **Fingerprint** = `(size_bytes, mtime_ns)` (there is no S3 ETag locally; `etag` is a
  synthetic `local:{size}:{mtime}` token, never compared). Unchanged `(size, mtime)`
  → the file is skipped with no read.
- **Removal** hard-deletes the `files` row (tags cascade); the append-only lookups,
  dictionaries, and `topic_sets` are left as harmless orphans (no GC).

## Architecture (single writer)

The `watchdog` observer, the debounce `Timer`s, and the periodic-rescan thread are
**producers** — they only enqueue events. One `worker_loop` (main thread) drains the
queue and performs **every** DB write. SQLite runs in **WAL** mode, so external
readers can query the catalog concurrently while the daemon writes.

The `topic_counts` blob is built from the file's sorted topic-set members with
`channel_message_counts.get(channel_id, 0)` (zero-message channels are absent from
that dict), guarded by an in-transaction `sum(counts) == message_count` check that
routes any mismatch to `indexer_failures` — making a wrong count impossible to commit.

## Tests

```bash
cd /home/davide/ws_plotjuggler/mcap_server
python3 -m compileall mcap_indexer
python3 -m pytest mcap_indexer/tests/ -v
```

The real-data end-to-end case is skipped (never failed) when `../DATA/dexory` is absent.
