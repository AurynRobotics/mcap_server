# mcap_catalog_builder

A local-filesystem daemon that watches a folder of `.mcap` recordings and keeps the
SQLite **catalog** (`schema.sql`) in sync: insert/update when a file is added or
modified, hard-delete when a file is removed. It is the **single writer** to the DB.

## Usage

```bash
python3 -m mcap_catalog_builder <watch_root> [options]
```

| Option | Default | Meaning |
|---|---|---|
| `watch_root` (positional) | — | folder of `.mcap` files to watch (recursive) |
| `--db` | `/tmp/pj-cloud-catalog.db` | catalog SQLite file |
| `--rescan-interval` | `300.0` | seconds between safety re-scans |
| `--debounce` | `2.0` | seconds to debounce file events |
| `--stability-checks` | `3` | size-stability polls before cataloging |
| `--stability-interval` | `0.5` | seconds between stability polls |
| `--log-level` | `INFO` | `DEBUG`/`INFO`/`WARNING`/`ERROR` |

On startup it runs a full **reconcile** (catalog missing files, hard-delete vanished
rows), then watches via `watchdog` (inotify) plus a periodic safety re-scan.

## How dimensions are resolved

Each file's `customer/site/robot/source/date` come from, in order:

1. an **`s3_key` MCAP metadata** record (`{"key": "customer=…/customer_site=…/…/<f>.mcap"}`);
2. else the file's **path relative to `watch_root`**, if it is Hive-structured;
3. else → `catalog_failures` (the raw key is kept; the file is skipped).

The parse is trusted only if `rebuild_hive_key(dims) == key` (round-trip), so a
near-miss key is never guessed into a wrong row.

> **Caveat:** the real sample files in `../DATA/dexory` are **flat** and carry **no
> `s3_key`**, so they route to `catalog_failures` as-is. The tests therefore copy
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
routes any mismatch to `catalog_failures` — making a wrong count impossible to commit.

## S3 backend (experimental)

The daemon is local-filesystem today, but the read/list/change-detect operations
sit behind a small **storage `Source`** seam so an object store can drop in:

- `storage.py` — the `Source` protocol (`stat` / `open_summary` / `list_all`) plus
  `LocalSource` (today's behavior).
- `s3_storage.py` — `S3Source` + `S3RangeReader`: reads an MCAP summary with **1–2
  HTTP range GETs** (footer → summary offset → summary), uses the **S3 ETag** as the
  R4 fingerprint, and lists via paginated `list_objects_v2`. The message body is
  never downloaded.
- `s3_producer.py` — `s3_event_producer`: drains **S3→SQS** notifications into the
  same `WatchEvent` queue the inotify handler feeds (the cloud-native inotify).

These modules **never import `boto3`** — the client is injected — so the library and
its tests run with no AWS dependency.

> **Scope:** the S3 modules are present and unit-tested, but **not yet wired into
> the daemon CLI** — there is no `--source s3` flag. Unifying the worker around the
> `Source` seam is the next step. For now the S3 read path is exercised via tests
> and the example below.

### How to try it

**1. Unit tests — no AWS, no boto3.** A fake in-memory S3 client serves real MCAP
bytes and records every range requested, so the cheap-read property is asserted
directly:

```bash
cd /home/davide/ws_plotjuggler/auryn-mcap-server
python3 -m pytest mcap_catalog_builder/tests/test_s3_storage.py \
                  mcap_catalog_builder/tests/test_s3_producer.py \
                  mcap_catalog_builder/tests/test_storage.py -v
```

**2. Against a real bucket** — needs `boto3` and AWS credentials (env vars,
`~/.aws`, or an instance role) with `s3:GetObject` (and `s3:ListBucket` for `--list`):

```bash
pip install boto3   # only needed to actually hit S3

# Read one recording's signals/counts/time span — prints bytes fetched vs object
# size, showing the body was skipped (e.g. "fetched 7,914 of 512,338,001 bytes"):
python3 examples/s3_read_summary.py s3://my-bucket/customer=acme/.../x.mcap

# List the .mcap objects under a prefix (key + ETag, from the listing, no body read):
python3 examples/s3_read_summary.py --list s3://my-bucket/customer=acme/
```

**3. The SQS producer** is driven by a real queue: configure an S3 bucket
notification (`ObjectCreated:*`, `ObjectRemoved:*`) to an SQS queue, then call
`s3_event_producer(boto3.client("sqs"), queue_url, work_q, stop_event)` — it
enqueues the same `WatchEvent`s the local watcher does. `test_s3_producer.py` shows
the contract with a fake SQS client.

## Tests

```bash
cd /home/davide/ws_plotjuggler/auryn-mcap-server
python3 -m compileall mcap_catalog_builder
python3 -m pytest mcap_catalog_builder/tests/ -v
```

The real-data end-to-end case is skipped (never failed) when `../DATA/dexory` is absent.
