# mcap_server

Makes a lake of **MCAP** recordings browsable and filterable as a fast database
query — so a client can find the few recordings it wants among millions and
stream only the signals it picks, without downloading whole files.

This repo currently holds the **catalog schema + catalog builder**: the catalog builder detects MCAP
files uploaded to the server and keeps a searchable **SQLite catalog** in sync. A
separate query/data server (later, likely Go) reads that catalog to serve
clients; the streaming path is future work.

```
upload ──► catalog builder ──► SQLite catalog ──► query server ──► client
(.mcap)    (writer)    (metadata only)    (reader)         filter + stream subset
```

## Layout

- **[`REQUIREMENTS.md`](REQUIREMENTS.md)** — start here: the vision, use cases,
  and numbered requirements.
- **[`mcap_catalog_builder/`](mcap_catalog_builder/)** — the catalog builder daemon (single writer, WAL,
  footer-only reads, fingerprint-skip) + tests.
  [`schema.sql`](mcap_catalog_builder/schema.sql) is the catalog schema;
  [`README.md`](mcap_catalog_builder/README.md) documents the CLI **and the
  experimental S3 backend** (range-GET reads + SQS-driven cataloging).
- **[`examples/`](examples/)** — runnable demos, e.g.
  [`s3_read_summary.py`](examples/s3_read_summary.py): read an MCAP summary straight
  from S3 without downloading the file.

## Quickstart

```bash
python3 -m mcap_catalog_builder <watch_root> [--db PATH]
python3 -m pytest mcap_catalog_builder/tests/ -v
```
