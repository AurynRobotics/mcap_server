# mcap_server

Makes a lake of **MCAP** recordings browsable and filterable as a fast database
query — so a client can find the few recordings it wants among millions and
stream only the signals it picks, without downloading whole files.

This repo currently holds the **catalog + indexer**: the indexer detects MCAP
files uploaded to the server and keeps a searchable **SQLite catalog** in sync. A
separate query/data server (later, likely Go) reads that catalog to serve
clients; the streaming path is future work.

```
upload ──► indexer ──► SQLite catalog ──► query server ──► client
(.mcap)    (writer)    (metadata only)    (reader)         filter + stream subset
```

## Layout

- **[`REQUIREMENTS.md`](REQUIREMENTS.md)** — start here: the vision, use cases,
  and numbered requirements.
- **[`mcap_indexer/`](mcap_indexer/)** — the indexer daemon (single writer, WAL,
  footer-only reads, fingerprint-skip) + tests.
  [`schema.sql`](mcap_indexer/schema.sql) is the catalog schema;
  [`README.md`](mcap_indexer/README.md) documents the CLI.

## Quickstart

```bash
python3 -m mcap_indexer <watch_root> [--db PATH]
python3 -m pytest mcap_indexer/tests/ -v
```
