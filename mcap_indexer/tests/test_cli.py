"""Tests for the CLI parser and the single-writer worker loop."""

import queue

from mcap_indexer.__main__ import build_parser, main, worker_loop
from mcap_indexer.watcher import WatchEvent


def test_parser_defaults():
    args = build_parser().parse_args(["/some/dir"])
    assert args.watch_root == "/some/dir"
    assert args.db == "/tmp/pj-cloud-catalog.db"
    assert args.rescan_interval == 300.0
    assert args.debounce == 2.0
    assert args.stability_checks == 3
    assert args.stability_interval == 0.5
    assert args.log_level == "INFO"


def test_main_bad_watch_root_returns_2(tmp_path):
    assert main([str(tmp_path / "does-not-exist")]) == 2


def test_worker_loop_stops_on_stop_event(tmp_db, tmp_path):
    conn, caches = tmp_db
    q: queue.Queue = queue.Queue()
    q.put(WatchEvent("stop"))
    worker_loop(conn, caches, str(tmp_path), q, 3, 0.0)  # returns promptly → ok


def test_worker_loop_processes_index_then_stop(tmp_db, tmp_path, monkeypatch):
    conn, caches = tmp_db
    import mcap_indexer.__main__ as m

    indexed: list[str] = []
    monkeypatch.setattr(m, "wait_for_stable", lambda *a, **k: True)
    monkeypatch.setattr(m, "index_file", lambda c, ca, p, r: indexed.append(p))

    q: queue.Queue = queue.Queue()
    q.put(WatchEvent("index", "/w/a.mcap"))
    q.put(WatchEvent("stop"))
    worker_loop(conn, caches, str(tmp_path), q, 3, 0.0)
    assert indexed == ["/w/a.mcap"]


def test_worker_loop_drops_unstable_file(tmp_db, tmp_path, monkeypatch):
    conn, caches = tmp_db
    import mcap_indexer.__main__ as m

    indexed: list[str] = []
    monkeypatch.setattr(m, "wait_for_stable", lambda *a, **k: False)  # never stable
    monkeypatch.setattr(m, "index_file", lambda c, ca, p, r: indexed.append(p))

    q: queue.Queue = queue.Queue()
    q.put(WatchEvent("index", "/w/a.mcap"))
    q.put(WatchEvent("stop"))
    worker_loop(conn, caches, str(tmp_path), q, 3, 0.0)
    assert indexed == []  # unstable file dropped, not indexed
