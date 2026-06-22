"""Tests for the CLI parser and the single-writer worker loop."""

import queue

from mcap_catalog_builder.__main__ import build_parser, main, worker_loop
from mcap_catalog_builder.watcher import WatchEvent


class _FakeSource:
    """A worker-facing Source stub: identity event keys, configurable stability."""

    def __init__(self, stable: bool = True) -> None:
        self._stable = stable

    def event_key(self, payload: str) -> str:
        return payload

    def wait_for_stable(self, payload: str) -> bool:
        return self._stable


def test_parser_defaults():
    args = build_parser().parse_args(["/some/dir"])
    assert args.watch_root == "/some/dir"
    assert args.db == "/tmp/pj-cloud-catalog.db"
    assert args.rescan_interval == 300.0
    assert args.debounce == 2.0
    assert args.stability_checks == 3
    assert args.stability_interval == 0.5
    assert args.log_level == "INFO"
    assert args.source == "local"  # default backend


def test_parser_s3_options():
    args = build_parser().parse_args(
        ["--source", "s3", "--s3-bucket", "b", "--s3-prefix", "p/", "--sqs-url", "http://q"]
    )
    assert args.source == "s3"
    assert (args.s3_bucket, args.s3_prefix, args.sqs_url) == ("b", "p/", "http://q")


def test_main_bad_watch_root_returns_2(tmp_path):
    assert main([str(tmp_path / "does-not-exist")]) == 2


def test_main_s3_without_bucket_returns_2():
    assert main(["--source", "s3"]) == 2  # --source s3 requires --s3-bucket


def test_main_s3_daemon_without_sqs_returns_2():
    # The watch daemon (no --once) still requires --sqs-url to drain live events.
    assert main(["--source", "s3", "--s3-bucket", "b"]) == 2


def test_parser_once_flag():
    assert build_parser().parse_args(["d"]).once is False
    assert build_parser().parse_args(["--once", "d"]).once is True


def test_worker_loop_stops_on_stop_event(tmp_db):
    conn, caches = tmp_db
    q: queue.Queue = queue.Queue()
    q.put(WatchEvent("stop"))
    worker_loop(conn, caches, _FakeSource(), q)  # returns promptly → ok


def test_worker_loop_processes_catalog_then_stop(tmp_db, monkeypatch):
    conn, caches = tmp_db
    import mcap_catalog_builder.__main__ as m

    cataloged: list[str] = []
    monkeypatch.setattr(m, "catalog_object", lambda c, ca, k, s: cataloged.append(k))

    q: queue.Queue = queue.Queue()
    q.put(WatchEvent("catalog", "/w/a.mcap"))
    q.put(WatchEvent("stop"))
    worker_loop(conn, caches, _FakeSource(stable=True), q)
    assert cataloged == ["/w/a.mcap"]  # event_key maps payload → key


def test_worker_loop_drops_unstable_file(tmp_db, monkeypatch):
    conn, caches = tmp_db
    import mcap_catalog_builder.__main__ as m

    cataloged: list[str] = []
    monkeypatch.setattr(m, "catalog_object", lambda c, ca, k, s: cataloged.append(k))

    q: queue.Queue = queue.Queue()
    q.put(WatchEvent("catalog", "/w/a.mcap"))
    q.put(WatchEvent("stop"))
    worker_loop(conn, caches, _FakeSource(stable=False), q)
    assert cataloged == []  # unstable file dropped, not cataloged


def test_worker_loop_delete_dispatches_by_key(tmp_db, monkeypatch):
    conn, caches = tmp_db
    import mcap_catalog_builder.__main__ as m

    deleted: list[str] = []
    monkeypatch.setattr(m, "delete_by_key", lambda c, ca, k: deleted.append(k))

    q: queue.Queue = queue.Queue()
    q.put(WatchEvent("delete", "/w/a.mcap"))
    q.put(WatchEvent("stop"))
    worker_loop(conn, caches, _FakeSource(), q)
    assert deleted == ["/w/a.mcap"]
