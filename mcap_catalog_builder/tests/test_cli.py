"""Tests for the CLI parser and the single-writer worker loop."""

import os
import queue
import signal
import sys
import types

from mcap_catalog_builder.__main__ import build_parser, main, worker_loop
from mcap_catalog_builder.tests.fixtures import write_minimal_mcap
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


def test_parser_no_watch_flag():
    assert build_parser().parse_args(["d"]).no_watch is False
    assert build_parser().parse_args(["--no-watch", "d"]).no_watch is True


def _run_daemon_main(argv):
    """Call ``main`` in (non-``--once``) daemon mode, restoring whatever
    SIGINT/SIGTERM handlers were installed beforehand — ``main`` overwrites
    them unconditionally once it reaches the daemon section, and daemon-mode
    tests must not leak that override into the rest of the suite."""
    orig_int = signal.getsignal(signal.SIGINT)
    orig_term = signal.getsignal(signal.SIGTERM)
    try:
        return main(argv)
    finally:
        signal.signal(signal.SIGINT, orig_int)
        signal.signal(signal.SIGTERM, orig_term)


def test_no_watch_local_daemon_starts_no_observer(tmp_path, monkeypatch):
    # --no-watch must skip the watchdog/inotify observer entirely (local
    # daemon mode), while the startup reconcile still runs (real reconcile,
    # against an empty dir — cheap — wrapped by a spy for the assertion).
    # worker_loop is stubbed to a no-op so main() doesn't block draining a
    # queue nothing will ever populate.
    import mcap_catalog_builder.__main__ as m
    from mcap_catalog_builder.reconcile import full_reconcile as real_full_reconcile

    root = str(tmp_path / "watch")
    os.makedirs(root)
    db = str(tmp_path / "catalog.db")

    observer_calls = []
    monkeypatch.setattr(m, "start_observer", lambda *a, **k: observer_calls.append((a, k)))

    reconciled = []

    def spy_reconcile(conn, caches, source):
        reconciled.append(source)
        return real_full_reconcile(conn, caches, source)

    monkeypatch.setattr(m, "full_reconcile", spy_reconcile)
    monkeypatch.setattr(m, "worker_loop", lambda *a, **k: None)

    assert _run_daemon_main(["--no-watch", root, "--db", db]) == 0
    assert observer_calls == []  # no watchdog observer ever started
    assert reconciled  # the startup reconcile still ran


class _EmptyS3Client:
    """Fake boto3 S3 client whose bucket listing is always empty — enough
    for a real ``full_reconcile`` to run cheaply with no network access."""

    def get_paginator(self, _name):
        class _Paginator:
            def paginate(self, **_kwargs):
                return iter([{"Contents": []}])

        return _Paginator()


def test_main_s3_daemon_no_watch_skips_sqs_requirement(tmp_path, monkeypatch):
    # --no-watch relaxes --source s3's --sqs-url requirement in daemon mode,
    # and the SQS producer must never be started. boto3 isn't installed in
    # this environment, so a bare fake module (with a real, empty-listing S3
    # client) is injected — the real full_reconcile runs against it unmocked.
    # worker_loop is stubbed to a no-op so main() doesn't block on the
    # (never populated, in this test) work queue.
    import mcap_catalog_builder.__main__ as m
    import mcap_catalog_builder.s3_producer as s3_producer_mod

    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.client = lambda name: _EmptyS3Client() if name == "s3" else object()
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)

    producer_calls = []
    monkeypatch.setattr(
        s3_producer_mod, "s3_event_producer", lambda *a, **k: producer_calls.append(a)
    )
    monkeypatch.setattr(m, "worker_loop", lambda *a, **k: None)

    db = str(tmp_path / "catalog.db")
    rc = _run_daemon_main(["--source", "s3", "--s3-bucket", "b", "--no-watch", "--db", db])

    assert rc == 0  # passes validation without --sqs-url
    assert os.path.exists(db)  # reconcile+publish actually completed
    assert producer_calls == []  # SQS long-poll producer never started


def test_parser_once_flag():
    assert build_parser().parse_args(["d"]).once is False
    assert build_parser().parse_args(["--once", "d"]).once is True


def test_parser_rebuild_flag():
    assert build_parser().parse_args(["d"]).rebuild is False
    assert build_parser().parse_args(["--rebuild", "d"]).rebuild is True


def test_parser_tag_socket_default_off():
    assert build_parser().parse_args(["d"]).tag_socket is None


def test_parser_tag_socket_option():
    args = build_parser().parse_args(["--tag-socket", "/tmp/x.sock", "d"])
    assert args.tag_socket == "/tmp/x.sock"


def _hive_one_file(root):
    dest = os.path.join(
        root,
        "customer=dexory", "customer_site=london", "robot=rob01",
        "source=ros-bags", "date=2026-06-01", "a.mcap",
    )
    write_minimal_mcap(dest)


def test_once_rebuild_on_existing_db_goes_through_publish_path(tmp_path):
    root = str(tmp_path / "watch")
    db = str(tmp_path / "catalog.db")
    _hive_one_file(root)

    assert main(["--once", root, "--db", db]) == 0  # first build: create path
    old_inode = os.stat(db).st_ino

    assert main(["--once", "--rebuild", root, "--db", db]) == 0
    new_inode = os.stat(db).st_ino
    assert new_inode != old_inode  # --rebuild republished a NEW file (§6.2a)
    assert not os.path.exists(db + ".building")


def test_once_without_rebuild_on_existing_db_stays_in_place(tmp_path):
    root = str(tmp_path / "watch")
    db = str(tmp_path / "catalog.db")
    _hive_one_file(root)

    assert main(["--once", root, "--db", db]) == 0  # first build: create path
    old_inode = os.stat(db).st_ino

    assert main(["--once", root, "--db", db]) == 0  # second run: no --rebuild
    new_inode = os.stat(db).st_ino
    assert new_inode == old_inode  # in-place mutation, same file


def test_once_no_watch_behaves_like_once(tmp_path):
    # --no-watch is meaningless with --once (--once already does a single
    # reconcile and exits before any producer would start) — it must be a
    # harmless no-op, not an error.
    root = str(tmp_path / "watch")
    db = str(tmp_path / "catalog.db")
    _hive_one_file(root)

    assert main(["--once", "--no-watch", root, "--db", db]) == 0
    assert os.path.exists(db)


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
