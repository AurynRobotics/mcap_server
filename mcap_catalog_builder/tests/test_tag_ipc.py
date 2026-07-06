"""End-to-end tests for the tag-edit IPC endpoint (catalog-migration §1.1
DECISION D2(a)): a real UNIX socket, a real HTTP client (``http.client``), and
the actual single-writer ``worker_loop`` draining the queue — so these tests
exercise the exact path a Go forwarder would drive.
"""

import http.client
import json
import os
import queue
import socket
import stat
import threading

import pytest

from mcap_catalog_builder.__main__ import worker_loop
from mcap_catalog_builder.db import load_caches, open_db
from mcap_catalog_builder.publish import build_and_publish
from mcap_catalog_builder.reconcile import full_reconcile
from mcap_catalog_builder.tag_ipc import TagEditServer
from mcap_catalog_builder.tests.fixtures import write_minimal_mcap
from mcap_catalog_builder.watcher import WatchEvent

KEY = (
    "customer=dexory/customer_site=london/robot=rob01/"
    "source=ros-bags/date=2026-06-01/x.mcap"
)


class _UnixHTTPConnection(http.client.HTTPConnection):
    """``http.client.HTTPConnection`` over a UNIX domain socket (stdlib has no
    built-in client for this; the recipe is just overriding ``connect``)."""

    def __init__(self, path: str, timeout: float = 5.0) -> None:
        super().__init__("localhost", timeout=timeout)
        self._unix_path = path

    def connect(self) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect(self._unix_path)
        self.sock = sock


def _post(socket_path: str, payload, timeout: float = 5.0):
    conn = _UnixHTTPConnection(socket_path, timeout=timeout)
    try:
        body = json.dumps(payload).encode("utf-8") if not isinstance(payload, (bytes, str)) else (
            payload.encode("utf-8") if isinstance(payload, str) else payload
        )
        conn.request(
            "POST", "/update_tags", body=body,
            headers={"Content-Type": "application/json", "Content-Length": str(len(body))},
        )
        resp = conn.getresponse()
        raw = resp.read()
        data = json.loads(raw.decode("utf-8")) if raw else {}
        return resp.status, data
    finally:
        conn.close()


def _raw_request(socket_path: str, request_bytes: bytes, timeout: float = 5.0):
    """Send exactly ``request_bytes`` over a fresh UNIX-socket connection and
    parse the HTTP status line back out — used where we need to control (or
    deliberately mismatch) the ``Content-Length`` header below what
    ``http.client`` would compute automatically (B1 tests)."""
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    sock.connect(socket_path)
    try:
        sock.sendall(request_bytes)
        sock.shutdown(socket.SHUT_WR)
        chunks = []
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)
    finally:
        sock.close()
    raw = b"".join(chunks)
    status = int(raw.split(b"\r\n", 1)[0].split(b" ")[1])
    return status, raw


def _hive(root, filename="x.mcap"):
    dest = os.path.join(
        root, "customer=dexory", "customer_site=london", "robot=rob01",
        "source=ros-bags", "date=2026-06-01", filename,
    )
    write_minimal_mcap(dest, channels=[("/a", "S", "ros2msg", 2)])
    return dest


class _StableSource:
    """Worker-facing Source stub: identity event keys, always stable."""

    def event_key(self, payload: str) -> str:
        return payload

    def wait_for_stable(self, payload: str) -> bool:
        return True


class _WorkerHarness:
    """Runs the REAL ``worker_loop`` (the single writer) on a background
    thread against a served DB, so TagEditItem dispatch is exercised through
    the exact production dispatch path, not a test-only stub."""

    def __init__(self, db_path: str) -> None:
        self.conn = open_db(db_path)
        self.caches = load_caches(self.conn)
        self.work_q: "queue.Queue" = queue.Queue()
        self._thread = threading.Thread(
            target=worker_loop,
            args=(self.conn, self.caches, _StableSource(), self.work_q),
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self.work_q.put(WatchEvent("stop"))
        self._thread.join(timeout=5)
        self.conn.close()


@pytest.fixture
def served_db(tmp_path):
    root = str(tmp_path / "watch")
    served = str(tmp_path / "catalog.db")
    _hive(root, "x.mcap")
    build_and_publish(served, lambda c, ca: full_reconcile(c, ca, root))
    return served


@pytest.fixture
def worker(served_db):
    h = _WorkerHarness(served_db)
    yield h
    h.stop()


@pytest.fixture
def sock_path(tmp_path):
    return str(tmp_path / "tagedit.sock")


def _run_server(server):
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return t


def _stop_server(server, thread):
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


def test_set_and_unset_round_trip_visible_in_tags_effective(worker, sock_path):
    server = TagEditServer(sock_path, worker.work_q)
    t = _run_server(server)
    try:
        status, data = _post(sock_path, {"key": KEY, "set_tags": {"env": "prod", "temp": "1"}})
        assert status == 200
        tags = {row["key"]: row for row in data["tags"]}
        assert tags["env"] == {"key": "env", "value": "prod", "is_override": True}
        assert tags["temp"] == {"key": "temp", "value": "1", "is_override": True}

        # unset a pure override (no embedded counterpart) -> deleted outright.
        status, data = _post(sock_path, {"key": KEY, "unset_keys": ["temp"]})
        assert status == 200
        tags = {row["key"]: row for row in data["tags"]}
        assert "temp" not in tags
        assert tags["env"]["value"] == "prod"
    finally:
        _stop_server(server, t)


def test_unknown_key_returns_404(worker, sock_path):
    server = TagEditServer(sock_path, worker.work_q)
    t = _run_server(server)
    try:
        missing_key = (
            "customer=dexory/customer_site=london/robot=rob01/"
            "source=ros-bags/date=2026-06-01/does-not-exist.mcap"
        )
        status, data = _post(sock_path, {"key": missing_key, "set_tags": {"a": "b"}})
        assert status == 404
        assert "error" in data
    finally:
        _stop_server(server, t)


def test_malformed_body_returns_400(worker, sock_path):
    server = TagEditServer(sock_path, worker.work_q)
    t = _run_server(server)
    try:
        status, data = _post(sock_path, "not json at all")
        assert status == 400
        assert "error" in data
    finally:
        _stop_server(server, t)


def test_malformed_key_returns_400(worker, sock_path):
    server = TagEditServer(sock_path, worker.work_q)
    t = _run_server(server)
    try:
        status, data = _post(sock_path, {"key": "not-a-hive-key", "set_tags": {"a": "b"}})
        assert status == 400
        assert "error" in data
    finally:
        _stop_server(server, t)


def test_missing_key_field_returns_400(worker, sock_path):
    server = TagEditServer(sock_path, worker.work_q)
    t = _run_server(server)
    try:
        status, data = _post(sock_path, {"set_tags": {"a": "b"}})
        assert status == 400
        assert "error" in data
    finally:
        _stop_server(server, t)


def test_deadline_expired_item_is_not_applied(worker, sock_path):
    # A negative deadline_seconds means the computed deadline is already in the
    # past the instant the item is built — the worker MUST see it as expired
    # and skip the write, regardless of how fast it dequeues it.
    server = TagEditServer(sock_path, worker.work_q, deadline_seconds=-10.0)
    t = _run_server(server)
    try:
        status, data = _post(sock_path, {"key": KEY, "set_tags": {"late": "nope"}})
        assert status == 503
        assert data == {"error": "busy"}
    finally:
        _stop_server(server, t)

    # The write must NEVER have landed.
    row = worker.conn.execute(
        "SELECT COUNT(*) FROM tags_override WHERE key='late'"
    ).fetchone()
    assert row[0] == 0


def test_stale_socket_file_at_bind_is_replaced(worker, sock_path):
    # A genuine stale UNIX socket special file, left behind by a prior daemon
    # that bound it and crashed/exited without unlinking (never listening
    # anymore, but still a real AF_UNIX socket on disk) — this IS the "stale
    # socket" case TagEditServer must clobber and rebind over (S3).
    stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    stale.bind(sock_path)
    stale.close()
    assert os.path.exists(sock_path)
    assert stat.S_ISSOCK(os.stat(sock_path).st_mode)

    server = TagEditServer(sock_path, worker.work_q)
    t = _run_server(server)
    try:
        status, _ = _post(sock_path, {"key": KEY, "set_tags": {"ok": "yes"}})
        assert status == 200
    finally:
        _stop_server(server, t)


def test_regular_file_at_socket_path_refuses_to_bind(worker, sock_path):
    # S3: a REGULAR file already at the socket path is a conflict, not
    # "staleness" — it must never be silently clobbered; startup must fail
    # loudly instead, and the file must survive untouched.
    with open(sock_path, "wb") as f:
        f.write(b"not a socket at all, some unrelated file")

    with pytest.raises(RuntimeError):
        TagEditServer(sock_path, worker.work_q)

    assert os.path.exists(sock_path)
    with open(sock_path, "rb") as f:
        assert f.read() == b"not a socket at all, some unrelated file"


def test_directory_at_socket_path_refuses_to_bind(worker, sock_path):
    os.mkdir(sock_path)

    with pytest.raises(RuntimeError):
        TagEditServer(sock_path, worker.work_q)

    assert os.path.isdir(sock_path)


def test_close_skips_unlink_if_socket_was_replaced(worker, sock_path):
    # S3: if another daemon rebinds socket_path to a NEW socket after we
    # bound ours (e.g. a slow shutdown racing a fresh instance's startup),
    # our server_close() must not delete the replacement out from under it.
    server = TagEditServer(sock_path, worker.work_q)
    os.remove(sock_path)
    replacement = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    replacement.bind(sock_path)
    try:
        server.server_close()
        assert os.path.exists(sock_path)  # the replacement must survive
        assert stat.S_ISSOCK(os.stat(sock_path).st_mode)
    finally:
        replacement.close()
        os.remove(sock_path)


def test_socket_chmod_applied(worker, sock_path):
    server = TagEditServer(sock_path, worker.work_q)
    t = _run_server(server)
    try:
        mode = stat.S_IMODE(os.stat(sock_path).st_mode)
        assert mode == 0o660
    finally:
        _stop_server(server, t)


# --- B1: bounded IPC surface ------------------------------------------------


def test_oversized_body_rejected_with_413(worker, sock_path):
    # Declares a Content-Length just ABOVE the cap, with no actual body bytes
    # sent — the 413 must come from the header check alone (see
    # test_huge_content_length_rejected_without_reading_body for why an actual
    # http.client-driven send of a real >1MiB body isn't used here: the server
    # closes the connection right after its 413, and the client would still be
    # mid-`sendall()` of the (unread) body when that happens, racing a
    # BrokenPipeError on the client side instead of exercising the assertion).
    from mcap_catalog_builder.tag_ipc import MAX_BODY_BYTES

    server = TagEditServer(sock_path, worker.work_q)
    t = _run_server(server)
    try:
        req = (
            b"POST /update_tags HTTP/1.0\r\nContent-Type: application/json\r\n"
            b"Content-Length: " + str(MAX_BODY_BYTES + 1).encode("ascii") + b"\r\n\r\n"
        )
        status, _ = _raw_request(sock_path, req)
        assert status == 413
    finally:
        _stop_server(server, t)

    # The oversized write must never have landed.
    row = worker.conn.execute(
        "SELECT COUNT(*) FROM tags_override WHERE key='blob'"
    ).fetchone()
    assert row[0] == 0


def test_missing_content_length_rejected_with_400(worker, sock_path):
    server = TagEditServer(sock_path, worker.work_q)
    t = _run_server(server)
    try:
        req = b"POST /update_tags HTTP/1.0\r\nContent-Type: application/json\r\n\r\n"
        status, _ = _raw_request(sock_path, req)
        assert status == 400
    finally:
        _stop_server(server, t)


def test_malformed_content_length_rejected_with_400(worker, sock_path):
    server = TagEditServer(sock_path, worker.work_q)
    t = _run_server(server)
    try:
        req = (
            b"POST /update_tags HTTP/1.0\r\nContent-Type: application/json\r\n"
            b"Content-Length: notanumber\r\n\r\n"
        )
        status, _ = _raw_request(sock_path, req)
        assert status == 400
    finally:
        _stop_server(server, t)


def test_negative_content_length_rejected_with_400(worker, sock_path):
    server = TagEditServer(sock_path, worker.work_q)
    t = _run_server(server)
    try:
        req = (
            b"POST /update_tags HTTP/1.0\r\nContent-Type: application/json\r\n"
            b"Content-Length: -5\r\n\r\n"
        )
        status, _ = _raw_request(sock_path, req)
        assert status == 400
    finally:
        _stop_server(server, t)


def test_huge_content_length_rejected_without_reading_body(worker, sock_path):
    # A claimed Content-Length far above MAX_BODY_BYTES, with NO actual body
    # bytes following it. If the handler ever tried self.rfile.read(length)
    # before checking the cap, it would block waiting for bytes that never
    # arrive until the (10s) handler-socket timeout — this test's own 5s recv
    # timeout would then fire first and raise, failing loudly rather than
    # silently passing. A prompt 413 proves the cap is enforced BEFORE read.
    server = TagEditServer(sock_path, worker.work_q)
    t = _run_server(server)
    try:
        req = (
            b"POST /update_tags HTTP/1.0\r\nContent-Type: application/json\r\n"
            b"Content-Length: 999999999\r\n\r\n"
        )
        status, _ = _raw_request(sock_path, req, timeout=5.0)
        assert status == 413
    finally:
        _stop_server(server, t)


def test_backpressure_503_when_pending_cap_reached(sock_path):
    # A dedicated queue with no consumer draining it: nothing should ever be
    # enqueued here once the pending-edit budget is exhausted.
    q: queue.Queue = queue.Queue()
    server = TagEditServer(sock_path, q, max_pending=1)
    t = _run_server(server)
    try:
        # Simulate one already-in-flight edit by taking the budget directly
        # (the same non-blocking acquire the handler itself performs).
        assert server.pending_sem.acquire(blocking=False)
        try:
            status, data = _post(sock_path, {"key": KEY, "set_tags": {"x": "y"}})
            assert status == 503
            assert data == {"error": "busy"}
        finally:
            server.pending_sem.release()

        # Rejected BEFORE ever building/enqueueing a TagEditItem.
        assert q.qsize() == 0
    finally:
        _stop_server(server, t)


# --- S2: strict request validation ------------------------------------------


def test_set_tags_null_value_returns_400(worker, sock_path):
    server = TagEditServer(sock_path, worker.work_q)
    t = _run_server(server)
    try:
        status, data = _post(sock_path, {"key": KEY, "set_tags": {"env": None}})
        assert status == 400
        assert "error" in data
    finally:
        _stop_server(server, t)

    # A null value must NEVER create a NULL-mask override via the set path —
    # masks are only ever created through unset_keys of an embedded key.
    row = worker.conn.execute(
        "SELECT COUNT(*) FROM tags_override WHERE key='env'"
    ).fetchone()
    assert row[0] == 0


def test_set_tags_non_string_value_returns_400(worker, sock_path):
    server = TagEditServer(sock_path, worker.work_q)
    t = _run_server(server)
    try:
        status, data = _post(sock_path, {"key": KEY, "set_tags": {"count": 42}})
        assert status == 400
        assert "error" in data
    finally:
        _stop_server(server, t)


def test_unset_keys_non_string_element_returns_400(worker, sock_path):
    server = TagEditServer(sock_path, worker.work_q)
    t = _run_server(server)
    try:
        status, data = _post(sock_path, {"key": KEY, "unset_keys": [123]})
        assert status == 400
        assert "error" in data
    finally:
        _stop_server(server, t)


def test_unset_keys_not_a_list_returns_400(worker, sock_path):
    server = TagEditServer(sock_path, worker.work_q)
    t = _run_server(server)
    try:
        status, data = _post(sock_path, {"key": KEY, "unset_keys": "not-a-list"})
        assert status == 400
        assert "error" in data
    finally:
        _stop_server(server, t)


def test_unknown_top_level_fields_are_ignored(worker, sock_path):
    server = TagEditServer(sock_path, worker.work_q)
    t = _run_server(server)
    try:
        status, data = _post(
            sock_path, {"key": KEY, "set_tags": {"env": "prod"}, "bogus_field": "whatever"}
        )
        assert status == 200
        tags = {row["key"]: row["value"] for row in data["tags"]}
        assert tags["env"] == "prod"
    finally:
        _stop_server(server, t)


# --- S5(a): wholly-unknown-dimension 404 must not fabricate rows -----------


def test_unknown_dimension_names_return_404_without_inserting_dimension_rows(worker, sock_path):
    server = TagEditServer(sock_path, worker.work_q)
    t = _run_server(server)
    try:
        dim_tables = ("customers", "sites", "robots", "sources")
        before = {
            tbl: worker.conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            for tbl in dim_tables
        }
        ghost_key = (
            "customer=nosuchcustomer/customer_site=nosuchsite/robot=nosuchrobot/"
            "source=nosuchsource/date=2099-01-01/ghost.mcap"
        )
        status, data = _post(sock_path, {"key": ghost_key, "set_tags": {"a": "b"}})
        assert status == 404
        assert "error" in data

        after = {
            tbl: worker.conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            for tbl in dim_tables
        }
        assert after == before
    finally:
        _stop_server(server, t)
