"""The tag-edit IPC endpoint (catalog-migration §1.1 DECISION D2, option (a)).

The Go server is a read-only catalog reader; only this Python builder may write
the catalog. A client's ``UpdateTags`` wire RPC must therefore reach this
process somehow — D2(a) is: **Go forwards it over a local IPC endpoint; this
module applies it through the SAME single-writer queue everything else in
``__main__.py`` goes through.** The IPC server thread never touches the DB
itself — it only builds a :class:`TagEditItem` and enqueues it, then blocks on
its ``threading.Event`` for the worker thread's reply.

Wire shape: ``POST /update_tags`` over a UNIX domain socket, JSON body
``{"key": "<object key>", "set_tags": {...}, "unset_keys": [...]}``, JSON
response. See ``CATALOG_CONTRACT.md``'s "Tag-edit IPC" section for the full
contract (addressing rationale, deadline semantics, socket permissions).

**Bounded surface (B1).** This is a local IPC endpoint, not a public one, but
it is still reachable by anything with local access — so it is deliberately
bounded against a local DoS: request bodies are capped at ``MAX_BODY_BYTES``
(rejected with 413/400 *before* being read), each handler socket carries a
``HANDLER_TIMEOUT_SECONDS`` read/write timeout, and at most ``MAX_PENDING_EDITS``
tag edits may be enqueued-but-unreplied at once (a non-blocking semaphore
acquired *before* enqueueing; failure to acquire is an immediate 503 that never
touches the work queue). See :class:`TagEditServer`'s docstring for the full
rationale.
"""

import http.server
import json
import logging
import os
import socketserver
import stat
import threading
import time
from dataclasses import dataclass, field

from .db import lookup_file_id, update_tags
from .keyparse import parse_hive_key, rebuild_hive_key

logger = logging.getLogger(__name__)

# How long (seconds) a worker gets to act on an enqueued edit before it is
# considered expired (never applied late — see TagEditItem/handle_tag_edit).
DEFAULT_DEADLINE_SECONDS = 5.0

# The IPC thread waits on the reply event LONGER than the deadline it handed
# the worker (deadline_seconds + this margin), so a genuine worker reply
# (ok/not_found/expired) is always preferred over the IPC thread's own local
# wait timing out — a 503 caused by THIS timeout means the worker had not even
# STARTED processing the item by its deadline (handle_tag_edit's own deadline
# check will make it skip the write when it eventually dequeues it).
#
# Residual pathological case (S1): if the worker dequeues the item a hair
# before its deadline and then spends longer than this margin inside the one
# small UPDATE transaction (an unrealistically slow single-row write), the IPC
# thread can still time out and reply 503 even though the edit actually lands
# moments later. Concurrency caveat: a caller that retries after such a 503,
# and whose retry lands as a SECOND, later write, will simply overwrite the
# first under the existing ``tags_override`` last-writer-wins semantic (the
# same semantic that already governs two genuinely concurrent edits) — not a
# new hazard, just the existing one surfacing through a different trigger.
_WAIT_MARGIN_SECONDS = 2.0

# B1: request-body cap, enforced from Content-Length before any read — a
# tag edit is a handful of short strings; 1 MiB is generous headroom.
MAX_BODY_BYTES = 1 * 1024 * 1024

# B1: read/write timeout (seconds) applied to every handler socket, bounding
# how long a slow/stalled client can hold a handler thread open.
HANDLER_TIMEOUT_SECONDS = 10.0

# B1: the default cap on concurrently in-flight (enqueued, awaiting reply)
# TagEditItems — see TagEditServer.__init__'s ``max_pending`` parameter.
MAX_PENDING_EDITS = 32


@dataclass
class TagEditResult:
    """The worker thread's reply, read by the IPC thread after ``event`` fires."""

    status: str = "pending"  # "ok" | "not_found" | "expired" | "error"
    tags: list[dict] = field(default_factory=list)  # only meaningful when status == "ok"


@dataclass
class TagEditItem:
    """A result-bearing work-queue item: one client tag edit, forwarded from the
    IPC server to the single DB-writer thread. Mirrors ``WatchEvent`` (the
    other queue item shape) closely enough that ``worker_loop`` can tell them
    apart by ``kind`` alone, but carries a reply channel WatchEvent does not
    need since nothing waits on those.
    """

    kind: str = field(default="tag_edit", init=False)
    dims: dict[str, str] = field(default_factory=dict)
    set_tags: dict[str, str] = field(default_factory=dict)
    unset_keys: list[str] = field(default_factory=list)
    deadline: float = 0.0  # time.monotonic() past which the write must NOT apply
    event: threading.Event = field(default_factory=threading.Event)
    result: TagEditResult = field(default_factory=TagEditResult)


def handle_tag_edit(conn, caches, item: TagEditItem) -> None:
    """Single-writer-thread handling of one queued tag edit.

    Always sets ``item.event`` exactly once (success, business-rule outcome, or
    internal error) so the IPC thread's ``event.wait()`` can never hang forever.
    Never applies the write once ``item.deadline`` (monotonic) has passed — a
    caller that has already given up must never observe its edit land late.
    """
    try:
        if time.monotonic() > item.deadline:
            logger.warning(
                "tag-edit deadline expired before processing, dropping (dims=%r)", item.dims
            )
            item.result.status = "expired"
            return
        file_id = lookup_file_id(conn, item.dims)
        if file_id is None:
            item.result.status = "not_found"
            return
        update_tags(conn, file_id, item.set_tags, item.unset_keys)
        rows = conn.execute(
            "SELECT key, value, is_override FROM tags_effective WHERE file_id=? ORDER BY key",
            (file_id,),
        ).fetchall()
        item.result.tags = [
            {"key": r["key"], "value": r["value"], "is_override": bool(r["is_override"])}
            for r in rows
        ]
        item.result.status = "ok"
    except Exception:  # noqa: BLE001 - the worker must never die
        logger.exception("tag-edit failed (dims=%r)", item.dims)
        item.result.status = "error"
    finally:
        item.event.set()


class _TagEditHTTPHandler(http.server.BaseHTTPRequestHandler):
    """HTTP/1.1 semantics (via ``BaseHTTPRequestHandler``) over the UNIX socket
    ``TagEditServer`` binds. Only the ``self.server`` attributes it reads
    (``work_q``/``deadline_seconds``/``wait_timeout``/``pending_sem``) are
    UNIX-socket-specific; everything else is stock ``http.server``.
    """

    # Default protocol_version ("HTTP/1.0"): each response closes the connection
    # — simplest correct behavior for a small, low-QPS control-plane endpoint
    # (no keep-alive bookkeeping to get right across ThreadingMixIn workers).

    # B1: a per-connection socket timeout — StreamRequestHandler.setup() applies
    # this to the accepted socket before any read (covers both header and body
    # reads), so a slow/stalled client can hold a handler thread for at most
    # this long.
    timeout = HANDLER_TIMEOUT_SECONDS

    def log_message(self, fmt, *args) -> None:  # override: route to logging, not stderr
        logger.debug("tag-ipc: %s", fmt % args)

    def address_string(self) -> str:
        # BaseHTTPRequestHandler.address_string() indexes self.client_address[0],
        # which assumes an (ip, port) TCP peer. A UNIX stream socket's peer
        # address is typically "" (unconnected/anonymous), so that indexing
        # raises — override with a fixed, harmless label (only used for logging).
        return "unix-socket"

    def do_POST(self) -> None:
        if self.path != "/update_tags":
            self._reply(404, {"error": "not found"})
            return

        # B1: validate + bound Content-Length BEFORE reading any body bytes.
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            self._reply(400, {"error": "missing Content-Length"})
            return
        try:
            length = int(raw_length)
        except ValueError:
            self._reply(400, {"error": "malformed Content-Length"})
            return
        if length < 0:
            self._reply(400, {"error": "negative Content-Length"})
            return
        if length > MAX_BODY_BYTES:
            self._reply(413, {"error": "body too large"})
            return

        try:
            raw = self.rfile.read(length) if length else b""
        except OSError:
            # Handler-socket timeout (or other I/O failure) mid-read: the
            # client gets no reply, just a closed connection — there is
            # nothing left to reliably respond to.
            logger.warning("tag-ipc: body read failed or timed out, dropping connection")
            return

        try:
            body = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._reply(400, {"error": "malformed JSON body"})
            return
        if not isinstance(body, dict):
            self._reply(400, {"error": "body must be a JSON object"})
            return

        key = body.get("key")
        if not isinstance(key, str) or not key:
            self._reply(400, {"error": "missing/invalid 'key'"})
            return
        dims = parse_hive_key(key)
        if dims is None or rebuild_hive_key(dims) != key.lstrip("/"):
            self._reply(400, {"error": "unparseable key"})
            return

        # S2: strict field validation — "set_tags" must map str keys to str
        # values (a null/None value is rejected outright: masking an embedded
        # tag is only ever done via "unset_keys", never by "setting" null).
        set_tags = body.get("set_tags") or {}
        if not isinstance(set_tags, dict):
            self._reply(400, {"error": "'set_tags' must be an object"})
            return
        for k, v in set_tags.items():
            if not isinstance(k, str) or not isinstance(v, str):
                self._reply(400, {"error": f"'set_tags' entry {k!r} must map a string key to a string value"})
                return

        unset_keys = body.get("unset_keys") or []
        if not isinstance(unset_keys, list) or not all(isinstance(k, str) for k in unset_keys):
            self._reply(400, {"error": "'unset_keys' must be a list of strings"})
            return

        # B1: bound the number of concurrently in-flight edits. Acquired
        # NON-blocking before the item ever reaches work_q — an overloaded
        # server rejects immediately rather than growing the queue or piling
        # up handler threads blocked on event.wait().
        if not self.server.pending_sem.acquire(blocking=False):
            self._reply(503, {"error": "busy"})
            return
        try:
            item = TagEditItem(
                dims=dims,
                set_tags=set_tags,
                unset_keys=unset_keys,
                deadline=time.monotonic() + self.server.deadline_seconds,
            )
            self.server.work_q.put(item)

            if not item.event.wait(timeout=self.server.wait_timeout):
                # The worker never replied in time (e.g. mid a slow full reconcile).
                # The item may still be sitting in the queue or even applied moments
                # from now, but AS FAR AS THIS CALLER IS CONCERNED the request is
                # busy/failed — it must not block indefinitely.
                self._reply(503, {"error": "busy"})
                return

            status = item.result.status
            if status == "ok":
                self._reply(200, {"tags": item.result.tags})
            elif status == "not_found":
                self._reply(404, {"error": "file not found"})
            elif status == "expired":
                self._reply(503, {"error": "busy"})
            else:  # "error" (or any unexpected value) — an internal failure
                self._reply(500, {"error": "internal error"})
        finally:
            self.server.pending_sem.release()

    def _reply(self, code: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class TagEditServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    """A threaded HTTP server bound to a UNIX domain socket, serving only
    ``POST /update_tags`` (see :class:`_TagEditHTTPHandler`).

    **Bounded surface (B1, local-DoS hardening).** ``ThreadingMixIn`` spawns one
    thread per connection with no built-in cap, so three things bound the
    resulting resource usage: request bodies are capped at ``MAX_BODY_BYTES``
    (validated against ``Content-Length`` and rejected with 413/400 *before*
    any read); every handler socket carries a ``HANDLER_TIMEOUT_SECONDS``
    read/write timeout; and at most ``max_pending`` tag edits (default
    ``MAX_PENDING_EDITS``) may be enqueued-but-unreplied at once, enforced by a
    ``threading.BoundedSemaphore`` acquired NON-blocking in the handler
    *before* the item ever reaches ``work_q`` — acquisition failure is an
    immediate 503 that never enqueues, never spawns a ``TagEditItem``, and
    never leaves a handler thread parked on ``event.wait()``. Together these
    bound the number of pending ``TagEditItem``s (and de-facto the number of
    *useful* handler threads) regardless of how many connections a local
    client opens.

    **Socket lifecycle (S3).** A stale socket file left at ``socket_path`` by a
    prior daemon is unlinked before bind — but only after confirming via
    ``stat.S_ISSOCK`` that it really IS a UNIX socket special file; a regular
    file or directory already occupying that path is a conflict, not "staleness",
    and fails startup with a clear error rather than being silently clobbered.
    After bind, the socket is ``chmod``'d ``0o660`` (owner+group read/write only
    — the socket is a local-machine-only trust boundary, never the auth
    boundary; see the contract doc) and its freshly-bound inode is captured.
    ``server_close()`` re-``stat``s ``socket_path`` and unlinks it only if the
    inode still matches what we bound — if it changed (another daemon replaced
    the socket while we were still up, e.g. during a slow shutdown racing a new
    instance's startup), the unlink is skipped and logged instead of deleting
    that other daemon's live socket.
    """

    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        socket_path: str,
        work_q,
        deadline_seconds: float = DEFAULT_DEADLINE_SECONDS,
        max_pending: int = MAX_PENDING_EDITS,
    ) -> None:
        self.work_q = work_q
        self.deadline_seconds = deadline_seconds
        self.wait_timeout = max(deadline_seconds, 0.0) + _WAIT_MARGIN_SECONDS
        self.pending_sem = threading.BoundedSemaphore(max_pending)
        self.socket_path = socket_path
        if os.path.exists(socket_path):
            st = os.stat(socket_path)
            if not stat.S_ISSOCK(st.st_mode):
                raise RuntimeError(
                    f"refusing to bind tag-edit socket: {socket_path!r} already exists "
                    f"and is NOT a UNIX socket (mode={oct(stat.S_IMODE(st.st_mode))}) — "
                    f"a regular file/directory there is a conflict, not a stale socket; "
                    f"remove it manually if that is safe to do"
                )
            logger.warning("removing stale tag-edit socket at %s", socket_path)
            os.remove(socket_path)
        super().__init__(socket_path, _TagEditHTTPHandler)
        os.chmod(socket_path, 0o660)
        # Captured post-bind so server_close() can verify it is still OUR
        # socket before unlinking (S3) — see the class docstring.
        self._bound_ino = os.stat(socket_path).st_ino

    def handle_error(self, request, client_address) -> None:  # override: route to logging
        logger.exception("tag-ipc: unhandled error handling request from %s", client_address)

    def server_close(self) -> None:
        super().server_close()
        try:
            current_ino = os.stat(self.socket_path).st_ino
        except FileNotFoundError:
            return
        if current_ino != self._bound_ino:
            logger.warning(
                "tag-edit socket at %s no longer refers to the socket we bound "
                "(inode changed) — skipping unlink; another daemon may have replaced it",
                self.socket_path,
            )
            return
        os.remove(self.socket_path)
