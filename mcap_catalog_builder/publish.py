"""Atomic catalog publish: build to a temp DB, then swap it in (catalog-migration
§6.2a).

In-place WAL mutation (``db.open_db`` + a normal reconcile) stays the norm for an
existing, schema-compatible catalog — it is transactionally safe per file (the
composite-key upsert preserves ``files.id``, the wire ``file_id`` contract) even
though a reader can observe a mid-reconcile state. This module exists for the two
cases where that in-place norm is unsafe: **creating** a catalog that does not yet
exist, and **rebuilding** one from scratch. Mutating (or first creating) the SERVED
path directly would expose a reader to a torn, half-built catalog; replacing a WAL
database file while its old ``-wal`` sidecar survives is a documented SQLite
corruption vector (stale frames replayed into the new file's generation).

Protocol (``build_and_publish``):

1. Build the whole catalog in a temp file (``<served_path>.building``), never
   touching the served path.
2. Checkpoint the temp file down to zero WAL frames and close it, so it is a
   single self-contained file with no ``-wal``/``-shm`` sidecars. This is
   *verified*, not assumed: the checkpoint's own ``busy``/``log_frames``/
   ``checkpointed_frames`` result is checked (the same condition as step 3's
   gate); if it ever comes back partial (e.g. a future ``build_fn`` leaks a
   second connection to the temp path), the publish ABORTS rather than risk
   deleting a non-empty ``-wal`` and shipping a catalog missing committed frames.
3. If a served DB already exists (the rebuild case), checkpoint-gate it: force its
   ``-wal`` to zero frames before the swap, so no reader can later replay stale
   frames from the pre-swap generation into the post-swap file. If a concurrent
   reader holds it busy past a bounded window, ABORT — never rename over an
   un-checkpointed served DB. Otherwise (fresh create — no served DB yet), clear
   any orphaned ``<served_path>-wal``/``-shm`` sidecars now, BEFORE the rename
   (an operator may have deleted the main DB by hand but left them behind) — a
   reader must never be able to open the new main file next to a stale non-empty
   WAL from an unrelated prior generation.
4. ``os.replace`` the temp file onto the served path (atomic, same filesystem).
5. Unlink any stale ``-wal``/``-shm`` left over at the served path's old generation
   (belt-and-braces for the rebuild case — step 3 should already leave it at zero
   frames; a no-op in the create case, already handled by step 3).
6. ``fsync`` the containing directory.

Durability note: publish durability is rename-atomic + directory-fsync only — the
catalog is a rebuildable cache of the bucket (see ``CATALOG_CONTRACT.md``), so a
lost write beyond that boundary (e.g. true power loss) is acceptable; the next
build reconstructs it from the bucket.

The reader's swap trigger is (dev, inode) identity polling on the served path, NOT
``build_metadata.build_id`` — the ``build_id`` counter is a freshness/confirmation
value only (see the "Publish & reopen protocol" section of ``CATALOG_CONTRACT.md``).
"""

import logging
import os
import sqlite3
import time
from typing import Callable, TypeVar

from .db import Caches, load_caches, open_db

logger = logging.getLogger(__name__)

T = TypeVar("T")

_TEMP_SUFFIX = ".building"

# The bounded window (seconds) the checkpoint gate retries in before giving up.
_GATE_TOTAL_SECONDS = 5.0
_GATE_POLL_SECONDS = 0.1


class PublishBusyError(RuntimeError):
    """The old served DB could not be checkpoint-gated within the bounded window.

    A concurrent reader held it busy (an open read transaction/cursor) for the
    whole retry window. The publish is ABORTED: the served DB is logically
    untouched (no content change — the gate's own checkpoint attempt may still
    have copied already-safe frames into the main file, a harmless PASSIVE-style
    partial checkpoint), and the temp build is discarded. The caller should retry
    the whole build next cycle — never rename over a served DB whose ``-wal`` may
    still hold un-checkpointed frames.
    """


class TempCheckpointError(RuntimeError):
    """The freshly-built temp DB could not be checkpointed to zero WAL frames.

    This should not happen on the normal single-connection build path; it would
    only occur if ``build_fn`` (or something else) leaked a second connection to
    the temp path holding it busy, or a checkpoint otherwise failed to fully
    truncate. Trusting an un-checked checkpoint here and deleting the temp
    ``-wal`` regardless would risk silently publishing a catalog missing
    committed frames, so the whole publish is ABORTED instead: the temp build is
    discarded and the served DB (if any) is left completely untouched — the
    served-DB gate/swap never runs.
    """


class UnstampedBuildError(RuntimeError):
    """``build_fn`` returned without stamping ``build_metadata`` with a real build.

    Guards against publishing the placeholder row ``_seed_build_id_floor`` seeds
    into the temp DB (``build_outcome='seeded'``, ``last_build_ns=0``) as if it
    were a completed build. ``db.record_build`` always writes a nonzero
    ``last_build_ns`` (``db.now_ns()``), so a zero value here means ``build_fn``
    never called it (a broken or no-op ``build_fn``). The publish is ABORTED: the
    temp build is discarded and the served DB (if any) is left untouched.
    """


def _remove_if_exists(path: str) -> None:
    if os.path.exists(path):
        os.remove(path)


def _cleanup_temp(temp_path: str) -> None:
    """Remove a temp DB and its WAL sidecars (a leftover from a crashed prior run,
    or our own scratch file after a successful/aborted publish)."""
    for suffix in ("", "-wal", "-shm"):
        _remove_if_exists(temp_path + suffix)


def _read_old_build_id(served_path: str) -> int | None:
    """Best-effort read of ``build_metadata.build_id`` from the currently-served DB.

    Returns ``None`` if the path is absent, unreadable, or not a stamped auryn
    catalog — the seeding step below then just starts fresh at build_id=1.
    """
    if not os.path.exists(served_path):
        return None
    try:
        conn = sqlite3.connect(f"file:{served_path}?mode=ro", uri=True)
        try:
            row = conn.execute(
                "SELECT build_id FROM build_metadata WHERE id = 1"
            ).fetchone()
            return int(row[0]) if row is not None else None
        finally:
            conn.close()
    except sqlite3.Error:
        logger.warning("could not read build_id from %s (starting fresh)", served_path, exc_info=True)
        return None


def _seed_build_id_floor(conn: sqlite3.Connection, floor: int) -> None:
    """Pre-seed the (empty) temp DB's ``build_metadata`` row so the next
    ``record_build`` call — which does ``build_id = build_metadata.build_id + 1``
    on conflict — lands strictly above the old served DB's build_id.

    The placeholder row's non-``build_id`` columns are dummy values; a real
    ``record_build`` call always overwrites them via ``excluded.*``. Its
    ``build_outcome`` is deliberately ``'seeded'`` (never ``'ok'``) and its
    ``last_build_ns`` is ``0`` — so if ``build_fn`` returns without ever calling
    ``record_build``, ``_ensure_build_stamped`` below can tell the difference and
    fail the publish instead of shipping the placeholder as a real build.
    """
    conn.execute(
        "INSERT INTO build_metadata(id, build_id, last_build_ns, files_scanned, "
        "files_failed, build_outcome, builder_version) VALUES (1, ?, 0, 0, 0, 'seeded', '') "
        "ON CONFLICT(id) DO UPDATE SET build_id = excluded.build_id",
        (floor,),
    )
    conn.commit()


def _ensure_build_stamped(conn: sqlite3.Connection) -> None:
    """Fail fast if ``build_fn`` returned without a real ``db.record_build`` call.

    A fresh temp DB either has no ``build_metadata`` row yet, or (rebuild case)
    the ``_seed_build_id_floor`` placeholder with ``last_build_ns=0``. A real
    ``record_build`` call always overwrites ``last_build_ns`` with a nonzero
    ``db.now_ns()``, so that column is the stamp signal.
    """
    row = conn.execute("SELECT last_build_ns FROM build_metadata WHERE id = 1").fetchone()
    if row is None or row["last_build_ns"] == 0:
        raise UnstampedBuildError(
            "build_fn returned without stamping build_metadata (no db.record_build "
            "call landed) — publish aborted, temp build discarded."
        )


def _checkpoint_truncate(conn: sqlite3.Connection) -> tuple[int, int, int]:
    row = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    busy, log_frames, checkpointed_frames = row[0], row[1], row[2]
    return busy, log_frames, checkpointed_frames


def _gate_old_served_db(served_path: str) -> None:
    """Force the currently-served DB's ``-wal`` to zero frames before it is
    replaced, retrying for a bounded window if a reader holds it busy.

    Raises ``PublishBusyError`` (leaving the served DB logically untouched — no
    content change, though the checkpoint attempt itself may copy already-safe
    frames into the main file) if the gate cannot complete within the window —
    the caller must NEVER rename over a served DB whose WAL may still hold
    un-checkpointed frames (a reader could later replay those stale frames into
    the new file's generation).
    """
    deadline = time.monotonic() + _GATE_TOTAL_SECONDS
    last = None
    while True:
        conn = sqlite3.connect(served_path)
        try:
            busy, log_frames, checkpointed_frames = _checkpoint_truncate(conn)
        finally:
            conn.close()
        last = (busy, log_frames, checkpointed_frames)
        if busy == 0 and log_frames == checkpointed_frames:
            return
        if time.monotonic() >= deadline:
            raise PublishBusyError(
                f"checkpoint-gate on {served_path!r} did not complete within "
                f"{_GATE_TOTAL_SECONDS}s (last wal_checkpoint(TRUNCATE) result: "
                f"busy={last[0]} log_frames={last[1]} checkpointed_frames={last[2]}); "
                f"a reader is holding it busy. Publish aborted, served DB untouched."
            )
        time.sleep(_GATE_POLL_SECONDS)


def _fsync_dir(path: str) -> None:
    dir_fd = os.open(os.path.dirname(os.path.abspath(path)) or ".", os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def build_and_publish(
    served_path: str,
    build_fn: Callable[[sqlite3.Connection, Caches], T],
    *,
    logger: logging.Logger = logger,
) -> T:
    """Build a full catalog to a temp file, then atomically publish it as
    ``served_path`` (§6.2a). See the module docstring for the full protocol.

    ``build_fn(conn, caches)`` receives a freshly-opened temp DB (schema applied,
    empty caches) and must perform the whole catalog build, including stamping
    ``build_metadata`` (e.g. via ``reconcile.full_reconcile``, which calls
    ``db.record_build``). Its return value is returned unchanged.

    Raises ``PublishBusyError`` (rebuild case only) if the OLD served DB cannot be
    checkpoint-gated within the bounded window — the publish is aborted, the
    served DB is left completely untouched, and the temp build is discarded.
    Raises ``TempCheckpointError`` if the NEW temp DB itself cannot be
    checkpointed to zero WAL frames, or ``UnstampedBuildError`` if ``build_fn``
    returned without stamping ``build_metadata`` — both abort the publish with
    the temp build discarded and the served DB (if any) untouched. Any other
    exception raised by ``build_fn`` is also cleaned up (temp build discarded)
    and re-raised unchanged.
    """
    temp_path = served_path + _TEMP_SUFFIX
    _cleanup_temp(temp_path)  # clobber any leftover from a crashed prior run

    conn = open_db(temp_path)
    try:
        try:
            caches = load_caches(conn)
            floor = _read_old_build_id(served_path)
            if floor is not None:
                _seed_build_id_floor(conn, floor)
            result = build_fn(conn, caches)
            _ensure_build_stamped(conn)
            busy, log_frames, checkpointed_frames = _checkpoint_truncate(conn)
        finally:
            conn.close()
    except Exception:
        _cleanup_temp(temp_path)
        logger.exception("publish aborted: temp build failed for %s", served_path)
        raise

    if not (busy == 0 and log_frames == checkpointed_frames):
        _cleanup_temp(temp_path)
        msg = (
            f"temp build at {temp_path!r} could not be checkpointed to zero WAL "
            f"frames (wal_checkpoint(TRUNCATE): busy={busy} log_frames={log_frames} "
            f"checkpointed_frames={checkpointed_frames}) — publish aborted, temp "
            f"build discarded, served DB untouched."
        )
        logger.error(msg)
        raise TempCheckpointError(msg)
    # A verified-clean TRUNCATE checkpoint + close removes -wal/-shm; remove
    # defensively so the temp file is unconditionally self-contained before swap.
    _remove_if_exists(temp_path + "-wal")
    _remove_if_exists(temp_path + "-shm")

    served_exists = os.path.exists(served_path)
    if served_exists:
        try:
            _gate_old_served_db(served_path)
        except PublishBusyError:
            _cleanup_temp(temp_path)
            logger.error("publish aborted: %s is busy, could not checkpoint-gate", served_path)
            raise
    else:
        # Fresh create: no served DB to gate, but an orphaned sidecar could still
        # survive an operator deleting the main DB by hand. Clear it BEFORE the
        # rename below, so a reader can never observe the new main file next to a
        # stale non-empty WAL from an unrelated prior generation (the corruption
        # vector the module docstring warns about).
        _remove_if_exists(served_path + "-wal")
        _remove_if_exists(served_path + "-shm")

    os.replace(temp_path, served_path)
    # Stale sidecars of the REPLACED generation: the gate above already drove the
    # old file's -wal to zero frames, so this is belt-and-braces, not load-bearing
    # (and a no-op in the create case, already handled above).
    _remove_if_exists(served_path + "-wal")
    _remove_if_exists(served_path + "-shm")
    _fsync_dir(served_path)

    logger.info("published catalog to %s (rebuild=%s)", served_path, served_exists)
    return result
