"""Single-writer PROCESS lock for the catalog builder (CATALOG_CONTRACT.md §11).

The whole catalog design rests on "the builder is the SOLE writer of the served
DB" — but before this lock that was a deploy convention, not an enforced
invariant: a second builder pointed at the same ``--db`` would happily start,
treat the first one's LIVE tag-edit socket as stale and unlink it, and the two
reconcilers would interleave writes (and tag edits would silently route to
whichever daemon bound the socket last).

This module turns the convention into a kernel-enforced invariant:

- The lock is ``flock(LOCK_EX | LOCK_NB)`` on ``<db_path>.writer.lock`` — a
  sidecar file NEXT to the served DB, so it keys the lock to exactly the
  resource being protected (two builders on *different* DB paths are fine, e.g.
  the smoke harness's own DB alongside an interactive one).
- It is acquired ONCE at startup, before any DB write or tag-socket bind, in
  BOTH daemon and ``--once`` modes (a ``--once --rebuild`` racing a live daemon
  is precisely the double-writer hazard).
- It is held for the process lifetime. The kernel releases a flock on ANY
  process death — crash, SIGKILL, clean exit — so there is no stale-lock
  recovery problem (the classic pidfile failure mode). The holder's PID is
  written into the file purely as a diagnostic for the "who has it?" error.
- The lock file is deliberately NEVER unlinked: unlink-on-release reintroduces
  a race (one process can lock a just-orphaned inode while another creates and
  locks a fresh file at the same path — two "holders"). A leftover 20-byte
  ``.writer.lock`` next to the DB is harmless and self-describing.
- ``flock`` is reliable only on a local filesystem — which the catalog volume
  already MUST be (CATALOG_CONTRACT.md §9: SQLite WAL forbids NFS/EFS).

The atomic-publish path (``--rebuild``: temp build + os.replace onto the served
path) does not touch the sidecar, so the lock survives a publish unchanged.
"""

import errno
import fcntl
import logging
import os

logger = logging.getLogger(__name__)

# Suffix appended to the served DB path to form the lock-file path.
LOCK_SUFFIX = ".writer.lock"


class WriterLockError(RuntimeError):
    """Another builder process already holds the writer lock for this DB."""


class WriterLock:
    """A held single-writer lock. ``release()`` is idempotent; the kernel also
    releases automatically on process exit."""

    def __init__(self, path: str, fd: int) -> None:
        self.path = path
        self._fd: int | None = fd

    def release(self) -> None:
        if self._fd is None:
            return
        fd, self._fd = self._fd, None
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def acquire_writer_lock(db_path: str) -> WriterLock:
    """Acquire the single-writer lock for ``db_path`` or raise WriterLockError.

    Non-blocking by design: a conflict means another builder is LIVE on this DB
    right now, and the correct behavior is to fail fast with a clear message —
    never to queue up behind it (two writers taking turns is still two writers).
    """
    lock_path = db_path + LOCK_SUFFIX
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as e:
        holder = "unknown pid"
        try:
            data = os.pread(fd, 64, 0).decode("ascii", "replace").strip()
            if data:
                holder = f"pid {data}"
        except OSError:
            pass
        os.close(fd)
        if e.errno in (errno.EAGAIN, errno.EACCES):
            raise WriterLockError(
                f"another catalog builder ({holder}) already holds the writer lock "
                f"{lock_path!r} — the catalog has exactly ONE writer per served DB; "
                f"stop the other builder (or point --db elsewhere) and retry"
            ) from None
        raise
    # Held. Record our PID for the conflict diagnostic above (best-effort).
    try:
        os.ftruncate(fd, 0)
        os.pwrite(fd, f"{os.getpid()}\n".encode("ascii"), 0)
    except OSError:
        pass
    logger.debug("writer lock held: %s (pid %d)", lock_path, os.getpid())
    return WriterLock(lock_path, fd)
