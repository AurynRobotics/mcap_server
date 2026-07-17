"""Tests for the single-writer process lock (CATALOG_CONTRACT.md §11).

The catalog's whole design assumes ONE builder process per served DB path (the
"sole writer"). Before the lock, that was a deploy convention only: a second
daemon would treat the first one's live tag socket as stale, remove it, and two
reconcilers would race writes to the same DB. The lock makes the convention a
kernel-enforced invariant: flock(LOCK_EX|LOCK_NB) on `<db>.writer.lock`, held
for the process lifetime, auto-released by the kernel on ANY process death (no
stale-pidfile hazard).
"""

import os
import subprocess
import sys
import textwrap

import pytest

from mcap_catalog_builder.__main__ import main
from mcap_catalog_builder.writer_lock import WriterLockError, acquire_writer_lock


def test_second_acquire_fails_with_holder_pid(tmp_path):
    db = str(tmp_path / "catalog.db")
    lock = acquire_writer_lock(db)
    try:
        with pytest.raises(WriterLockError) as exc:
            acquire_writer_lock(db)
        # The error must name the lock path and the holder pid (diagnostics).
        assert str(os.getpid()) in str(exc.value)
        assert db + ".writer.lock" in str(exc.value)
    finally:
        lock.release()


def test_release_allows_reacquire(tmp_path):
    db = str(tmp_path / "catalog.db")
    lock = acquire_writer_lock(db)
    lock.release()
    lock.release()  # idempotent
    lock2 = acquire_writer_lock(db)
    lock2.release()


def test_lock_released_on_process_death(tmp_path):
    """The kernel releases flock on ANY exit path — acquire in a subprocess
    that dies WITHOUT releasing, then acquire here."""
    db = str(tmp_path / "catalog.db")
    code = textwrap.dedent(
        f"""
        from mcap_catalog_builder.writer_lock import acquire_writer_lock
        acquire_writer_lock({db!r})
        # exit without release(): the kernel must drop the flock
        """
    )
    env = dict(os.environ, PYTHONPATH=os.getcwd())
    subprocess.run([sys.executable, "-c", code], check=True, env=env)
    lock = acquire_writer_lock(db)
    lock.release()


def test_different_db_paths_do_not_conflict(tmp_path):
    a = acquire_writer_lock(str(tmp_path / "a.db"))
    b = acquire_writer_lock(str(tmp_path / "b.db"))
    a.release()
    b.release()


def test_cli_once_refuses_while_lock_held(tmp_path):
    """A --once run against a DB whose writer lock is held must fail fast with
    exit code 3 (the double-writer hazard: e.g. a --rebuild racing a live
    daemon), touching neither the DB nor the tag socket."""
    root = tmp_path / "lake"
    root.mkdir()
    db = str(tmp_path / "catalog.db")

    lock = acquire_writer_lock(db)
    try:
        rc = main([str(root), "--db", db, "--once"])
        assert rc == 3
        assert not os.path.exists(db), "the locked-out builder must not touch the DB"
    finally:
        lock.release()

    # With the lock released the same invocation succeeds.
    assert main([str(root), "--db", db, "--once"]) == 0
    assert os.path.exists(db)
