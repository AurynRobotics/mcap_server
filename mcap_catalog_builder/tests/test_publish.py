"""Tests for the atomic catalog publish protocol (catalog-migration §6.2a):
build to a temp DB, checkpoint-gate, then rename into place."""

import os
import sqlite3

import pytest

import mcap_catalog_builder.publish as publish_mod
from mcap_catalog_builder.db import load_caches, lookup_file_id, open_db, update_tags
from mcap_catalog_builder.publish import (
    PublishBusyError,
    TempCheckpointError,
    UnstampedBuildError,
    build_and_publish,
)
from mcap_catalog_builder.reconcile import full_reconcile
from mcap_catalog_builder.tests.fixtures import write_minimal_mcap

CH = [("/a", "S", "ros2msg", 2), ("/b", "S", "ros2msg", 1)]


def _hive(root, filename="x.mcap", channels=None):
    dest = os.path.join(
        root,
        "customer=dexory",
        "customer_site=london",
        "robot=rob01",
        "source=ros-bags",
        "date=2026-06-01",
        filename,
    )
    write_minimal_mcap(dest, channels=channels or CH)
    return dest


def _build_fn_for(root):
    return lambda conn, caches: full_reconcile(conn, caches, root)


def test_fresh_create_publishes_complete_db(tmp_path):
    served = str(tmp_path / "catalog.db")
    root = str(tmp_path / "watch")
    _hive(root, "a.mcap")
    _hive(root, "b.mcap")

    tally = build_and_publish(served, _build_fn_for(root))

    assert tally["cataloged"] == 2
    assert os.path.exists(served)
    assert not os.path.exists(served + ".building")
    assert not os.path.exists(served + ".building-wal")
    assert not os.path.exists(served + ".building-shm")

    # open_db must be able to (re)open the published DB normally.
    conn = open_db(served)
    try:
        assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 2
        row = conn.execute("SELECT build_id FROM build_metadata WHERE id=1").fetchone()
        assert row["build_id"] == 1
    finally:
        conn.close()


def test_rebuild_over_existing_bumps_build_id_and_swaps_inode(tmp_path):
    served = str(tmp_path / "catalog.db")
    root = str(tmp_path / "watch")
    _hive(root, "a.mcap")

    # Build an initial served DB (N builds happen: build once, then reconcile in
    # place a second time to get build_id=2, so we can assert strictly-greater).
    build_and_publish(served, _build_fn_for(root))
    conn = open_db(served)
    from mcap_catalog_builder.db import load_caches
    caches = load_caches(conn)
    full_reconcile(conn, caches, root)  # bumps build_id to 2 in place
    conn.close()

    old_conn = sqlite3.connect(served)
    old_build_id = old_conn.execute(
        "SELECT build_id FROM build_metadata WHERE id=1"
    ).fetchone()[0]
    old_inode = os.stat(served).st_ino
    old_conn.close()
    assert old_build_id == 2

    # Now rebuild from scratch with a different (larger) corpus.
    _hive(root, "c.mcap")
    tally = build_and_publish(served, _build_fn_for(root))
    assert tally["cataloged"] == 2  # a.mcap + c.mcap, freshly re-cataloged

    new_inode = os.stat(served).st_ino
    assert new_inode != old_inode  # the file itself was replaced, not mutated

    assert not os.path.exists(served + "-wal")
    assert not os.path.exists(served + "-shm")
    assert not os.path.exists(served + ".building")

    conn = open_db(served)
    try:
        assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 2
        row = conn.execute("SELECT build_id FROM build_metadata WHERE id=1").fetchone()
        assert row["build_id"] > old_build_id
    finally:
        conn.close()


def test_busy_gate_aborts_and_leaves_served_db_untouched(tmp_path):
    served = str(tmp_path / "catalog.db")
    root = str(tmp_path / "watch")
    _hive(root, "a.mcap")

    build_and_publish(served, _build_fn_for(root))

    # Put a pending WAL frame in place (a plain reconnect+write; the served DB was
    # left fully checkpointed by the publish above, so with nothing pending, a
    # checkpoint attempt trivially succeeds regardless of readers). Keep this
    # connection OPEN (SQLite auto-checkpoints on the last connection closing,
    # which would empty the WAL again before the gate ever runs).
    writer = sqlite3.connect(served)
    writer.execute("UPDATE build_metadata SET files_scanned = files_scanned + 1 WHERE id=1")
    writer.commit()

    old_inode = os.stat(served).st_ino
    old_files = writer.execute(
        "SELECT filename FROM files ORDER BY filename"
    ).fetchall()
    old_build_row = writer.execute("SELECT * FROM build_metadata WHERE id=1").fetchone()

    # Hold the served DB busy: a second connection with an open read transaction
    # and an unfinished cursor over the pending WAL frame (empirically verified to
    # make wal_checkpoint(TRUNCATE) report busy=1 — see publish.py's gate).
    blocker = sqlite3.connect(served)
    blocker.execute("BEGIN")
    cur = blocker.execute("SELECT * FROM files")
    cur.fetchone()

    # Self-validating (S4): probe the contention DIRECTLY, not through
    # build_and_publish, so this test can never pass vacuously. If this
    # SQLite build/platform doesn't actually report checkpoint busy-ness from
    # the blocker's open read transaction, skip rather than assert on a
    # gate that was never really exercised.
    probe = sqlite3.connect(served)
    try:
        probe_busy, _, _ = probe.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    finally:
        probe.close()
    if probe_busy != 1:
        blocker.rollback()
        blocker.close()
        pytest.skip(
            f"direct wal_checkpoint(TRUNCATE) returned busy={probe_busy}, not 1 — "
            "this SQLite build/platform did not reproduce checkpoint contention "
            "from an open read transaction, so the busy-gate path cannot be "
            "exercised here"
        )

    try:
        with pytest.raises(PublishBusyError):
            build_and_publish(served, _build_fn_for(root))
    finally:
        blocker.rollback()
        blocker.close()

    # Served DB completely untouched: same inode, same logical content. (A byte-
    # for-byte file comparison would be too strict here — SQLite's checkpoint
    # gate itself does a best-effort PASSIVE-style copy of already-safe frames
    # into the main db file even when it cannot fully TRUNCATE, which can shuffle
    # bytes without changing any row's content.)
    assert os.stat(served).st_ino == old_inode
    assert writer.execute("SELECT filename FROM files ORDER BY filename").fetchall() == old_files
    assert writer.execute("SELECT * FROM build_metadata WHERE id=1").fetchone() == old_build_row
    writer.close()

    # Temp build discarded.
    assert not os.path.exists(served + ".building")
    assert not os.path.exists(served + ".building-wal")
    assert not os.path.exists(served + ".building-shm")


def test_leftover_building_file_from_crash_is_clobbered(tmp_path):
    served = str(tmp_path / "catalog.db")
    root = str(tmp_path / "watch")
    _hive(root, "a.mcap")

    with open(served + ".building", "wb") as f:
        f.write(b"not a real sqlite file, left over from a crashed build")
    with open(served + ".building-wal", "wb") as f:
        f.write(b"garbage")
    with open(served + ".building-shm", "wb") as f:
        f.write(b"garbage")

    tally = build_and_publish(served, _build_fn_for(root))
    assert tally["cataloged"] == 1
    assert os.path.exists(served)
    assert not os.path.exists(served + ".building")
    assert not os.path.exists(served + ".building-wal")
    assert not os.path.exists(served + ".building-shm")

    conn = open_db(served)
    try:
        assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 1
    finally:
        conn.close()


def test_temp_checkpoint_busy_aborts_publish(tmp_path, monkeypatch):
    """B1: a partial/busy checkpoint on the TEMP db (e.g. a leaked second
    connection to the temp path) must abort the publish, never be trusted and
    have its -wal deleted regardless."""
    served = str(tmp_path / "catalog.db")
    root = str(tmp_path / "watch")
    _hive(root, "a.mcap")

    monkeypatch.setattr(
        publish_mod, "_checkpoint_truncate", lambda conn: (1, 5, 3)  # busy, partial
    )

    with pytest.raises(TempCheckpointError):
        build_and_publish(served, _build_fn_for(root))

    # Served DB was never created; temp build fully discarded.
    assert not os.path.exists(served)
    assert not os.path.exists(served + ".building")
    assert not os.path.exists(served + ".building-wal")
    assert not os.path.exists(served + ".building-shm")


def test_fresh_create_clears_stale_served_sidecars_before_replace(tmp_path, monkeypatch):
    """B2: when the served main DB is absent but an orphaned -wal survives
    (operator deleted the main db by hand), it must be cleared BEFORE the temp
    file is renamed onto the served path — never after — so a reader can never
    observe the new main db coexisting with a stale non-empty WAL."""
    served = str(tmp_path / "catalog.db")
    root = str(tmp_path / "watch")
    _hive(root, "a.mcap")

    with open(served + "-wal", "wb") as f:
        f.write(b"stale wal frames from an unrelated prior generation")

    real_replace = os.replace

    def spy_replace(src, dst):
        # Pin the ordering: the stale sidecar must already be gone by the time
        # we expose the new main file via rename.
        assert not os.path.exists(served + "-wal")
        assert not os.path.exists(served + "-shm")
        return real_replace(src, dst)

    monkeypatch.setattr(publish_mod.os, "replace", spy_replace)

    tally = build_and_publish(served, _build_fn_for(root))
    assert tally["cataloged"] == 1
    assert os.path.exists(served)
    assert not os.path.exists(served + "-wal")
    assert not os.path.exists(served + "-shm")


def test_build_fn_that_never_stamps_aborts_publish(tmp_path):
    """S1: a build_fn that never calls db.record_build (e.g. a no-op / broken
    build_fn) must never have its placeholder/empty build_metadata row
    published as a real build."""
    served = str(tmp_path / "catalog.db")

    def noop_build_fn(conn, caches):
        return "should never be returned"

    with pytest.raises(UnstampedBuildError):
        build_and_publish(served, noop_build_fn)

    assert not os.path.exists(served)
    assert not os.path.exists(served + ".building")
    assert not os.path.exists(served + ".building-wal")
    assert not os.path.exists(served + ".building-shm")


_DIMS_A = {
    "customer": "dexory", "site": "london", "robot": "rob01",
    "source": "ros-bags", "date": "2026-06-01", "filename": "a.mcap",
}
_DIMS_B = {**_DIMS_A, "filename": "b.mcap"}


def test_rebuild_carries_forward_tags_override_by_composite_identity(tmp_path):
    """A user-authored override (including a NULL mask) is NOT derivable from
    the bucket, so it must survive a --rebuild when the same file (by composite
    identity) is present in the new build — even though files.id is renumbered."""
    served = str(tmp_path / "catalog.db")
    root = str(tmp_path / "watch")
    _hive(root, "a.mcap")

    build_and_publish(served, _build_fn_for(root))

    conn = open_db(served)
    file_id = lookup_file_id(conn, _DIMS_A)
    assert file_id is not None
    update_tags(conn, file_id, set_kv={"env": "prod"})
    # A NULL-valued override (masking an embedded tag) — inserted directly since
    # derive_tags() is currently a stub with no embedded tags to mask in practice.
    with conn:
        conn.execute(
            "INSERT INTO tags_override(file_id, key, value, updated_at) VALUES (?, 'hidden', NULL, ?)",
            (file_id, 424242),
        )
    conn.close()

    build_and_publish(served, _build_fn_for(root))  # --rebuild, same corpus

    conn = open_db(served)
    try:
        new_file_id = lookup_file_id(conn, _DIMS_A)
        assert new_file_id is not None
        rows = {
            r["key"]: (r["value"], r["updated_at"])
            for r in conn.execute(
                "SELECT key, value, updated_at FROM tags_override WHERE file_id=?", (new_file_id,)
            )
        }
        assert rows["env"] == ("prod", rows["env"][1])
        assert rows["hidden"] == (None, 424242)  # value AND updated_at both preserved
    finally:
        conn.close()


def test_rebuild_drops_tags_override_for_file_no_longer_present(tmp_path, caplog):
    """An override whose file no longer exists in the new build has nothing to
    attach to and must be DROPPED, not carried forward under a stale identity."""
    served = str(tmp_path / "catalog.db")
    root = str(tmp_path / "watch")
    _hive(root, "a.mcap")
    b_path = _hive(root, "b.mcap")

    build_and_publish(served, _build_fn_for(root))

    conn = open_db(served)
    b_id = lookup_file_id(conn, _DIMS_B)
    assert b_id is not None
    update_tags(conn, b_id, set_kv={"note": "gone-soon"})
    conn.close()

    os.remove(b_path)  # b.mcap no longer in the corpus

    with caplog.at_level("INFO"):
        build_and_publish(served, _build_fn_for(root))
    assert "1 dropped" in caplog.text or "dropped" in caplog.text
    # N1: the aggregate count alone isn't enough to act on — the dropped
    # composite identity + tag key must also be logged individually so an
    # operator can tell WHICH override(s) were lost.
    assert "b.mcap" in caplog.text
    assert "note" in caplog.text

    conn = open_db(served)
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM files WHERE filename='b.mcap'"
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM tags_override WHERE key='note'"
        ).fetchone()[0] == 0
    finally:
        conn.close()


def test_corrupt_served_db_is_treated_as_garbage_and_replaced(tmp_path, caplog):
    """S4: _gate_old_served_db can raise a raw sqlite3.Error (not
    PublishBusyError) when the served path is genuinely corrupt / not a
    database at all — that must not abort build_and_publish. No verified
    reader could be usefully attached to a corrupt DB anyway, so it is
    treated as garbage: warn, clear its -wal/-shm BEFORE the rename, and
    let the publish proceed."""
    served = str(tmp_path / "catalog.db")
    root = str(tmp_path / "watch")
    _hive(root, "a.mcap")

    with open(served, "wb") as f:
        f.write(b"not a real sqlite file at all -- corrupt garbage bytes")
    with open(served + "-wal", "wb") as f:
        f.write(b"garbage wal bytes from an unrelated prior generation")

    with caplog.at_level("WARNING"):
        tally = build_and_publish(served, _build_fn_for(root))
    assert "unusable" in caplog.text or "garbage" in caplog.text

    assert tally["cataloged"] == 1
    assert os.path.exists(served)
    assert not os.path.exists(served + "-wal")
    assert not os.path.exists(served + "-shm")
    assert not os.path.exists(served + ".building")

    conn = open_db(served)
    try:
        assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 1
    finally:
        conn.close()


_DIMS_ACME_A = {
    "customer": "acme", "site": "london", "robot": "rob01",
    "source": "ros-bags", "date": "2026-06-01", "filename": "a.mcap",
}


def test_rebuild_carry_forward_disambiguates_same_filename_different_customer(tmp_path):
    """S5(b): two files that share filename+date but live under DIFFERENT
    customers must not be confused during carry-forward — an override set on
    one must reattach to THAT file only after a rebuild, never bleed onto
    the other file sharing its filename/date."""
    served = str(tmp_path / "catalog.db")
    root = str(tmp_path / "watch")
    _hive(root, "a.mcap")  # customer=dexory (see _hive's fixed dims)
    acme_dest = os.path.join(
        root, "customer=acme", "customer_site=london", "robot=rob01",
        "source=ros-bags", "date=2026-06-01", "a.mcap",
    )
    write_minimal_mcap(acme_dest, channels=CH)

    build_and_publish(served, _build_fn_for(root))

    conn = open_db(served)
    dexory_id = lookup_file_id(conn, _DIMS_A)
    acme_id = lookup_file_id(conn, _DIMS_ACME_A)
    assert dexory_id is not None
    assert acme_id is not None
    assert dexory_id != acme_id
    update_tags(conn, dexory_id, set_kv={"only_dexory": "yes"})
    conn.close()

    build_and_publish(served, _build_fn_for(root))  # --rebuild, same corpus

    conn = open_db(served)
    try:
        new_dexory_id = lookup_file_id(conn, _DIMS_A)
        new_acme_id = lookup_file_id(conn, _DIMS_ACME_A)
        dexory_tags = {
            r["key"]: r["value"]
            for r in conn.execute(
                "SELECT key, value FROM tags_override WHERE file_id=?", (new_dexory_id,)
            )
        }
        acme_tags = {
            r["key"]: r["value"]
            for r in conn.execute(
                "SELECT key, value FROM tags_override WHERE file_id=?", (new_acme_id,)
            )
        }
        assert dexory_tags == {"only_dexory": "yes"}
        assert acme_tags == {}
    finally:
        conn.close()


def test_rebuild_with_unreadable_old_db_still_publishes(tmp_path, caplog):
    """The carry-forward step is best-effort (mirrors _read_old_build_id): an
    old served DB that cannot be read for tags_override must not fail the
    publish — it just carries forward nothing."""
    served = str(tmp_path / "catalog.db")
    root = str(tmp_path / "watch")
    _hive(root, "a.mcap")

    # A valid SQLite file (so the pre-existing checkpoint-gate step tolerates
    # it), but with none of the auryn tables — the carry-forward SELECT will
    # raise sqlite3.OperationalError, which must be swallowed (best-effort).
    raw = sqlite3.connect(served)
    raw.execute("CREATE TABLE dummy(x)")
    raw.commit()
    raw.close()

    with caplog.at_level("WARNING"):
        tally = build_and_publish(served, _build_fn_for(root))
    assert "tags_override" in caplog.text

    assert tally["cataloged"] == 1
    conn = open_db(served)
    try:
        assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 1
    finally:
        conn.close()
