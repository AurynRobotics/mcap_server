"""Tests for the full reconcile scan: index, dedup, warm no-op, deletion sweep."""

import os

from mcap_indexer.reconcile import full_reconcile, scan_disk
from mcap_indexer.tests.fixtures import write_minimal_mcap

CH = [("/a", "S", "ros2msg", 2), ("/b", "S", "ros2msg", 1), ("/zero", "S", "ros2msg", 0)]


def _hive(root, filename="x.mcap", channels=None, s3_key=None):
    dest = os.path.join(
        root,
        "customer=dexory",
        "customer_site=london",
        "robot=rob01",
        "source=ros-bags",
        "date=2026-06-01",
        filename,
    )
    write_minimal_mcap(dest, s3_key=s3_key, channels=channels or CH)
    return dest


def test_scan_disk_filters_temp_and_hidden(tmp_path):
    root = str(tmp_path)
    write_minimal_mcap(os.path.join(root, "good.mcap"))
    open(os.path.join(root, "partial.mcap.tmp"), "w").close()
    open(os.path.join(root, "x.part"), "w").close()
    open(os.path.join(root, ".hidden.mcap"), "w").close()
    open(os.path.join(root, "notes.txt"), "w").close()
    os.makedirs(os.path.join(root, ".git"))
    write_minimal_mcap(os.path.join(root, ".git", "buried.mcap"))
    assert [os.path.basename(p) for p in scan_disk(root)] == ["good.mcap"]


def test_reconcile_indexes_and_dedups(tmp_db, tmp_path):
    conn, caches = tmp_db
    root = str(tmp_path / "watch")
    _hive(root, filename="a.mcap")
    _hive(root, filename="b.mcap")  # same channel layout → one topic set
    tally = full_reconcile(conn, caches, root)
    assert tally["indexed"] == 2
    assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM topic_sets").fetchone()[0] == 1


def test_reconcile_warm_noop(tmp_db, tmp_path):
    conn, caches = tmp_db
    root = str(tmp_path / "watch")
    _hive(root, filename="a.mcap")
    full_reconcile(conn, caches, root)
    tally = full_reconcile(conn, caches, root)
    assert tally["indexed"] == 0 and tally["skipped"] == 1
    assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 1


def test_reconcile_deletes_removed(tmp_db, tmp_path):
    conn, caches = tmp_db
    root = str(tmp_path / "watch")
    a = _hive(root, filename="a.mcap")
    _hive(root, filename="b.mcap")
    full_reconcile(conn, caches, root)
    os.remove(a)
    tally = full_reconcile(conn, caches, root)
    assert tally["deleted"] == 1
    assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM topic_sets").fetchone()[0] == 1  # set survives
