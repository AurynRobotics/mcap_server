"""Tests for the full reconcile scan: catalog, dedup, warm no-op, deletion sweep."""

import os

from mcap_catalog_builder.db import load_caches, open_db
from mcap_catalog_builder.reconcile import full_reconcile, scan_disk
from mcap_catalog_builder.storage import LocalSource
from mcap_catalog_builder.tests.fixtures import write_minimal_mcap
from mcap_catalog_builder.varint import decode_counts_blob

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


def test_reconcile_catalogs_and_dedups(tmp_db, tmp_path):
    conn, caches = tmp_db
    root = str(tmp_path / "watch")
    _hive(root, filename="a.mcap")
    _hive(root, filename="b.mcap")  # same channel layout → one topic set
    tally = full_reconcile(conn, caches, root)
    assert tally["cataloged"] == 2
    assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM topic_sets").fetchone()[0] == 1


def test_reconcile_warm_noop(tmp_db, tmp_path):
    conn, caches = tmp_db
    root = str(tmp_path / "watch")
    _hive(root, filename="a.mcap")
    full_reconcile(conn, caches, root)
    tally = full_reconcile(conn, caches, root)
    assert tally["cataloged"] == 0 and tally["skipped"] == 1
    assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 1


def _catalog_snapshot(conn) -> dict:
    """An order-independent view of the catalog: per file, its dims + fingerprint +
    ``{topic_name: count}``. Independent of lookup-id assignment order, so a parallel
    build and a sequential build of the same corpus must produce EQUAL snapshots
    even though their internal ids may be numbered differently."""
    snap: dict = {}
    for f in conn.execute(
        "SELECT cu.name c, si.name s, ro.name r, f.date d, f.filename fn, "
        "f.start_time_ns st, f.end_time_ns et, f.chunk_count ch, "
        "f.topic_set_id tsid, f.topic_counts tc "
        "FROM files f JOIN customers cu ON cu.id=f.customer_id "
        "JOIN sites si ON si.id=f.site_id JOIN robots ro ON ro.id=f.robot_id"
    ).fetchall():
        member_ids = [
            row[0] for row in conn.execute(
                "SELECT topic_id FROM topic_set_members WHERE set_id=? ORDER BY topic_id",
                (f["tsid"],),
            ).fetchall()
        ]
        counts = decode_counts_blob(f["tc"])
        topics = {}
        for tid, cnt in zip(member_ids, counts):
            name = conn.execute("SELECT name FROM topic_names WHERE id=?", (tid,)).fetchone()[0]
            topics[name] = cnt
        snap[(f["c"], f["s"], f["r"], f["d"], f["fn"])] = (f["st"], f["et"], f["ch"], topics)
    return snap


def test_reconcile_parallel_matches_sequential(tmp_path):
    """workers>1 parallelizes only the summary reads; the resulting catalog must be
    byte-for-byte equivalent (content-wise) to the sequential build."""
    layouts = [
        [("/a", "S", "ros2msg", 2), ("/b", "S", "ros2msg", 1)],
        [("/a", "S", "ros2msg", 2), ("/b", "S", "ros2msg", 1)],  # dup layout -> shared set
        [("/x", "T", "ros2msg", 3)],
        [("/a", "S", "ros2msg", 1), ("/c", "U", "ros2msg", 4), ("/z", "S", "ros2msg", 0)],
    ]

    def build(root):
        for i, ch in enumerate(layouts):
            _hive(root, filename=f"f{i}.mcap", channels=ch)

    build(str(tmp_path / "seq"))
    build(str(tmp_path / "par"))
    cs = open_db(str(tmp_path / "seq.db"))
    cp = open_db(str(tmp_path / "par.db"))
    try:
        t_seq = full_reconcile(cs, load_caches(cs), str(tmp_path / "seq"), workers=1)
        t_par = full_reconcile(cp, load_caches(cp), str(tmp_path / "par"), workers=4)
        assert t_seq["cataloged"] == t_par["cataloged"] == len(layouts)
        assert _catalog_snapshot(cs) == _catalog_snapshot(cp)
        assert (
            cs.execute("SELECT COUNT(*) FROM topic_sets").fetchone()[0]
            == cp.execute("SELECT COUNT(*) FROM topic_sets").fetchone()[0]
        )
    finally:
        cs.close()
        cp.close()


class _ReadCountingSource(LocalSource):
    """A LocalSource that records every ``open_summary`` (the body-region read)."""

    def __init__(self, root: str) -> None:
        super().__init__(root)
        self.reads: list[str] = []

    def open_summary(self, key: str, size: int):
        self.reads.append(key)
        return super().open_summary(key, size)


def test_reconcile_skip_touches_no_summary(tmp_db, tmp_path):
    """R4 + lever B: an unchanged file is skipped from the listing fingerprint alone,
    so a warm re-scan opens NO summary (over S3 that is zero network)."""
    conn, caches = tmp_db
    root = str(tmp_path / "watch")
    _hive(root, filename="a.mcap")
    src = _ReadCountingSource(root)

    full_reconcile(conn, caches, src, workers=1)
    assert len(src.reads) == 1  # cold pass reads the one file's summary

    src.reads.clear()
    tally = full_reconcile(conn, caches, src, workers=4)
    assert tally["skipped"] == 1 and tally["cataloged"] == 0
    assert src.reads == []  # warm pass: summary never opened for the skipped file


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
