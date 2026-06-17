"""End-to-end tests: full reconcile lifecycle against real + synthetic MCAPs.

``test_e2e_reconcile`` needs the real Dexory data and auto-skips if absent; the
other two are synthetic and always run.
"""

import os

from mcap_catalog_builder.db import load_caches, open_db
from mcap_catalog_builder.reconcile import full_reconcile
from mcap_catalog_builder.tests.fixtures import (
    dexory_file,
    make_hive_fixture,
    write_flat_no_metadata,
    write_minimal_mcap,
)
from mcap_catalog_builder.varint import decode_counts_blob

F197 = "197_continuous_2026_06_01-04_43_33.mcap"
F198 = "198_continuous_2026_06_01-05_03_33.mcap"
DIMS = {
    "customer": "dexory",
    "site": "london",
    "robot": "rob01",
    "source": "ros-bags",
    "date": "2026-06-01",
    "filename": F197,
}


def test_e2e_reconcile(tmp_path):
    src = dexory_file(F197)  # skips the test if the Dexory dir is absent
    watch = str(tmp_path / "watch")
    dest197 = make_hive_fixture(src, watch, DIMS)

    conn = open_db(str(tmp_path / "catalog.db"))
    caches = load_caches(conn)
    try:
        full_reconcile(conn, caches, watch)
        row = conn.execute("SELECT * FROM files").fetchone()
        assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 1
        assert row["size_bytes"] == 87636213
        assert row["start_time_ns"] == 1780289013410214795
        assert row["end_time_ns"] == 1780290213410240515
        assert row["has_error"] == 0
        assert row["etag"] == f"local:{row['size_bytes']}:{row['last_modified_ns']}"
        assert row["cataloged_at_ns"] > row["last_modified_ns"]

        n_members = conn.execute(
            "SELECT COUNT(*) FROM topic_set_members WHERE set_id=?", (row["topic_set_id"],)
        ).fetchone()[0]
        assert n_members == 162
        counts = decode_counts_blob(row["topic_counts"])
        assert len(counts) == 162
        assert sum(counts) == 1283397

        # warm no-op
        tally = full_reconcile(conn, caches, watch)
        assert tally["cataloged"] == 0 and tally["skipped"] == 1
        assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 1

        # add 198 → 2 files, 1 topic set (identical layout → deduped)
        dest198 = make_hive_fixture(dexory_file(F198), watch, {**DIMS, "filename": F198})
        full_reconcile(conn, caches, watch)
        assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM topic_sets").fetchone()[0] == 1

        # modify (touch mtime) → recatalog, still 2 rows
        st = os.stat(dest198)
        os.utime(dest198, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000_000))
        tally = full_reconcile(conn, caches, watch)
        assert tally["cataloged"] >= 1
        assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 2

        # remove 197 → hard-delete, topic set survives
        os.remove(dest197)
        tally = full_reconcile(conn, caches, watch)
        assert tally["deleted"] == 1
        assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM topic_sets").fetchone()[0] == 1
    finally:
        conn.close()


def test_e2e_failure_path(tmp_path):
    watch = str(tmp_path / "watch")
    os.makedirs(watch)
    write_flat_no_metadata(os.path.join(watch, "bad.mcap"))  # flat, no s3_key

    conn = open_db(str(tmp_path / "catalog.db"))
    caches = load_caches(conn)
    try:
        full_reconcile(conn, caches, watch)
        assert conn.execute("SELECT COUNT(*) FROM catalog_failures").fetchone()[0] >= 1
        assert conn.execute(
            "SELECT COUNT(*) FROM files WHERE filename='bad.mcap'"
        ).fetchone()[0] == 0
    finally:
        conn.close()


def test_e2e_s3key_metadata(tmp_path):
    watch = str(tmp_path / "watch")
    os.makedirs(watch)
    key = (
        "customer=acme/customer_site=hq/robot=r1/source=ros-bags/"
        "date=2026-06-02/x.mcap"
    )
    # Flat on-disk name, but the s3_key metadata carries the real dimensions.
    write_minimal_mcap(
        os.path.join(watch, "flat_name.mcap"),
        s3_key=key,
        channels=[("/a", "S", "ros2msg", 2), ("/zero", "S", "ros2msg", 0)],
    )
    conn = open_db(str(tmp_path / "catalog.db"))
    caches = load_caches(conn)
    try:
        full_reconcile(conn, caches, watch)
        row = conn.execute(
            "SELECT c.name AS customer, f.date, f.filename, f.topic_counts "
            "FROM files f JOIN customers c ON c.id = f.customer_id"
        ).fetchone()
        assert row["customer"] == "acme"  # from metadata, not the flat on-disk name
        assert row["date"] == "2026-06-02"
        assert row["filename"] == "x.mcap"
        counts = decode_counts_blob(row["topic_counts"])
        assert len(counts) == 2 and sum(counts) == 2  # zero-count member preserved
    finally:
        conn.close()
