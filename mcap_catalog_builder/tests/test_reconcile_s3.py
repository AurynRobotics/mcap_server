"""full_reconcile driven by an S3 Source: catalog, dedup, warm no-op, deletion."""

from mcap_catalog_builder.reconcile import full_reconcile
from mcap_catalog_builder.s3_storage import S3Source
from mcap_catalog_builder.tests.fixtures import InMemoryS3Client, write_minimal_mcap

CH = [("/a", "S", "ros2msg", 2), ("/b", "S", "ros2msg", 1), ("/zero", "S", "ros2msg", 0)]
KA = "customer=acme/customer_site=hq/robot=r1/source=ros-bags/date=2026-06-02/a.mcap"
KB = "customer=acme/customer_site=hq/robot=r1/source=ros-bags/date=2026-06-02/b.mcap"


def _raw(tmp_path):
    local = str(tmp_path / "src.mcap")
    write_minimal_mcap(local, channels=CH)
    with open(local, "rb") as f:
        return f.read()


def test_reconcile_over_s3_catalogs_and_dedups(tmp_db, tmp_path):
    conn, caches = tmp_db
    raw = _raw(tmp_path)
    src = S3Source(InMemoryS3Client({KA: raw, KB: raw}), "bucket")
    tally = full_reconcile(conn, caches, src)
    assert tally["cataloged"] == 2
    assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM topic_sets").fetchone()[0] == 1  # same layout


def test_reconcile_over_s3_warm_noop(tmp_db, tmp_path):
    conn, caches = tmp_db
    raw = _raw(tmp_path)
    src = S3Source(InMemoryS3Client({KA: raw}), "bucket")
    full_reconcile(conn, caches, src)
    tally = full_reconcile(conn, caches, src)  # same ETags → all skipped
    assert tally["cataloged"] == 0 and tally["skipped"] == 1


def test_reconcile_over_s3_deletes_vanished(tmp_db, tmp_path):
    conn, caches = tmp_db
    raw = _raw(tmp_path)
    full_reconcile(conn, caches, S3Source(InMemoryS3Client({KA: raw, KB: raw}), "bucket"))
    # B is gone from the bucket on the next sweep:
    tally = full_reconcile(conn, caches, S3Source(InMemoryS3Client({KA: raw}), "bucket"))
    assert tally["deleted"] == 1
    assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 1
