"""The unified catalog core (catalog_object / delete_by_key) driven by S3.

These exercise the SAME core that the local path uses, but with an S3Source over
an in-memory fake — proving the single-writer transaction, dedup, count check,
and round-trip guard all hold when the bytes come from an object store.
"""

from mcap_catalog_builder.builder import catalog_object, delete_by_key
from mcap_catalog_builder.s3_storage import S3Source
from mcap_catalog_builder.tests.fixtures import InMemoryS3Client, write_minimal_mcap

KEY = "customer=acme/customer_site=hq/robot=r1/source=ros-bags/date=2026-06-02/x.mcap"
CH = [("/a", "S", "ros2msg", 3), ("/b", "S", "ros2msg", 2), ("/zero", "S", "ros2msg", 0)]


def _s3_source(tmp_path, key=KEY, channels=CH):
    local = str(tmp_path / "src.mcap")
    write_minimal_mcap(local, channels=channels)
    with open(local, "rb") as f:
        raw = f.read()
    return S3Source(InMemoryS3Client({key: raw}), "bucket")


def test_catalog_object_writes_row_from_s3(tmp_db, tmp_path):
    conn, caches = tmp_db
    src = _s3_source(tmp_path)
    assert catalog_object(conn, caches, KEY, src).status == "cataloged"

    row = conn.execute(
        "SELECT c.name AS customer, f.date, f.filename, f.etag, f.size_bytes, f.topic_counts "
        "FROM files f JOIN customers c ON c.id = f.customer_id"
    ).fetchone()
    assert (row["customer"], row["date"], row["filename"]) == ("acme", "2026-06-02", "x.mcap")
    assert row["etag"] == f"etag-{KEY}"          # the real S3 ETag is stored, not a synth
    from mcap_catalog_builder.varint import decode_counts_blob
    assert sum(decode_counts_blob(row["topic_counts"])) == 5   # 3 + 2 + 0 preserved


def test_catalog_object_unchanged_etag_skips(tmp_db, tmp_path):
    conn, caches = tmp_db
    src = _s3_source(tmp_path)
    assert catalog_object(conn, caches, KEY, src).status == "cataloged"
    assert catalog_object(conn, caches, KEY, src).status == "skipped"  # same ETag → no re-read
    assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 1


def test_catalog_object_unparseable_key_records_failure(tmp_db, tmp_path):
    conn, caches = tmp_db
    bad = "not/a/hive/key.mcap"
    src = _s3_source(tmp_path, key=bad)
    assert catalog_object(conn, caches, bad, src).status == "failed"
    assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 0
    assert conn.execute(
        "SELECT COUNT(*) FROM catalog_failures WHERE s3_key=?", (bad,)
    ).fetchone()[0] == 1


def test_delete_by_key_removes_row(tmp_db, tmp_path):
    conn, caches = tmp_db
    src = _s3_source(tmp_path)
    catalog_object(conn, caches, KEY, src)
    assert delete_by_key(conn, caches, KEY) is True
    assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 0
    assert delete_by_key(conn, caches, KEY) is False  # already gone
