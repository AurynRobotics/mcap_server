"""Tests for the catalog builder core: the §8 transaction, skip, failure, delete."""

import os

import pytest

from mcap_catalog_builder import builder, mcap_summary
from mcap_catalog_builder.builder import (
    compute_set_fingerprint,
    delete_by_path,
    catalog_file,
    resolve_dimensions,
    synth_etag,
)
from mcap_catalog_builder.mcap_summary import ChannelInfo, FileSummary
from mcap_catalog_builder.tests.fixtures import write_minimal_mcap
from mcap_catalog_builder.varint import decode_counts_blob

DIMS = {
    "customer": "dexory",
    "site": "london",
    "robot": "rob01",
    "source": "ros-bags",
    "date": "2026-06-01",
    "filename": "x.mcap",
}


def _hive_path(root: str, dims: dict[str, str]) -> str:
    return os.path.join(
        root,
        f"customer={dims['customer']}",
        f"customer_site={dims['site']}",
        f"robot={dims['robot']}",
        f"source={dims['source']}",
        f"date={dims['date']}",
        dims["filename"],
    )


def _write_hive(root, dims=DIMS, channels=None, s3_key=None):
    dest = _hive_path(root, dims)
    write_minimal_mcap(
        dest,
        s3_key=s3_key,
        channels=channels
        or [("/a", "S", "ros2msg", 3), ("/b", "S", "ros2msg", 2), ("/zero", "S", "ros2msg", 0)],
    )
    return dest


def test_fingerprint_is_stable_and_order_independent():
    assert compute_set_fingerprint([(1, 2), (3, 4)]) == compute_set_fingerprint([(3, 4), (1, 2)])


def test_catalog_happy_path(tmp_db, tmp_path):
    conn, caches = tmp_db
    root = str(tmp_path / "watch")
    dest = _write_hive(root)
    assert catalog_file(conn, caches, dest, root).status == "cataloged"

    row = conn.execute("SELECT * FROM files").fetchone()
    assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 1
    n_members = conn.execute(
        "SELECT COUNT(*) FROM topic_set_members WHERE set_id=?", (row["topic_set_id"],)
    ).fetchone()[0]
    assert n_members == 3
    counts = decode_counts_blob(row["topic_counts"])
    assert len(counts) == 3
    assert sum(counts) == 5  # 3 + 2 + 0; the zero-count channel is preserved
    assert row["has_error"] == 0
    assert row["etag"] == synth_etag(row["size_bytes"], row["last_modified_ns"])
    assert row["cataloged_at_ns"] > row["last_modified_ns"]


def test_recatalog_unchanged_is_skipped(tmp_db, tmp_path):
    conn, caches = tmp_db
    root = str(tmp_path / "watch")
    dest = _write_hive(root)
    assert catalog_file(conn, caches, dest, root).status == "cataloged"
    assert catalog_file(conn, caches, dest, root).status == "skipped"
    assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM topic_set_members").fetchone()[0] == 3


def test_mtime_change_recatalogs(tmp_db, tmp_path):
    conn, caches = tmp_db
    root = str(tmp_path / "watch")
    dest = _write_hive(root)
    catalog_file(conn, caches, dest, root)
    st = os.stat(dest)
    os.utime(dest, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000_000))
    assert catalog_file(conn, caches, dest, root).status == "cataloged"
    assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 1


def test_unparseable_key_records_failure(tmp_db, tmp_path):
    conn, caches = tmp_db
    root = str(tmp_path / "watch")
    os.makedirs(root, exist_ok=True)
    dest = os.path.join(root, "flat.mcap")  # not Hive-structured
    write_minimal_mcap(dest, channels=[("/a", "S", "ros2msg", 1)])
    assert catalog_file(conn, caches, dest, root).status == "failed"
    assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM catalog_failures").fetchone()[0] == 1


def test_s3_key_metadata_overrides_path(tmp_db, tmp_path):
    conn, caches = tmp_db
    root = str(tmp_path / "watch")
    os.makedirs(root, exist_ok=True)
    dest = os.path.join(root, "flat_name.mcap")  # flat path…
    key = (
        "customer=acme/customer_site=hq/robot=r1/source=ros-bags/"
        "date=2026-06-02/real.mcap"
    )
    write_minimal_mcap(dest, s3_key=key, channels=[("/a", "S", "ros2msg", 2), ("/z", "S", "ros2msg", 0)])
    assert catalog_file(conn, caches, dest, root).status == "cataloged"
    row = conn.execute(
        "SELECT c.name AS customer, f.date, f.filename FROM files f "
        "JOIN customers c ON c.id = f.customer_id"
    ).fetchone()
    assert row["customer"] == "acme"  # from metadata, not the flat on-disk name
    assert row["date"] == "2026-06-02"
    assert row["filename"] == "real.mcap"


def test_count_mismatch_guard_rolls_back(tmp_db, tmp_path, monkeypatch):
    conn, caches = tmp_db
    root = str(tmp_path / "watch")
    dest = _write_hive(root)

    bad = FileSummary(
        start_time_ns=1,
        end_time_ns=2,
        message_count=999,  # does not match the channel counts below
        chunk_count=1,
        channels=[ChannelInfo(1, "/a", "S", "ros2msg", 1)],
    )
    monkeypatch.setattr(builder, "summary_from_stream", lambda _stream: bad)
    assert catalog_file(conn, caches, dest, root).status == "failed"
    assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM catalog_failures").fetchone()[0] == 1


def test_delete_by_path(tmp_db, tmp_path):
    conn, caches = tmp_db
    root = str(tmp_path / "watch")
    dest = _write_hive(root)
    catalog_file(conn, caches, dest, root)
    assert delete_by_path(conn, caches, dest, root) is True
    assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 0
    assert delete_by_path(conn, caches, dest, root) is False  # already gone


def test_delete_on_success_clears_prior_failure(tmp_db, tmp_path):
    conn, caches = tmp_db
    from mcap_catalog_builder.db import record_failure
    from mcap_catalog_builder.keyparse import relpath_key

    root = str(tmp_path / "watch")
    dest = _write_hive(root)
    key = relpath_key(dest, root)
    record_failure(conn, key, "earlier transient error")
    conn.commit()
    assert catalog_file(conn, caches, dest, root).status == "cataloged"
    assert conn.execute(
        "SELECT COUNT(*) FROM catalog_failures WHERE s3_key=?", (key,)
    ).fetchone()[0] == 0


def test_catalog_vanished_file_does_not_crash(tmp_db, tmp_path):
    # A file that disappears mid-reconcile (TOCTOU) must not crash catalog_file
    # nor write a spurious failure row — the deletion sweep cleans up its row.
    conn, caches = tmp_db
    root = str(tmp_path / "watch")
    missing = os.path.join(
        root, "customer=dexory", "customer_site=london", "robot=rob01",
        "source=ros-bags", "date=2026-06-01", "ghost.mcap",
    )
    assert catalog_file(conn, caches, missing, root).status == "failed"
    assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM catalog_failures").fetchone()[0] == 0


def test_chunk_count_populated(tmp_db, tmp_path):
    # files.chunk_count is written from MCAP Statistics.chunk_count (M2 / the Go
    # reader's flat 'chunk_count'); it matches what the summary parser reports.
    conn, caches = tmp_db
    root = str(tmp_path / "watch")
    dest = _write_hive(root)
    catalog_file(conn, caches, dest, root)
    row = conn.execute("SELECT chunk_count FROM files").fetchone()
    assert row["chunk_count"] == mcap_summary.read_file_summary(dest).chunk_count


def test_override_survives_recatalog(tmp_db, tmp_path):
    # The override-survives-reindex invariant (Go's smoke step h): a forced
    # re-catalog rewrites tags_embedded but NEVER touches a user override.
    from mcap_catalog_builder.db import update_tags

    conn, caches = tmp_db
    root = str(tmp_path / "watch")
    dest = _write_hive(root)
    catalog_file(conn, caches, dest, root)
    fid = conn.execute("SELECT id FROM files").fetchone()["id"]

    update_tags(conn, fid, set_kv={"quality": "good"})  # user edit
    st = os.stat(dest)  # force a re-catalog via mtime bump
    os.utime(dest, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000_000))
    assert catalog_file(conn, caches, dest, root).status == "cataloged"

    row = conn.execute(
        "SELECT value, is_override FROM tags_effective WHERE file_id=? AND key='quality'",
        (fid,),
    ).fetchone()
    assert row is not None and row["value"] == "good" and row["is_override"] == 1


def test_update_tags_set_unset_mask(tmp_db, tmp_path):
    # set => override wins over embedded; unset of an embedded key => NULL mask
    # (tag disappears); unset of a pure override => plain delete.
    from mcap_catalog_builder.db import update_tags

    conn, caches = tmp_db
    root = str(tmp_path / "watch")
    dest = _write_hive(root)
    catalog_file(conn, caches, dest, root)
    fid = conn.execute("SELECT id FROM files").fetchone()["id"]
    # seed an embedded tag directly (derive_tags is a stub today).
    conn.execute("INSERT INTO tags_embedded(file_id, key, value) VALUES (?, 'site', 'london')", (fid,))
    conn.commit()

    update_tags(conn, fid, set_kv={"site": "paris", "mission": "inventory"})
    eff = {
        r["key"]: (r["value"], r["is_override"])
        for r in conn.execute(
            "SELECT key, value, is_override FROM tags_effective WHERE file_id=?", (fid,)
        )
    }
    assert eff["site"] == ("paris", 1)         # override wins over embedded 'london'
    assert eff["mission"] == ("inventory", 1)

    update_tags(conn, fid, unset_keys=["site"])  # masks the embedded tag
    keys = {r["key"] for r in conn.execute(
        "SELECT key FROM tags_effective WHERE file_id=?", (fid,))}
    assert "site" not in keys                  # NULL override hides the embedded tag
    assert "mission" in keys

    update_tags(conn, fid, unset_keys=["mission"])  # pure override => deleted
    keys = {r["key"] for r in conn.execute(
        "SELECT key FROM tags_effective WHERE file_id=?", (fid,))}
    assert "mission" not in keys
