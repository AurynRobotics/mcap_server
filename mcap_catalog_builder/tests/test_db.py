"""Tests for the DB connection, schema init, caches, and id resolvers."""

import sqlite3

import pytest

from mcap_catalog_builder.db import (
    SCHEMA_VERSION,
    SchemaVersionError,
    load_caches,
    open_db,
    record_failure,
    resolve_customer,
    resolve_robot,
    resolve_schema,
    resolve_site,
    resolve_source,
    resolve_topic,
    resolve_topic_set,
)


@pytest.fixture
def conn(tmp_path):
    c = open_db(str(tmp_path / "catalog.db"))
    yield c
    c.close()


def test_open_db_creates_all_tables(conn):
    tables = {
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    expected = {
        "files", "customers", "sites", "robots", "sources", "topic_names",
        "schemas", "topic_sets", "topic_set_members", "tags_embedded",
        "tags_override", "catalog_failures", "build_metadata",
    }
    assert expected <= tables


def test_record_build_stamps_and_bumps_build_id(conn):
    from mcap_catalog_builder.db import record_build

    record_build(conn, files_scanned=10, files_failed=0)
    row = conn.execute("SELECT * FROM build_metadata WHERE id=1").fetchone()
    assert row["build_id"] == 1
    assert row["files_scanned"] == 10 and row["files_failed"] == 0
    assert row["build_outcome"] == "ok" and row["last_build_ns"] > 0

    # A second build bumps build_id monotonically (swap-detection token).
    record_build(conn, files_scanned=12, files_failed=2, outcome="partial")
    row = conn.execute("SELECT * FROM build_metadata WHERE id=1").fetchone()
    assert row["build_id"] == 2
    assert row["files_scanned"] == 12 and row["files_failed"] == 2
    assert row["build_outcome"] == "partial"


def test_open_db_idempotent(tmp_path):
    p = str(tmp_path / "c.db")
    open_db(p).close()
    open_db(p).close()  # reopening an existing DB must not error


def test_pragmas_on_file_db(conn):
    assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1


def test_schema_version_stamped_on_fresh_db(conn):
    # A fresh DB is stamped with the current SCHEMA_VERSION, exactly one row, id=1.
    rows = conn.execute("SELECT id, version FROM schema_version").fetchall()
    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert rows[0]["version"] == SCHEMA_VERSION


def test_schema_version_single_row_enforced(conn):
    # CHECK(id=1) makes the table structurally single-row: a second row is rejected.
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO schema_version(id, version) VALUES (2, 99)")


def test_schema_version_idempotent_reopen(tmp_path):
    # Re-opening a DB already at the current version must not error or duplicate.
    p = str(tmp_path / "c.db")
    open_db(p).close()
    c = open_db(p)
    try:
        assert c.execute("SELECT COUNT(*) FROM schema_version").fetchone()[0] == 1
    finally:
        c.close()


def test_open_db_refuses_foreign_db(tmp_path):
    # A DB that already has tables but no schema_version (e.g. a legacy Go-written
    # catalog) must be REFUSED, not silently stamped as auryn v1 — otherwise the
    # cross-language interlock is defeated (the Go reader would accept it).
    p = str(tmp_path / "legacy.db")
    raw = sqlite3.connect(p)
    raw.execute("CREATE TABLE files (id INTEGER PRIMARY KEY, s3_key TEXT)")  # Go-ish shape
    raw.commit()
    raw.close()
    with pytest.raises(SchemaVersionError):
        open_db(p)


def test_stale_version_db_not_mutated(tmp_path):
    # A stale-version auryn DB must be refused BEFORE any DDL — the version gate
    # precedes executescript, so the DB is never polluted with new-version tables
    # while still stamped at the old version (M2 review M-1).
    p = str(tmp_path / "stale.db")
    raw = sqlite3.connect(p)
    raw.execute("CREATE TABLE schema_version (id INTEGER PRIMARY KEY CHECK (id=1), version INTEGER NOT NULL)")
    raw.execute("INSERT INTO schema_version(id, version) VALUES (1, ?)", (SCHEMA_VERSION - 1,))
    raw.execute("CREATE TABLE files (id INTEGER PRIMARY KEY)")  # a pre-existing (old) table
    raw.commit()
    raw.close()

    with pytest.raises(SchemaVersionError):
        open_db(p)

    # No current-version tables may have been created before the gate fired.
    raw = sqlite3.connect(p)
    tables = {r[0] for r in raw.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    raw.close()
    assert "tags_embedded" not in tables
    assert "tags_override" not in tables


def test_schema_version_mismatch_fails_fast(tmp_path):
    # A DB stamped with a DIFFERENT version must fail fast on open (never silently
    # served under an incompatible builder/reader).
    p = str(tmp_path / "c.db")
    open_db(p).close()
    raw = sqlite3.connect(p)
    raw.execute("UPDATE schema_version SET version = ? WHERE id = 1", (SCHEMA_VERSION + 1,))
    raw.commit()
    raw.close()
    with pytest.raises(SchemaVersionError):
        open_db(p)


def test_resolve_customer_idempotent(conn):
    caches = load_caches(conn)
    a = resolve_customer(conn, caches, "dexory")
    b = resolve_customer(conn, caches, "dexory")
    assert a == b
    conn.commit()
    n = conn.execute("SELECT COUNT(*) FROM customers WHERE name='dexory'").fetchone()[0]
    assert n == 1


def test_resolve_hierarchy_scopes_by_parent(conn):
    caches = load_caches(conn)
    cid = resolve_customer(conn, caches, "dexory")
    sid = resolve_site(conn, caches, cid, "nashville")
    rid = resolve_robot(conn, caches, sid, "arri-182")
    conn.commit()
    assert conn.execute("SELECT customer_id FROM sites WHERE id=?", (sid,)).fetchone()[0] == cid
    assert conn.execute("SELECT site_id FROM robots WHERE id=?", (rid,)).fetchone()[0] == sid
    # same site name under a different customer → a distinct row
    cid2 = resolve_customer(conn, caches, "other")
    sid2 = resolve_site(conn, caches, cid2, "nashville")
    assert sid2 != sid


def test_resolve_schema_distinguishes_encoding(conn):
    caches = load_caches(conn)
    assert resolve_schema(conn, caches, "S", "ros2msg") != resolve_schema(
        conn, caches, "S", "protobuf"
    )


def test_resolve_topic_set_members_on_first_insert_only(conn):
    caches = load_caches(conn)
    t1, t2 = resolve_topic(conn, caches, "/a"), resolve_topic(conn, caches, "/b")
    s1 = resolve_schema(conn, caches, "S", "ros2msg")
    members = sorted([(t1, s1), (t2, s1)])
    with conn:
        set_id = resolve_topic_set(conn, caches, "fp-test", members)
    assert conn.execute(
        "SELECT COUNT(*) FROM topic_set_members WHERE set_id=?", (set_id,)
    ).fetchone()[0] == 2
    with conn:  # same fingerprint → reuse id, write zero new members
        assert resolve_topic_set(conn, caches, "fp-test", members) == set_id
    assert conn.execute("SELECT COUNT(*) FROM topic_set_members").fetchone()[0] == 2


def test_resolve_topic_set_rejects_unsorted_members(conn):
    caches = load_caches(conn)
    with pytest.raises(ValueError):
        with conn:
            resolve_topic_set(conn, caches, "fp-bad", [(5, 1), (2, 1)])


def test_caches_reload_from_db(conn):
    caches = load_caches(conn)
    cid = resolve_customer(conn, caches, "dexory")
    conn.commit()
    assert load_caches(conn).customer["dexory"] == cid


def test_record_failure_upserts(conn):
    record_failure(conn, "k1", "boom")
    conn.commit()
    record_failure(conn, "k1", "boom2")
    conn.commit()
    rows = conn.execute(
        "SELECT error_text FROM catalog_failures WHERE s3_key='k1'"
    ).fetchall()
    assert len(rows) == 1 and rows[0][0] == "boom2"
