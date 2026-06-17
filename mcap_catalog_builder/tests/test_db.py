"""Tests for the DB connection, schema init, caches, and id resolvers."""

import pytest

from mcap_catalog_builder.db import (
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
        "schemas", "topic_sets", "topic_set_members", "tags", "catalog_failures",
    }
    assert expected <= tables


def test_open_db_idempotent(tmp_path):
    p = str(tmp_path / "c.db")
    open_db(p).close()
    open_db(p).close()  # reopening an existing DB must not error


def test_pragmas_on_file_db(conn):
    assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1


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
