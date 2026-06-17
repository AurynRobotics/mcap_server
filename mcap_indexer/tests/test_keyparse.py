"""Tests for Hive object-key parsing / rebuilding / relpath derivation."""

import os

from mcap_indexer.keyparse import parse_hive_key, rebuild_hive_key, relpath_key

VALID = (
    "customer=dexory/customer_site=nashville/robot=arri-182/"
    "source=ros-bags/date=2026-05-19/rosbox_2026-05-19_16-43-46.mcap"
)
DIMS = {
    "customer": "dexory",
    "site": "nashville",
    "robot": "arri-182",
    "source": "ros-bags",
    "date": "2026-05-19",
    "filename": "rosbox_2026-05-19_16-43-46.mcap",
}


def test_parse_valid_key():
    assert parse_hive_key(VALID) == DIMS


def test_parse_leading_slash():
    assert parse_hive_key("/" + VALID) == DIMS


def test_parse_flat_name_is_none():
    assert parse_hive_key("bad.mcap") is None


def test_parse_partial_key_missing_date_is_none():
    partial = (
        "customer=dexory/customer_site=nashville/robot=arri-182/"
        "source=ros-bags/rosbox.mcap"
    )
    assert parse_hive_key(partial) is None


def test_parse_non_mcap_is_none():
    assert parse_hive_key(VALID.replace(".mcap", ".txt")) is None


def test_rebuild_is_exact_inverse():
    assert rebuild_hive_key(parse_hive_key(VALID)) == VALID


def test_rebuild_roundtrip_strips_leading_slash():
    k = "/" + VALID
    assert rebuild_hive_key(parse_hive_key(k)) == k.lstrip("/")


def test_relpath_key_posix():
    root = "/data/watch"
    p = os.path.join(root, "customer=dexory", "customer_site=nashville", "x.mcap")
    assert relpath_key(p, root) == "customer=dexory/customer_site=nashville/x.mcap"
