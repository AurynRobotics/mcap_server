"""Tests for the storage Source seam and its local-filesystem implementation."""

import os

from mcap_catalog_builder.mcap_summary import read_file_summary, summary_from_stream
from mcap_catalog_builder.storage import LocalSource, Listing, Stat
from mcap_catalog_builder.tests.fixtures import write_minimal_mcap


def test_local_stat_returns_size_and_synthetic_etag(tmp_path):
    write_minimal_mcap(str(tmp_path / "sub" / "x.mcap"))
    src = LocalSource(str(tmp_path))
    st = src.stat("sub/x.mcap")
    assert isinstance(st, Stat)
    assert st.size == os.path.getsize(str(tmp_path / "sub" / "x.mcap"))
    mtime_ns = os.stat(str(tmp_path / "sub" / "x.mcap")).st_mtime_ns
    assert st.etag == f"local:{st.size}:{mtime_ns}"


def test_local_stat_missing_returns_none(tmp_path):
    assert LocalSource(str(tmp_path)).stat("nope.mcap") is None


def test_local_open_summary_reuses_parser(tmp_path):
    dest = str(tmp_path / "x.mcap")
    write_minimal_mcap(dest, channels=[("/a", "S", "ros2msg", 2)])
    src = LocalSource(str(tmp_path))
    with src.open_summary("x.mcap", src.stat("x.mcap").size) as f:
        assert summary_from_stream(f) == read_file_summary(dest)


def test_local_list_all_yields_relative_keys(tmp_path):
    write_minimal_mcap(str(tmp_path / "a" / "one.mcap"))
    write_minimal_mcap(str(tmp_path / "b" / "two.mcap"))
    (tmp_path / "note.txt").write_text("not mcap")
    src = LocalSource(str(tmp_path))
    listings = sorted(src.list_all(), key=lambda x: x.key)
    assert [x.key for x in listings] == ["a/one.mcap", "b/two.mcap"]
    assert all(isinstance(x, Listing) and x.stat.size > 0 for x in listings)


def test_local_stat_carries_mtime_ns(tmp_path):
    write_minimal_mcap(str(tmp_path / "x.mcap"))
    st = LocalSource(str(tmp_path)).stat("x.mcap")
    assert st.mtime_ns == os.stat(str(tmp_path / "x.mcap")).st_mtime_ns


def test_local_event_key_is_path_relative_to_root(tmp_path):
    src = LocalSource(str(tmp_path))
    abs_path = str(tmp_path / "sub" / "x.mcap")
    assert src.event_key(abs_path) == "sub/x.mcap"


def test_local_intended_key_reads_s3_key_metadata(tmp_path):
    hive = "customer=acme/customer_site=hq/robot=r1/source=ros/date=2026-06-02/real.mcap"
    write_minimal_mcap(str(tmp_path / "flat.mcap"), s3_key=hive)
    write_minimal_mcap(str(tmp_path / "plain.mcap"))  # no s3_key record
    src = LocalSource(str(tmp_path))
    assert src.intended_key("flat.mcap") == hive
    assert src.intended_key("plain.mcap") is None


def test_local_wait_for_stable_true_for_static_file(tmp_path):
    write_minimal_mcap(str(tmp_path / "x.mcap"))
    src = LocalSource(str(tmp_path), stability_checks=2, stability_interval=0.0)
    assert src.wait_for_stable(str(tmp_path / "x.mcap")) is True
