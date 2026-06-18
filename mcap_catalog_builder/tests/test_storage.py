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
