"""Tests for MCAP summary extraction + s3_key metadata reading."""

from mcap_catalog_builder.mcap_summary import (
    derive_tags,
    extract_s3_key,
    read_file_summary,
    summary_from_stream,
)
from mcap_catalog_builder.tests.fixtures import dexory_file, write_minimal_mcap


def test_summary_from_stream_matches_path_read(tmp_path):
    """summary_from_stream(open_file) equals read_file_summary(path) — the split
    that lets the S3 backend reuse the parser over a range-backed stream."""
    dest = str(tmp_path / "x.mcap")
    write_minimal_mcap(
        dest, channels=[("/a", "S", "ros2msg", 3), ("/zero", "S", "ros2msg", 0)]
    )
    from_path = read_file_summary(dest)
    with open(dest, "rb") as f:
        from_stream = summary_from_stream(f)
    assert from_stream == from_path


def test_zero_count_channel_present_with_zero(tmp_path):
    dest = str(tmp_path / "x.mcap")
    write_minimal_mcap(
        dest, channels=[("/a", "S", "ros2msg", 3), ("/zero", "S", "ros2msg", 0)]
    )
    summary = read_file_summary(dest)
    by_topic = {c.topic: c for c in summary.channels}
    assert set(by_topic) == {"/a", "/zero"}
    assert by_topic["/a"].message_count == 3
    assert by_topic["/zero"].message_count == 0  # absent from counts dict → 0
    assert summary.message_count == 3
    assert sum(c.message_count for c in summary.channels) == summary.message_count


def test_extract_s3_key_present(tmp_path):
    dest = str(tmp_path / "x.mcap")
    key = (
        "customer=acme/customer_site=hq/robot=r1/source=ros-bags/"
        "date=2026-06-02/x.mcap"
    )
    write_minimal_mcap(dest, s3_key=key, channels=[("/a", "S", "ros2msg", 1)])
    assert extract_s3_key(dest) == key


def test_extract_s3_key_absent_returns_none(tmp_path):
    dest = str(tmp_path / "x.mcap")
    write_minimal_mcap(dest, s3_key=None, channels=[("/a", "S", "ros2msg", 1)])
    assert extract_s3_key(dest) is None


def test_derive_tags_empty(tmp_path):
    dest = str(tmp_path / "x.mcap")
    write_minimal_mcap(dest, channels=[("/a", "S", "ros2msg", 1)])
    assert derive_tags(read_file_summary(dest)) == []


def test_real_dexory_summary():
    path = dexory_file("197_continuous_2026_06_01-04_43_33.mcap")
    summary = read_file_summary(path)
    assert len(summary.channels) == 162
    assert sum(c.message_count for c in summary.channels) == 1283397
    assert summary.message_count == 1283397
    assert summary.start_time_ns == 1780289013410214795
    assert summary.end_time_ns == 1780290213410240515
