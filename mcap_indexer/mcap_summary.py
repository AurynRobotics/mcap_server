"""Read per-file facts from the MCAP summary, plus the s3_key metadata record.

Per-file stats come ONLY from the summary section (Statistics + channels +
schemas) — never from the embedded ``rosbag2`` metadata, which describes the
whole multi-day bag, not the individual split file.

The critical detail: a channel with zero messages is present in
``summary.channels`` but ABSENT from ``statistics.channel_message_counts``, so
the count must be read with ``.get(ch.id, 0)``.
"""

import logging
from dataclasses import dataclass

from mcap.reader import make_reader

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ChannelInfo:
    channel_id: int
    topic: str
    schema_name: str
    schema_encoding: str
    message_count: int  # 0 if channel_id absent from channel_message_counts


@dataclass(frozen=True)
class FileSummary:
    start_time_ns: int
    end_time_ns: int
    message_count: int  # statistics.message_count — the authoritative total
    channels: list[ChannelInfo]


def read_file_summary(path: str) -> FileSummary:
    """Read the MCAP summary. Raises ``ValueError`` if no summary/statistics."""
    with open(path, "rb") as f:
        summary = make_reader(f).get_summary()
        if summary is None or summary.statistics is None:
            raise ValueError("no summary/statistics in MCAP")
        counts = summary.statistics.channel_message_counts
        channels: list[ChannelInfo] = []
        for ch in summary.channels.values():
            schema = summary.schemas.get(ch.schema_id)
            channels.append(
                ChannelInfo(
                    channel_id=ch.id,
                    topic=ch.topic,
                    schema_name=schema.name if schema is not None else "",
                    schema_encoding=schema.encoding if schema is not None else "",
                    message_count=counts.get(ch.id, 0),  # .get default is MANDATORY
                )
            )
        return FileSummary(
            start_time_ns=summary.statistics.message_start_time,
            end_time_ns=summary.statistics.message_end_time,
            message_count=summary.statistics.message_count,
            channels=channels,
        )


def extract_s3_key(path: str) -> str | None:
    """Return the object key from the first ``s3_key`` metadata record, or ``None``.

    Absence of an ``s3_key`` record is a valid state (the daemon then falls back
    to the relative path), so read errors are logged at DEBUG and swallowed.
    """
    try:
        with open(path, "rb") as f:
            for meta in make_reader(f).iter_metadata():
                if meta.name == "s3_key":
                    return meta.metadata.get("key")
    except Exception as e:  # pragma: no cover - defensive
        logger.debug("extract_s3_key failed for %s: %s", path, e)
    return None


def derive_tags(summary: FileSummary) -> list[tuple[str, str]]:
    """Derive ``(key, value)`` tags from a file. Empty for Dexory (no embedded tags)."""
    return []
