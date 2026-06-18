#!/usr/bin/env python3
"""Read an MCAP recording's summary straight from S3 — without downloading it.

A hands-on demo of the S3 backend (``mcap_catalog_builder.s3_storage``). It HEADs
the object for its fingerprint, then reads only the footer + summary section via
range GETs, and prints how few bytes that took next to the object's full size.

    # one object: print its signals/counts/time span + bytes fetched vs size
    python3 examples/s3_read_summary.py s3://my-bucket/customer=acme/.../x.mcap

    # discovery: list the .mcap objects under a prefix (key + ETag, no body read)
    python3 examples/s3_read_summary.py --list s3://my-bucket/customer=acme/

Requires ``boto3`` and AWS credentials (env vars, ~/.aws, or an instance role)
with ``s3:GetObject`` and — for ``--list`` — ``s3:ListBucket``. boto3 is needed
only to run this script; the library itself never imports it.
"""

import argparse
import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # repo root on path

from mcap_catalog_builder.mcap_summary import summary_from_stream  # noqa: E402
from mcap_catalog_builder.s3_storage import S3Source  # noqa: E402


class _CountingS3:
    """Wraps a boto3 S3 client to tally the bytes actually fetched by GETs."""

    def __init__(self, client) -> None:
        self._c = client
        self.fetched = 0

    def head_object(self, **kw):
        return self._c.head_object(**kw)

    def get_object(self, **kw):
        data = self._c.get_object(**kw)["Body"].read()
        self.fetched += len(data)
        return {"Body": io.BytesIO(data)}

    def get_paginator(self, name):
        return self._c.get_paginator(name)


def _split_s3_url(url: str) -> tuple[str, str]:
    if not url.startswith("s3://"):
        sys.exit("path must look like s3://bucket/key")
    bucket, _, key = url[len("s3://"):].partition("/")
    return bucket, key


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("url", help="s3://bucket/key (or s3://bucket/prefix with --list)")
    p.add_argument("--list", action="store_true", help="list .mcap under the prefix")
    p.add_argument("--region", default=None, help="AWS region (else boto3 default)")
    args = p.parse_args(argv)

    import boto3  # imported lazily so the library has no boto3 dependency

    bucket, key = _split_s3_url(args.url)
    client = boto3.client("s3", region_name=args.region)

    if args.list:
        src = S3Source(client, bucket, prefix=key)
        n = 0
        for listing in src.list_all():
            print(f"{listing.stat.size:>14,d}  {listing.stat.etag:<34}  {listing.key}")
            n += 1
        print(f"\n{n} .mcap object(s) under s3://{bucket}/{key} (listing only, no body read)")
        return 0

    counting = _CountingS3(client)
    src = S3Source(counting, bucket)
    st = src.stat(key)
    if st is None:
        sys.exit(f"no such object: s3://{bucket}/{key}")
    with src.open_summary(key, st.size) as stream:
        summary = summary_from_stream(stream)

    print(f"s3://{bucket}/{key}")
    print(f"  etag        {st.etag}")
    print(f"  time span   {summary.start_time_ns} .. {summary.end_time_ns} ns")
    print(f"  messages    {summary.message_count:,d}")
    print(f"  signals     {len(summary.channels)}")
    for ch in sorted(summary.channels, key=lambda c: c.topic)[:20]:
        print(f"    {ch.message_count:>12,d}  {ch.topic}  [{ch.schema_name}]")
    if len(summary.channels) > 20:
        print(f"    ... and {len(summary.channels) - 20} more")
    pct = 100.0 * counting.fetched / st.size if st.size else 0.0
    print(f"\n  fetched {counting.fetched:,d} of {st.size:,d} bytes "
          f"({pct:.2f}%) — footer + summary only, body skipped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
