"""CLI entry point: ``python3 -m mcap_catalog_builder <watch_root> [options]``.

Architecture: the producers (the watchdog observer + debounce Timers for local,
or the SQS event drainer for S3, plus the periodic rescan thread) only enqueue
WatchEvents. ``worker_loop`` (run on the main thread) is the single CONSUMER and
the only DB writer. It is driven by a storage ``Source`` and is identical for
both backends.
"""

import argparse
import logging
import os
import queue
import signal
import sqlite3
import threading

from .db import Caches, load_caches, open_db
from .builder import catalog_object, delete_by_key
from .reconcile import full_reconcile
from .storage import LocalSource
from .watcher import McapEventHandler, WatchEvent, start_observer

logger = logging.getLogger(__name__)

DEFAULT_DB = "/tmp/pj-cloud-catalog.db"


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    p = argparse.ArgumentParser(
        prog="mcap_catalog_builder",
        description="Watch a folder (or S3 bucket) of .mcap files and keep the SQLite catalog in sync.",
    )
    p.add_argument("watch_root", nargs="?", default=".",
                   help="folder of .mcap recordings to watch (local source)")
    p.add_argument("--source", choices=["local", "s3"], default="local",
                   help="storage backend (default: local)")
    p.add_argument("--s3-bucket", default=None, help="[s3] bucket name")
    p.add_argument("--s3-prefix", default="", help="[s3] key prefix to scope listing")
    p.add_argument("--sqs-url", default=None, help="[s3] SQS queue URL for S3 event notifications")
    p.add_argument("--db", default=DEFAULT_DB, help=f"catalog DB path (default: {DEFAULT_DB})")
    p.add_argument("--rescan-interval", type=float, default=300.0,
                   help="seconds between safety re-scans (default: 300)")
    p.add_argument("--debounce", type=float, default=2.0,
                   help="[local] seconds to debounce file events (default: 2)")
    p.add_argument("--stability-checks", type=int, default=3,
                   help="[local] size-stability poll count before cataloging (default: 3)")
    p.add_argument("--stability-interval", type=float, default=0.5,
                   help="[local] seconds between size-stability polls (default: 0.5)")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p


def worker_loop(
    conn: sqlite3.Connection,
    caches: Caches,
    source,
    work_q: "queue.Queue[WatchEvent]",
) -> None:
    """Drain the work queue and perform all DB writes (the single writer).

    Backend-agnostic: each event's payload is mapped to a key via the source,
    stability is gated by the source (local polls; S3 is atomic), and every event
    is handled under a try/except so the worker never dies.
    """
    while True:
        ev = work_q.get()
        try:
            if ev.kind == "stop":
                break
            if ev.kind == "catalog":
                if source.wait_for_stable(ev.path):
                    catalog_object(conn, caches, source.event_key(ev.path), source)
                else:
                    logger.warning("file not stable, dropping (retries on rescan): %s", ev.path)
            elif ev.kind == "delete":
                delete_by_key(conn, caches, source.event_key(ev.path))
            elif ev.kind == "rescan":
                full_reconcile(conn, caches, source)
            else:
                logger.warning("unknown event: %r", ev)
        except Exception:  # noqa: BLE001 - the worker must never die
            logger.exception("worker error handling %r", ev)


def main(argv: list[str] | None = None) -> int:
    """Entry point. Returns a process exit code."""
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    work_q: "queue.Queue[WatchEvent]" = queue.Queue()
    stop_event = threading.Event()
    observer = None
    handler = None
    start_producer = None  # deferred until after the startup reconcile

    # --- build + validate the source (producers are started later) -----------
    if args.source == "s3":
        if not args.s3_bucket or not args.sqs_url:
            logger.error("--source s3 requires --s3-bucket and --sqs-url")
            return 2
        import boto3  # imported lazily so local mode has no boto3 dependency
        from .s3_storage import S3Source
        from .s3_producer import s3_event_producer

        source = S3Source(boto3.client("s3"), args.s3_bucket, args.s3_prefix)

        def start_producer() -> None:
            threading.Thread(
                target=s3_event_producer,
                args=(boto3.client("sqs"), args.sqs_url, work_q, stop_event),
                daemon=True,
            ).start()
            logger.info("watching s3://%s/%s via %s", args.s3_bucket, args.s3_prefix, args.sqs_url)
    else:
        if not os.path.isdir(args.watch_root):
            logger.error("watch_root is not a directory: %s", args.watch_root)
            return 2
        source = LocalSource(args.watch_root, args.stability_checks, args.stability_interval)

        def start_producer() -> None:
            nonlocal observer, handler
            handler = McapEventHandler(work_q, args.debounce)
            observer = start_observer(args.watch_root, handler)
            logger.info("watching %s", args.watch_root)

    conn = open_db(args.db)
    caches = load_caches(conn)

    logger.info("startup reconcile (db=%s)", args.db)
    full_reconcile(conn, caches, source)  # synchronous, before watching for events

    start_producer()  # begin enqueuing live events only after the reconcile

    def rescan_loop() -> None:
        while not stop_event.wait(args.rescan_interval):
            work_q.put(WatchEvent("rescan"))

    threading.Thread(target=rescan_loop, daemon=True).start()

    def _on_signal(_signum, _frame) -> None:
        work_q.put(WatchEvent("stop"))

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    try:
        worker_loop(conn, caches, source, work_q)
    finally:
        stop_event.set()
        if handler is not None:
            handler.cancel_timers()
        if observer is not None:
            observer.stop()
            observer.join()
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
