"""CLI entry point: ``python3 -m mcap_catalog_builder <watch_root> [options]``.

Architecture: the watchdog observer, the debounce Timers, and the periodic
rescan thread are PRODUCERS — they only enqueue WatchEvents. ``worker_loop`` (run
on the main thread) is the single CONSUMER and the only DB writer.
"""

import argparse
import logging
import os
import queue
import signal
import sqlite3
import threading

from .db import Caches, load_caches, open_db
from .builder import delete_by_path, catalog_file
from .reconcile import full_reconcile
from .watcher import McapEventHandler, WatchEvent, start_observer, wait_for_stable

logger = logging.getLogger(__name__)

DEFAULT_DB = "/tmp/pj-cloud-catalog.db"


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    p = argparse.ArgumentParser(
        prog="mcap_catalog_builder",
        description="Watch a folder of .mcap files and keep the SQLite catalog in sync.",
    )
    p.add_argument("watch_root", help="folder of .mcap recordings to watch")
    p.add_argument("--db", default=DEFAULT_DB, help=f"catalog DB path (default: {DEFAULT_DB})")
    p.add_argument("--rescan-interval", type=float, default=300.0,
                   help="seconds between safety re-scans (default: 300)")
    p.add_argument("--debounce", type=float, default=2.0,
                   help="seconds to debounce file events (default: 2)")
    p.add_argument("--stability-checks", type=int, default=3,
                   help="size-stability poll count before cataloging (default: 3)")
    p.add_argument("--stability-interval", type=float, default=0.5,
                   help="seconds between size-stability polls (default: 0.5)")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p


def worker_loop(
    conn: sqlite3.Connection,
    caches: Caches,
    watched_root: str,
    work_q: "queue.Queue[WatchEvent]",
    stability_checks: int,
    stability_interval: float,
) -> None:
    """Drain the work queue and perform all DB writes (the single writer).

    Each event is handled under a try/except so the worker never dies.
    """
    while True:
        ev = work_q.get()
        try:
            if ev.kind == "stop":
                break
            if ev.kind == "catalog":
                if wait_for_stable(ev.path, stability_interval, stability_checks):
                    catalog_file(conn, caches, ev.path, watched_root)
                else:
                    logger.warning("file not stable, dropping (retries on rescan): %s", ev.path)
            elif ev.kind == "delete":
                delete_by_path(conn, caches, ev.path, watched_root)
            elif ev.kind == "rescan":
                full_reconcile(conn, caches, watched_root)
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
    if not os.path.isdir(args.watch_root):
        logger.error("watch_root is not a directory: %s", args.watch_root)
        return 2

    conn = open_db(args.db)
    caches = load_caches(conn)
    work_q: "queue.Queue[WatchEvent]" = queue.Queue()

    logger.info("startup reconcile of %s", args.watch_root)
    full_reconcile(conn, caches, args.watch_root)  # synchronous, before the observer

    handler = McapEventHandler(work_q, args.debounce)
    observer = start_observer(args.watch_root, handler)

    stop_event = threading.Event()

    def rescan_loop() -> None:
        while not stop_event.wait(args.rescan_interval):
            work_q.put(WatchEvent("rescan"))

    rescan_thread = threading.Thread(target=rescan_loop, daemon=True)
    rescan_thread.start()

    def _on_signal(_signum, _frame) -> None:
        work_q.put(WatchEvent("stop"))

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    logger.info("watching %s (db=%s)", args.watch_root, args.db)
    try:
        worker_loop(
            conn, caches, args.watch_root, work_q,
            args.stability_checks, args.stability_interval,
        )
    finally:
        stop_event.set()
        handler.cancel_timers()
        observer.stop()
        observer.join()
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
