"""Watchdog inotify handler (enqueue-only) + file-stability poll.

The handler NEVER touches the database — it only ``queue.put(...)`` WatchEvents.
The single worker thread drains the queue and does all DB writes. Per-path
debounce Timers (daemon threads) collapse a burst of modify events into one
index, and ``wait_for_stable`` (run by the worker, not the handler) guards
against indexing a file that is still being copied.
"""

import logging
import os
import queue
import threading
import time
from dataclasses import dataclass

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WatchEvent:
    kind: str  # "index" | "delete" | "rescan" | "stop"
    path: str | None = None


def _is_indexable(path: str) -> bool:
    name = os.path.basename(path)
    return (
        name.endswith(".mcap")
        and not name.startswith(".")
        and not name.endswith(".mcap.tmp")
        and not name.endswith(".part")
    )


class McapEventHandler(FileSystemEventHandler):
    """Translates filesystem events into queued WatchEvents (no DB access)."""

    def __init__(self, work_q: "queue.Queue[WatchEvent]", debounce_secs: float) -> None:
        self._q = work_q
        self._debounce = debounce_secs
        self._timers: dict[str, threading.Timer] = {}
        self._lock = threading.Lock()

    def _schedule_index(self, path: str) -> None:
        with self._lock:
            existing = self._timers.pop(path, None)
            if existing is not None:
                existing.cancel()
            timer = threading.Timer(self._debounce, self._fire_index, args=(path,))
            timer.daemon = True
            self._timers[path] = timer
            timer.start()

    def _fire_index(self, path: str) -> None:
        with self._lock:
            self._timers.pop(path, None)
        self._q.put(WatchEvent("index", path))

    def on_created(self, event) -> None:
        if not event.is_directory and _is_indexable(event.src_path):
            self._schedule_index(event.src_path)

    def on_modified(self, event) -> None:
        if not event.is_directory and _is_indexable(event.src_path):
            self._schedule_index(event.src_path)

    def on_deleted(self, event) -> None:
        if not event.is_directory and _is_indexable(event.src_path):
            with self._lock:  # cancel a pending index timer for this now-gone path
                timer = self._timers.pop(event.src_path, None)
                if timer is not None:
                    timer.cancel()
            self._q.put(WatchEvent("delete", event.src_path))

    def on_moved(self, event) -> None:
        if event.is_directory:
            return
        if _is_indexable(event.src_path):
            self._q.put(WatchEvent("delete", event.src_path))
        if _is_indexable(event.dest_path):
            self._schedule_index(event.dest_path)

    def cancel_timers(self) -> None:
        with self._lock:
            for t in self._timers.values():
                t.cancel()
            self._timers.clear()


def wait_for_stable(path: str, interval: float = 0.5, checks: int = 3) -> bool:
    """Poll ``os.path.getsize`` ``checks`` times ``interval`` apart.

    Returns ``True`` only if every reading is equal and the file still exists.
    Returns ``False`` on any ``OSError`` (e.g. the file vanished mid-copy).
    Runs on the worker thread only (it blocks ~``checks * interval`` seconds).
    """
    sizes: list[int] = []
    for i in range(checks):
        try:
            sizes.append(os.path.getsize(path))
        except OSError:
            return False
        if i < checks - 1:
            time.sleep(interval)
    return os.path.exists(path) and len(set(sizes)) == 1


def start_observer(watched_root: str, handler: McapEventHandler) -> Observer:
    """Schedule ``handler`` on ``watched_root`` (recursive) and start the observer."""
    observer = Observer()
    observer.schedule(handler, watched_root, recursive=True)
    observer.start()
    return observer
