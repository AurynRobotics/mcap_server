"""Tests for the watchdog event handler (enqueue-only) and wait_for_stable."""

import os
import queue
import time

from watchdog.events import (
    DirModifiedEvent,
    FileCreatedEvent,
    FileDeletedEvent,
    FileModifiedEvent,
    FileMovedEvent,
)

from mcap_indexer.watcher import McapEventHandler, WatchEvent, wait_for_stable


def test_debounce_coalesces_modifies():
    q: queue.Queue = queue.Queue()
    h = McapEventHandler(q, debounce_secs=0.05)
    for _ in range(5):
        h.on_modified(FileModifiedEvent("/w/a.mcap"))
    time.sleep(0.12)
    assert q.get(timeout=1) == WatchEvent("index", "/w/a.mcap")
    assert q.empty()  # five modifies collapsed to one enqueue


def test_filters_non_indexable_and_dirs():
    q: queue.Queue = queue.Queue()
    h = McapEventHandler(q, debounce_secs=0.01)
    h.on_modified(FileModifiedEvent("/w/x.txt"))
    h.on_modified(FileModifiedEvent("/w/x.mcap.tmp"))
    h.on_modified(FileModifiedEvent("/w/.hidden.mcap"))
    h.on_modified(DirModifiedEvent("/w/somedir"))
    time.sleep(0.05)
    assert q.empty()


def test_on_deleted_enqueues_delete():
    q: queue.Queue = queue.Queue()
    h = McapEventHandler(q, debounce_secs=0.01)
    h.on_deleted(FileDeletedEvent("/w/a.mcap"))
    assert q.get_nowait() == WatchEvent("delete", "/w/a.mcap")


def test_on_created_indexes_after_debounce():
    q: queue.Queue = queue.Queue()
    h = McapEventHandler(q, debounce_secs=0.02)
    h.on_created(FileCreatedEvent("/w/a.mcap"))
    time.sleep(0.06)
    assert q.get_nowait() == WatchEvent("index", "/w/a.mcap")


def test_on_moved_tmp_to_final_is_index_only():
    q: queue.Queue = queue.Queue()
    h = McapEventHandler(q, debounce_secs=0.02)
    h.on_moved(FileMovedEvent("/w/a.mcap.tmp", "/w/a.mcap"))  # atomic upload rename
    time.sleep(0.06)
    assert q.get_nowait() == WatchEvent("index", "/w/a.mcap")
    assert q.empty()  # the .tmp source is not indexable → no delete


def test_on_moved_within_tree_is_delete_then_index():
    q: queue.Queue = queue.Queue()
    h = McapEventHandler(q, debounce_secs=0.02)
    h.on_moved(FileMovedEvent("/w/a.mcap", "/w/b.mcap"))
    assert q.get_nowait() == WatchEvent("delete", "/w/a.mcap")
    time.sleep(0.06)
    assert q.get_nowait() == WatchEvent("index", "/w/b.mcap")


def test_cancel_timers_prevents_enqueue():
    q: queue.Queue = queue.Queue()
    h = McapEventHandler(q, debounce_secs=0.1)
    h.on_created(FileCreatedEvent("/w/a.mcap"))
    h.cancel_timers()
    time.sleep(0.15)
    assert q.empty()


def test_wait_for_stable_true_when_steady(monkeypatch):
    monkeypatch.setattr(os.path, "getsize", lambda _p: 100)
    monkeypatch.setattr(os.path, "exists", lambda _p: True)
    assert wait_for_stable("x", interval=0.0, checks=3) is True


def test_wait_for_stable_false_when_growing(monkeypatch):
    sizes = iter([10, 20, 30])
    monkeypatch.setattr(os.path, "getsize", lambda _p: next(sizes))
    monkeypatch.setattr(os.path, "exists", lambda _p: True)
    assert wait_for_stable("x", interval=0.0, checks=3) is False


def test_wait_for_stable_false_when_missing(monkeypatch):
    def boom(_p):
        raise OSError("gone")

    monkeypatch.setattr(os.path, "getsize", boom)
    assert wait_for_stable("x", interval=0.0, checks=3) is False


def test_on_deleted_cancels_pending_index_timer():
    q: queue.Queue = queue.Queue()
    h = McapEventHandler(q, debounce_secs=0.1)
    h.on_created(FileCreatedEvent("/w/a.mcap"))  # schedules an index timer
    h.on_deleted(FileDeletedEvent("/w/a.mcap"))  # must cancel it + enqueue delete
    assert q.get_nowait() == WatchEvent("delete", "/w/a.mcap")
    time.sleep(0.15)
    assert q.empty()  # the index timer was cancelled → no stale index event
