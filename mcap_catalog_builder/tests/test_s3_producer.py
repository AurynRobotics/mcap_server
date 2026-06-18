"""Tests for the SQS-driven producer (the cloud-native inotify replacement).

A FakeSQS yields one canned batch of S3 event notifications, then stops the
loop. The producer must enqueue the same WatchEvents the inotify handler would,
decode the URL-encoded object keys, skip non-.mcap keys, and ack each message.
"""

import json
import queue
import threading

from mcap_catalog_builder.s3_producer import s3_event_producer
from mcap_catalog_builder.watcher import WatchEvent

_KEY_ENC = "customer%3Dacme/customer_site%3Dhq/robot%3Dr1/source%3Dros/date%3D2026-06-02/x.mcap"
_KEY_DEC = "customer=acme/customer_site=hq/robot=r1/source=ros/date=2026-06-02/x.mcap"


def _s3_event(records: list[tuple[str, str]]) -> str:
    return json.dumps(
        {"Records": [{"eventName": n, "s3": {"object": {"key": k}}} for n, k in records]}
    )


class FakeSQS:
    """Serves one batch, then sets the stop event and returns nothing."""

    def __init__(self, batch, stop_event) -> None:
        self._batch = batch
        self._stop = stop_event
        self.deleted: list[str] = []

    def receive_message(self, QueueUrl, MaxNumberOfMessages=10, WaitTimeSeconds=20):
        if self._batch is None:
            self._stop.set()
            return {"Messages": []}
        batch, self._batch = self._batch, None
        return {"Messages": batch}

    def delete_message(self, QueueUrl, ReceiptHandle):
        self.deleted.append(ReceiptHandle)


def _drain(sqs, stop_event):
    work_q: "queue.Queue[WatchEvent]" = queue.Queue()
    s3_event_producer(sqs, "http://q", work_q, stop_event)
    out = []
    while not work_q.empty():
        out.append(work_q.get())
    return out


def test_created_and_removed_become_catalog_and_delete_events():
    stop = threading.Event()
    body = _s3_event([("ObjectCreated:Put", _KEY_ENC),
                      ("ObjectRemoved:Delete", _KEY_ENC)])
    sqs = FakeSQS([{"Body": body, "ReceiptHandle": "rh-1"}], stop)
    events = _drain(sqs, stop)
    assert events == [WatchEvent("catalog", _KEY_DEC), WatchEvent("delete", _KEY_DEC)]
    assert sqs.deleted == ["rh-1"]  # acked after enqueue


def test_non_mcap_keys_are_ignored_but_message_is_acked():
    stop = threading.Event()
    body = _s3_event([("ObjectCreated:Put", "customer%3Dacme/notes.txt")])
    sqs = FakeSQS([{"Body": body, "ReceiptHandle": "rh-2"}], stop)
    events = _drain(sqs, stop)
    assert events == []
    assert sqs.deleted == ["rh-2"]
